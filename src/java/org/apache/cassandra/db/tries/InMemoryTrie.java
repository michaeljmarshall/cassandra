/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.tries;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.github.jamm.MemoryLayoutSpecification;

/**
 * In-memory trie built for fast modification and reads executing concurrently with writes from a single mutator thread.
 * <p>
 * The main method for performing writes is {@link #apply(Trie, UpsertTransformer, Predicate)} which takes a trie as
 * an argument and merges it into the current trie using the methods supplied by the given {@link UpsertTransformer},
 * force copying anything below the points where the third argument returns true.
 * </p><p>
 * The predicate can be used to implement several forms of atomicity and consistency guarantees:
 * <list>
 * <li> if the predicate is {@code nf -> false}, neither atomicity nor sequential consistency is guaranteed - readers
 *      can see any mixture of old and modified content
 * <li> if the predicate is {@code nf -> true}, full sequential consistency will be provided, i.e. if a reader sees any
 *      part of a modification, it will see all of it, and all the results of all previous modifications
 * <li> if the predicate is {@code nf -> nf.isBranching()} the write will be atomic, i.e. either none or all of the
 *      content of the merged trie will be visible by concurrent readers, but not sequentially consistent, i.e. there
 *      may be writes that are not visible to a reader even when they precede writes that are visible.
 * <li> if the predicate is {@code nf -> <some_test>(nf.content())} the write will be consistent below the identified
 *      point (used e.g. by Memtable to ensure partition-level consistency)
 * </list>
 * </p><p>
 * Additionally, the class provides several simpler write methods for efficiency and convenience:
 * <list>
 * <li> {@link #putRecursive(ByteComparable, Object, UpsertTransformer)} inserts a single value using a recursive walk.
 *      It cannot provide consistency (single-path writes are always atomic). This is more efficient as it stores the
 *      walk state in the stack rather than on the heap but can cause a {@code StackOverflowException}.
 * <li> {@link #putSingleton(ByteComparable, Object, UpsertTransformer)} is a non-recursive version of the above, using
 *      the {@code apply} machinery.
 * <li> {@link #putSingleton(ByteComparable, Object, UpsertTransformer, boolean)} uses the fourth argument to choose
 *      between the two methods above, where some external property can be used to decide if the keys are short enough
 *      to permit recursive execution.
 * </list>
 * </p><p>
 * Because it uses 32-bit pointers in byte buffers, this trie has a fixed size limit of 2GB.
 */
public class InMemoryTrie<T> extends InMemoryReadTrie<T>
{
    // See the trie format description in InMemoryReadTrie.

    /**
     * Trie size limit. This is not enforced, but users must check from time to time that it is not exceeded (using
     * {@link #reachedAllocatedSizeThreshold()}) and start switching to a new trie if it is.
     * This must be done to avoid tries growing beyond their hard 2GB size limit (due to the 32-bit pointers).
     */
    @VisibleForTesting
    static final int ALLOCATED_SIZE_THRESHOLD;
    static
    {
        // Default threshold + 10% == 2 GB. This should give the owner enough time to react to the
        // {@link #reachedAllocatedSizeThreshold()} signal and switch this trie out before it fills up.
        int limitInMB = CassandraRelevantProperties.MEMTABLE_TRIE_SIZE_LIMIT.getInt(2048 * 10 / 11);
        if (limitInMB < 1 || limitInMB > 2047)
            throw new AssertionError(CassandraRelevantProperties.MEMTABLE_TRIE_SIZE_LIMIT.getKey() +
                                     " must be within 1 and 2047");
        ALLOCATED_SIZE_THRESHOLD = 1024 * 1024 * limitInMB;
    }

    private int allocatedPos = 0;
    private int contentCount = 0;

    final BufferType bufferType;    // on or off heap
    final MemoryAllocationStrategy cellAllocator;
    final MemoryAllocationStrategy objectAllocator;


    // constants for space calculations
    private static final long EMPTY_SIZE_ON_HEAP;
    private static final long EMPTY_SIZE_OFF_HEAP;
    private static final long REFERENCE_ARRAY_ON_HEAP_SIZE = ObjectSizes.measureDeep(new AtomicReferenceArray<>(0));

    static
    {
        // Measuring the empty size of long-lived tries, because these are the ones for which we want to track size.
        InMemoryTrie<Object> empty = new InMemoryTrie<>(ByteComparable.Version.OSS50, BufferType.ON_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryTrie<>(ByteComparable.Version.OSS50, BufferType.OFF_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    enum ExpectedLifetime
    {
        SHORT, LONG
    }

    InMemoryTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion,
              new UnsafeBuffer[31 - BUF_START_SHIFT],  // last one is 1G for a total of ~2G bytes
              new AtomicReferenceArray[29 - CONTENTS_START_SHIFT],  // takes at least 4 bytes to write pointer to one content -> 4 times smaller than buffers
              NONE);
        this.bufferType = bufferType;

        switch (lifetime)
        {
            case SHORT:
                cellAllocator = new MemoryAllocationStrategy.NoReuseStrategy(new MemoryAllocationStrategy.Allocator()
                {
                    @Override
                    public int allocate() throws TrieSpaceExhaustedException
                    {
                        return allocateNewCell();
                    }
                });
                objectAllocator = new MemoryAllocationStrategy.NoReuseStrategy(new MemoryAllocationStrategy.Allocator()
                {
                    @Override
                    public int allocate()
                    {
                        return allocateNewObject();
                    }
                });
                break;
            case LONG:
                cellAllocator = new MemoryAllocationStrategy.OpOrderReuseStrategy(new MemoryAllocationStrategy.Allocator()
                {
                    @Override
                    public int allocate() throws TrieSpaceExhaustedException
                    {
                        return allocateNewCell();
                    }
                }, opOrder);
                objectAllocator = new MemoryAllocationStrategy.OpOrderReuseStrategy(new MemoryAllocationStrategy.Allocator()
                {
                    @Override
                    public int allocate()
                    {
                        return allocateNewObject();
                    }
                }, opOrder);
                break;
            default:
                throw new AssertionError();
        }
    }

    public static <T> InMemoryTrie<T> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryTrie<>(byteComparableVersion, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <T> InMemoryTrie<T> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <T> InMemoryTrie<T> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <T> InMemoryTrie<T> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder);
    }


    // Buffer, content list and cell management

    private void putInt(int pos, int value)
    {
        getBuffer(pos).putInt(inBufferOffset(pos), value);
    }

    private void putIntVolatile(int pos, int value)
    {
        getBuffer(pos).putIntVolatile(inBufferOffset(pos), value);
    }

    private void putShort(int pos, short value)
    {
        getBuffer(pos).putShort(inBufferOffset(pos), value);
    }

    private void putShortVolatile(int pos, short value)
    {
        getBuffer(pos).putShort(inBufferOffset(pos), value);
    }

    private void putByte(int pos, byte value)
    {
        getBuffer(pos).putByte(inBufferOffset(pos), value);
    }

    /**
     * Allocate a new cell in the data buffers. This is called by the memory allocation strategy when it runs out of
     * free cells to reuse.
     */
    private int allocateNewCell() throws TrieSpaceExhaustedException
    {
        // Note: If this method is modified, please run InMemoryTrieTest.testOver1GSize to verify it acts correctly
        // close to the 2G limit.
        int v = allocatedPos;
        if (inBufferOffset(v) == 0)
        {
            int leadBit = getBufferIdx(v, BUF_START_SHIFT, BUF_START_SIZE);
            if (leadBit + BUF_START_SHIFT == 31)
                throw new TrieSpaceExhaustedException();

            ByteBuffer newBuffer = bufferType.allocate(BUF_START_SIZE << leadBit);
            buffers[leadBit] = new UnsafeBuffer(newBuffer);
            // Note: Since we are not moving existing data to a new buffer, we are okay with no happens-before enforcing
            // writes. Any reader that sees a pointer in the new buffer may only do so after reading the volatile write
            // that attached the new path.
        }

        allocatedPos += CELL_SIZE;
        return v;
    }

    /**
     * Allocate a cell to use for storing data. This uses the memory allocation strategy to reuse cells if any are
     * available, or to allocate new cells using {@link #allocateNewCell}. Because some node types rely on cells being
     * filled with 0 as initial state, any cell we get through the allocator must also be cleaned.
     */
    private int allocateCell() throws TrieSpaceExhaustedException
    {
        int cell = cellAllocator.allocate();
        getBuffer(cell).setMemory(inBufferOffset(cell), CELL_SIZE, (byte) 0);
        return cell;
    }

    private void recycleCell(int cell)
    {
        cellAllocator.recycle(cell & -CELL_SIZE);
    }

    /**
     * Creates a copy of a given cell and marks the original for recycling. Used when a mutation needs to force-copy
     * paths to ensure earlier states are still available for concurrent readers.
     */
    private int copyCell(int cell) throws TrieSpaceExhaustedException
    {
        int copy = cellAllocator.allocate();
        getBuffer(copy).putBytes(inBufferOffset(copy), getBuffer(cell), inBufferOffset(cell & -CELL_SIZE), CELL_SIZE);
        recycleCell(cell);
        return copy | (cell & (CELL_SIZE - 1));
    }

    /**
     * Allocate a new position in the object array. Used by the memory allocation strategy to allocate a content spot
     * when it runs out of recycled positions.
     */
    private int allocateNewObject()
    {
        int index = contentCount++;
        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        if (array == null)
        {
            assert inBufferOffset(index, leadBit, CONTENTS_START_SIZE) == 0 : "Error in content arrays configuration.";
            contentArrays[leadBit] = new AtomicReferenceArray<>(CONTENTS_START_SIZE << leadBit);
        }
        return index;
    }


    /**
     * Add a new content value.
     *
     * @return A content id that can be used to reference the content, encoded as ~index where index is the
     *         position of the value in the content array.
     */
    private int addContent(T value) throws TrieSpaceExhaustedException
    {
        int index = objectAllocator.allocate();
        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        // no need for a volatile set here; at this point the item is not referenced
        // by any node in the trie, and a volatile set will be made to reference it.
        array.setPlain(ofs, value);
        return ~index;
    }

    /**
     * Change the content associated with a given content id.
     *
     * @param id content id, encoded as ~index where index is the position in the content array
     * @param value new content value to store
     */
    private void setContent(int id, T value)
    {
        int leadBit = getBufferIdx(~id, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(~id, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        array.set(ofs, value);
    }

    private void releaseContent(int id)
    {
        objectAllocator.recycle(~id);
    }

    /**
     * Called to clean up all buffers when the trie is known to no longer be needed.
     */
    public void discardBuffers()
    {
        if (bufferType == BufferType.ON_HEAP)
            return; // no cleaning needed

        for (UnsafeBuffer b : buffers)
        {
            if (b != null)
                FileUtils.clean(b.byteBuffer());
        }
    }

    private int copyIfOriginal(int node, int originalNode) throws TrieSpaceExhaustedException
    {
        return (node == originalNode)
               ? copyCell(originalNode)
               : node;
    }

    private int getOrAllocate(int pointerAddress, int offsetWhenAllocating) throws TrieSpaceExhaustedException
    {
        int child = getIntVolatile(pointerAddress);
        if (child != NONE)
            return child;

        child = allocateCell() | offsetWhenAllocating;
        // volatile writes not needed because this branch is not attached yet
        putInt(pointerAddress, child);
        return child;
    }

    private int getCopyOrAllocate(int pointerAddress, int originalChild, int offsetWhenAllocating) throws TrieSpaceExhaustedException
    {
        int child = getIntVolatile(pointerAddress);
        if (child == originalChild)
        {
            if (originalChild == NONE)
                child = allocateCell() | offsetWhenAllocating;
            else
                child = copyCell(originalChild);

            // volatile writes not needed because this branch is not attached yet
            putInt(pointerAddress, child);
        }

        return child;
    }

    // Write methods

    // Write visibility model: writes are not volatile, with the exception of the final write before a call returns
    // the same value that was present before (e.g. content was updated in-place / existing node got a new child or had
    // a child pointer updated); if the whole path including the root node changed, the root itself gets a volatile
    // write.
    // This final write is the point where any new cells created during the write become visible for readers for the
    // first time, and such readers must pass through reading that pointer, which forces a happens-before relationship
    // that extends to all values written by this thread before it.

    /**
     * Attach a child to the given non-content node. This may be an update for an existing branch, or a new child for
     * the node. An update _is_ required (i.e. this is only called when the newChild pointer is not the same as the
     * existing value).
     * This method is called when the original node content must be preserved for concurrent readers (i.e. any cell to
     * be modified needs to be copied first.)
     *
     * @param node pointer to the node to update or copy
     * @param originalNode pointer to the node as it was before any updates in the current modification (i.e. apply
     *                     call) were started. In other words, the node that is currently reachable by readers if they
     *                     follow the same key, and which will become unreachable for new readers after this update
     *                     completes. Used to avoid copying again if already done -- if node is already != originalNode
     *                     (which is the case when a second or further child of a node is changed by an update),
     *                     then node is currently not reachable and can be safely modified or completely overwritten.
     * @param trans transition to modify/add
     * @param newChild new child pointer
     * @return pointer to the updated node
     */
    private int attachChildCopying(int node, int originalNode, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        assert !isLeaf(node) : "attachChild cannot be used on content nodes.";

        switch (offset(node))
        {
            case PREFIX_OFFSET:
                assert false : "attachChild cannot be used on content nodes.";
            case SPARSE_OFFSET:
                // If the node is already copied (e.g. this is not the first child being modified), there's no need to copy
                // it again.
                return attachChildToSparseCopying(node, originalNode, trans, newChild);
            case SPLIT_OFFSET:
                // This call will copy the split node itself and any intermediate cells as necessary to make sure cells
                // reachable from the original node are not modified.
                return attachChildToSplitCopying(node, originalNode, trans, newChild);
            default:
                // chain nodes
                return attachChildToChainCopying(node, originalNode, trans, newChild); // always copies
        }
    }

    /**
     * Attach a child to the given node. This may be an update for an existing branch, or a new child for the node.
     * An update _is_ required (i.e. this is only called when the newChild pointer is not the same as the existing value).
     *
     * @param node pointer to the node to update or copy
     * @param trans transition to modify/add
     * @param newChild new child pointer
     * @return pointer to the updated node; same as node if update was in-place
     */
    private int attachChild(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        assert !isLeaf(node) : "attachChild cannot be used on content nodes.";

        switch (offset(node))
        {
            case PREFIX_OFFSET:
                assert false : "attachChild cannot be used on content nodes.";
            case SPARSE_OFFSET:
                return attachChildToSparse(node, trans, newChild);
            case SPLIT_OFFSET:
                return attachChildToSplit(node, trans, newChild);
            default:
                return attachChildToChain(node, trans, newChild);
        }
    }

    /**
     * Attach a child to the given split node. This may be an update for an existing branch, or a new child for the node.
     */
    private int attachChildToSplit(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        int midPos = splitCellPointerAddress(node, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        int mid = getIntVolatile(midPos);
        if (isNull(mid))
        {
            mid = createEmptySplitNode();
            int tailPos = splitCellPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
            int tail = createEmptySplitNode();
            int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
            putInt(childPos, newChild);
            putInt(tailPos, tail);
            putIntVolatile(midPos, mid);
            return node;
        }

        int tailPos = splitCellPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        int tail = getIntVolatile(tailPos);
        if (isNull(tail))
        {
            tail = createEmptySplitNode();
            int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
            putInt(childPos, newChild);
            putIntVolatile(tailPos, tail);
            return node;
        }

        int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        putIntVolatile(childPos, newChild);
        return node;
    }

    /**
     * Non-volatile version of attachChildToSplit. Used when the split node is not reachable yet (during the conversion
     * from sparse).
     */
    private int attachChildToSplitNonVolatile(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        assert offset(node) == SPLIT_OFFSET : "Invalid split node in trie";
        int midPos = splitCellPointerAddress(node, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        int mid = getOrAllocate(midPos, SPLIT_OFFSET);
        assert offset(mid) == SPLIT_OFFSET : "Invalid split node in trie";
        int tailPos = splitCellPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        int tail = getOrAllocate(tailPos, SPLIT_OFFSET);
        assert offset(tail) == SPLIT_OFFSET : "Invalid split node in trie";
        int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        putInt(childPos, newChild);
        return node;
    }

    /**
     * Attach a child to the given split node, copying all modified content to enable atomic visibility
     * of modification.
     * This may be an update for an existing branch, or a new child for the node.
     */
    private int attachChildToSplitCopying(int node, int originalNode, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        if (offset(originalNode) != SPLIT_OFFSET)  // includes originalNode == NONE
            return attachChildToSplitNonVolatile(node, trans, newChild);

        node = copyIfOriginal(node, originalNode);
        assert offset(node) == SPLIT_OFFSET : "Invalid split node in trie";

        int midPos = splitCellPointerAddress(0, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        int midOriginal = originalNode != NONE ? getIntVolatile(midPos + originalNode) : NONE;
        int mid = getCopyOrAllocate(node + midPos, midOriginal, SPLIT_OFFSET);
        assert offset(mid) == SPLIT_OFFSET : "Invalid split node in trie";

        int tailPos = splitCellPointerAddress(0, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        int tailOriginal = midOriginal != NONE ? getIntVolatile(tailPos + midOriginal) : NONE;
        int tail = getCopyOrAllocate(mid + tailPos, tailOriginal, SPLIT_OFFSET);
        assert offset(tail) == SPLIT_OFFSET : "Invalid split node in trie";

        int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        putInt(childPos, newChild);
        return node;
    }

    /**
     * Attach a child to the given sparse node. This may be an update for an existing branch, or a new child for the node.
     */
    private int attachChildToSparse(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        int index;
        int smallerCount = 0;
        // first check if this is an update and modify in-place if so
        for (index = 0; index < SPARSE_CHILD_COUNT; ++index)
        {
            if (isNull(getIntVolatile(node + SPARSE_CHILDREN_OFFSET + index * 4)))
                break;
            final int existing = getUnsignedByte(node + SPARSE_BYTES_OFFSET + index);
            if (existing == trans)
            {
                putIntVolatile(node + SPARSE_CHILDREN_OFFSET + index * 4, newChild);
                return node;
            }
            else if (existing < trans)
                ++smallerCount;
        }
        int childCount = index;

        if (childCount == SPARSE_CHILD_COUNT)
        {
            // Node is full. Switch to split
            return upgradeSparseToSplit(node, trans, newChild);
        }

        // Add a new transition. They are not kept in order, so append it at the first free position.
        putByte(node + SPARSE_BYTES_OFFSET + childCount, (byte) trans);

        // Update order word.
        int order = getUnsignedShortVolatile(node + SPARSE_ORDER_OFFSET);
        int newOrder = insertInOrderWord(order, childCount, smallerCount);

        // Sparse nodes have two access modes: via the order word, when listing transitions, or directly to characters
        // and addresses.
        // To support the former, we volatile write to the order word last, and everything is correctly set up.
        // The latter does not touch the order word. To support that too, we volatile write the address, as the reader
        // can't determine if the position is in use based on the character byte alone (00 is also a valid transition).
        // Note that this means that reader must check the transition byte AFTER the address, to ensure they get the
        // correct value (see getSparseChild).

        // setting child enables reads to start seeing the new branch
        putIntVolatile(node + SPARSE_CHILDREN_OFFSET + childCount * 4, newChild);

        // some readers will decide whether to check the pointer based on the order word
        // write that volatile to make sure they see the new change too
        putShortVolatile(node + SPARSE_ORDER_OFFSET,  (short) newOrder);
        return node;
    }

    /**
     * Attach a child to the given sparse node. This may be an update for an existing branch, or a new child for the node.
     * Resulting node is not reachable, no volatile set needed.
     */
    private int attachChildToSparseCopying(int node, int originalNode, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        int index;
        int smallerCount = 0;
        // first check if this is an update and modify in-place if so
        for (index = 0; index < SPARSE_CHILD_COUNT; ++index)
        {
            if (isNull(getIntVolatile(node + SPARSE_CHILDREN_OFFSET + index * 4)))
                break;
            final int existing = getUnsignedByte(node + SPARSE_BYTES_OFFSET + index);
            if (existing == trans)
            {
                node = copyIfOriginal(node, originalNode);
                putInt(node + SPARSE_CHILDREN_OFFSET + index * 4, newChild);
                return node;
            }
            else if (existing < trans)
                ++smallerCount;
        }
        int childCount = index;

        if (childCount == SPARSE_CHILD_COUNT)
        {
            // Node is full. Switch to split.
            // Note that even if node != originalNode, we still have to recycle it as it was a temporary one that will
            // no longer be attached.
            return upgradeSparseToSplit(node, trans, newChild);
        }

        node = copyIfOriginal(node, originalNode);

        // Add a new transition. They are not kept in order, so append it at the first free position.
        putByte(node + SPARSE_BYTES_OFFSET + childCount,  (byte) trans);

        putInt(node + SPARSE_CHILDREN_OFFSET + childCount * 4, newChild);

        // Update order word.
        int order = getUnsignedShortVolatile(node + SPARSE_ORDER_OFFSET);
        int newOrder = insertInOrderWord(order, childCount, smallerCount);
        putShort(node + SPARSE_ORDER_OFFSET,  (short) newOrder);

        return node;
    }

    private int upgradeSparseToSplit(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        int split = createEmptySplitNode();
        for (int i = 0; i < SPARSE_CHILD_COUNT; ++i)
        {
            int t = getUnsignedByte(node + SPARSE_BYTES_OFFSET + i);
            int p = getIntVolatile(node + SPARSE_CHILDREN_OFFSET + i * 4);
            attachChildToSplitNonVolatile(split, t, p);
        }
        attachChildToSplitNonVolatile(split, trans, newChild);
        recycleCell(node);
        return split;
    }

    /**
     * Insert the given newIndex in the base-6 encoded order word in the correct position with respect to the ordering.
     * <p>
     * E.g.
     *   - insertOrderWord(120, 3, 0) must return 1203 (decimal 48*6 + 3)
     *   - insertOrderWord(120, 3, 1, ptr) must return 1230 (decimal 8*36 + 3*6 + 0)
     *   - insertOrderWord(120, 3, 2, ptr) must return 1320 (decimal 1*216 + 3*36 + 12)
     *   - insertOrderWord(120, 3, 3, ptr) must return 3120 (decimal 3*216 + 48)
     */
    private static int insertInOrderWord(int order, int newIndex, int smallerCount)
    {
        int r = 1;
        for (int i = 0; i < smallerCount; ++i)
            r *= 6;
        int head = order / r;
        int tail = order % r;
        // insert newIndex after the ones we have passed (order % r) and before the remaining (order / r)
        return tail + (head * 6 + newIndex) * r;
    }

    /**
     * Attach a child to the given chain node. This may be an update for an existing branch with different target
     * address, or a second child for the node.
     * This method always copies the node -- with the exception of updates that change the child of the last node in a
     * chain cell with matching transition byte (which this method is not used for, see attachChild), modifications to
     * chain nodes cannot be done in place, either because we introduce a new transition byte and have to convert from
     * the single-transition chain type to sparse, or because we have to remap the child from the implicit node + 1 to
     * something else.
     */
    private int attachChildToChain(int node, int transitionByte, int newChild) throws TrieSpaceExhaustedException
    {
        int existingByte = getUnsignedByte(node);
        if (transitionByte == existingByte)
        {
            // This is still a single path. Update child if possible (only if this is the last character in the chain).
            if (offset(node) == LAST_POINTER_OFFSET - 1)
            {
                putIntVolatile(node + 1, newChild);
                return node;
            }
            else
            {
                // This will only be called if new child is different from old, and the update is not on the final child
                // where we can change it in place (see attachChild). We must always create something new.
                // Note that since this is not the last character, we either still need this cell or we have already
                // released it (a createSparseNode must have been called earlier).
                // If the child is a chain, we can expand it (since it's a different value, its branch must be new and
                // nothing can already reside in the rest of the cell).
                return expandOrCreateChainNode(transitionByte, newChild);
            }
        }

        // The new transition is different, so we no longer have only one transition. Change type.
        return convertChainToSparse(node, existingByte, newChild, transitionByte);
    }

    /**
     * Attach a child to the given chain node, when we are force-copying.
     */
    private int attachChildToChainCopying(int node, int originalNode, int transitionByte, int newChild)
    throws TrieSpaceExhaustedException
    {
        int existingByte = getUnsignedByte(node);
        if (transitionByte == existingByte)
        {
            // This is still a single path.
            // Make sure we release the cell if it will no longer be referenced (if we update last reference, the whole
            // path has to move as the other nodes in this chain can't be remapped).
            if (offset(node) == LAST_POINTER_OFFSET - 1)
            {
                assert node == originalNode;    // if we have already created a node, the character can't match what
                                                // it was created with

                recycleCell(node);
            }

            return expandOrCreateChainNode(transitionByte, newChild);
        }
        else
        {
            // The new transition is different, so we no longer have only one transition. Change type.
            return convertChainToSparse(node, existingByte, newChild, transitionByte);
        }
    }

    private int convertChainToSparse(int node, int existingByte, int newChild, int transitionByte)
    throws TrieSpaceExhaustedException
    {
        int existingChild = node + 1;
        if (offset(existingChild) == LAST_POINTER_OFFSET)
        {
            existingChild = getIntVolatile(existingChild);
            // This was a chain with just one transition which will no longer be referenced.
            // The cell may contain other characters/nodes leading to this, which are also guaranteed to be
            // unreferenced.
            // However, these leading nodes may still be in the parent path and will be needed until the
            // mutation completes.
            recycleCell(node);
        }
        // Otherwise the sparse node we will now create references this cell, so it can't be recycled.
        return createSparseNode(existingByte, existingChild, transitionByte, newChild);
    }

    private boolean isExpandableChain(int newChild)
    {
        int newOffset = offset(newChild);
        return newChild > 0 && newChild - 1 > NONE && newOffset > CHAIN_MIN_OFFSET && newOffset <= CHAIN_MAX_OFFSET;
    }

    /**
     * Create a sparse node with two children.
     */
    private int createSparseNode(int byte1, int child1, int byte2, int child2) throws TrieSpaceExhaustedException
    {
        assert byte1 != byte2 : "Attempted to create a sparse node with two of the same transition";
        if (byte1 > byte2)
        {
            // swap them so the smaller is byte1, i.e. there's always something bigger than child 0 so 0 never is
            // at the end of the order
            int t = byte1; byte1 = byte2; byte2 = t;
            t = child1; child1 = child2; child2 = t;
        }

        int node = allocateCell() + SPARSE_OFFSET;
        putByte(node + SPARSE_BYTES_OFFSET + 0,  (byte) byte1);
        putByte(node + SPARSE_BYTES_OFFSET + 1,  (byte) byte2);
        putInt(node + SPARSE_CHILDREN_OFFSET + 0 * 4, child1);
        putInt(node + SPARSE_CHILDREN_OFFSET + 1 * 4, child2);
        putShort(node + SPARSE_ORDER_OFFSET,  (short) (1 * 6 + 0));
        // Note: this does not need a volatile write as it is a new node, returning a new pointer, which needs to be
        // put in an existing node or the root. That action ends in a happens-before enforcing write.
        return node;
    }

    /**
     * Creates a chain node with the single provided transition (pointing to the provided child).
     * Note that to avoid creating inefficient tries with under-utilized chain nodes, this should only be called from
     * {@link #expandOrCreateChainNode} and other call-sites should call {@link #expandOrCreateChainNode}.
     */
    private int createNewChainNode(int transitionByte, int newChild) throws TrieSpaceExhaustedException
    {
        int newNode = allocateCell() + LAST_POINTER_OFFSET - 1;
        putByte(newNode, (byte) transitionByte);
        putInt(newNode + 1, newChild);
        // Note: this does not need a volatile write as it is a new node, returning a new pointer, which needs to be
        // put in an existing node or the root. That action ends in a happens-before enforcing write.
        return newNode;
    }

    /** Like {@link #createNewChainNode}, but if the new child is already a chain node and has room, expand
     * it instead of creating a brand new node. */
    private int expandOrCreateChainNode(int transitionByte, int newChild) throws TrieSpaceExhaustedException
    {
        if (isExpandableChain(newChild))
        {
            // attach as a new character in child node
            int newNode = newChild - 1;
            putByte(newNode, (byte) transitionByte);
            return newNode;
        }

        return createNewChainNode(transitionByte, newChild);
    }

    private int createEmptySplitNode() throws TrieSpaceExhaustedException
    {
        return allocateCell() + SPLIT_OFFSET;
    }

    private int createPrefixNode(int contentId, int child, boolean isSafeChain) throws TrieSpaceExhaustedException
    {
        assert !isNullOrLeaf(child) : "Prefix node cannot reference a childless node.";

        int offset = offset(child);
        int node;
        if (offset == SPLIT_OFFSET || isSafeChain && offset > (PREFIX_FLAGS_OFFSET + PREFIX_OFFSET) && offset <= CHAIN_MAX_OFFSET)
        {
            // We can do an embedded prefix node
            // Note: for chain nodes we have a risk that the node continues beyond the current point, in which case
            // creating the embedded node may overwrite information that is still needed by concurrent readers or the
            // mutation process itself.
            node = (child & -CELL_SIZE) | PREFIX_OFFSET;
            putByte(node + PREFIX_FLAGS_OFFSET, (byte) offset);
        }
        else
        {
            // Full prefix node
            node = allocateCell() + PREFIX_OFFSET;
            putByte(node + PREFIX_FLAGS_OFFSET, (byte) 0xFF);
            putInt(node + PREFIX_POINTER_OFFSET, child);
        }

        putInt(node + PREFIX_CONTENT_OFFSET, contentId);
        return node;
    }

    private int updatePrefixNodeChild(int node, int child, boolean forcedCopy) throws TrieSpaceExhaustedException
    {
        assert offset(node) == PREFIX_OFFSET : "updatePrefix called on non-prefix node";
        assert !isNullOrLeaf(child) : "Prefix node cannot reference a childless node.";

        // We can only update in-place if we have a full prefix node
        if (!isEmbeddedPrefixNode(node))
        {
            if (!forcedCopy)
            {
                // This attaches the child branch and makes it reachable -- the write must be volatile.
                putIntVolatile(node + PREFIX_POINTER_OFFSET, child);
                return node;
            }
            else
            {
                node = copyCell(node);
                putInt(node + PREFIX_POINTER_OFFSET, child);
                return node;
            }
        }
        else
        {
            // No need to recycle this cell because that is already done by the modification of the child
            int contentId = getIntVolatile(node + PREFIX_CONTENT_OFFSET);
            return createPrefixNode(contentId, child, true);
        }
    }

    private boolean isEmbeddedPrefixNode(int node)
    {
        return getUnsignedByte(node + PREFIX_FLAGS_OFFSET) < CELL_SIZE;
    }

    /**
     * Copy the content from an existing node, if it has any, to a newly-prepared update for its child.
     *
     * @param existingPreContentNode pointer to the existing node before skipping over content nodes, i.e. this is
     *                               either the same as existingPostContentNode or a pointer to a prefix or leaf node
     *                               whose child is existingPostContentNode
     * @param existingPostContentNode pointer to the existing node being updated, after any content nodes have been
     *                                skipped and before any modification have been applied; always a non-content node
     * @param updatedPostContentNode is the updated node, i.e. the node to which all relevant modifications have been
     *                               applied; if the modifications were applied in-place, this will be the same as
     *                               existingPostContentNode, otherwise a completely different pointer; always a non-
     *                               content node
     * @param forcedCopy whether or not we need to preserve all pre-existing data for concurrent readers
     * @return a node which has the children of updatedPostContentNode combined with the content of
     *         existingPreContentNode
     */
    private int preserveContent(int existingPreContentNode,
                                int existingPostContentNode,
                                int updatedPostContentNode,
                               boolean forcedCopy)
    throws TrieSpaceExhaustedException
    {
        if (existingPreContentNode == existingPostContentNode)
            return updatedPostContentNode;     // no content to preserve

        if (existingPostContentNode == updatedPostContentNode)
        {
            assert !forcedCopy;
            return existingPreContentNode;     // child didn't change, no update necessary
        }

        // else we have existing prefix node, and we need to reference a new child
        if (isLeaf(existingPreContentNode))
        {
            return createPrefixNode(existingPreContentNode, updatedPostContentNode, true);
        }

        assert offset(existingPreContentNode) == PREFIX_OFFSET : "Unexpected content in non-prefix and non-leaf node.";
        return updatePrefixNodeChild(existingPreContentNode, updatedPostContentNode, forcedCopy);
    }

    private final ApplyState applyState = new ApplyState();

    /**
     * Represents the state for an {@link #apply} operation. Contains a stack of all nodes we descended through
     * and used to update the nodes with any new data during ascent.
     * <p>
     * To make this as efficient and GC-friendly as possible, we use an integer array (instead of is an object stack)
     * and we reuse the same object. The latter is safe because memtable tries cannot be mutated in parallel by multiple
     * writers.
     */
    private class ApplyState implements KeyProducer<T>
    {
        int[] data = new int[16 * 5];
        int currentDepth = -1;

        /**
         * Pointer to the existing node before skipping over content nodes, i.e. this is either the same as
         * existingPostContentNode or a pointer to a prefix or leaf node whose child is existingPostContentNode.
         */
        int existingPreContentNode()
        {
            return data[currentDepth * 5 + 0];
        }
        void setExistingPreContentNode(int value)
        {
            data[currentDepth * 5 + 0] = value;
        }

        /**
         * Pointer to the existing node being updated, after any content nodes have been skipped and before any
         * modification have been applied. Always a non-content node.
         */
        int existingPostContentNode()
        {
            return data[currentDepth * 5 + 1];
        }
        void setExistingPostContentNode(int value)
        {
            data[currentDepth * 5 + 1] = value;
        }

        /**
         * The updated node, i.e. the node to which the relevant modifications are being applied. This will change as
         * children are processed and attached to the node. After all children have been processed, this will contain
         * the fully updated node (i.e. the union of existingPostContentNode and mutationNode) without any content,
         * which will be processed separately and, if necessary, attached ahead of this. If the modifications were
         * applied in-place, this will be the same as existingPostContentNode, otherwise a completely different
         * pointer. Always a non-content node.
         */
        int updatedPostContentNode()
        {
            return data[currentDepth * 5 + 2];
        }
        void setUpdatedPostContentNode(int value)
        {
            data[currentDepth * 5 + 2] = value;
        }

        /**
         * The transition we took on the way down.
         */
        int transition()
        {
            return data[currentDepth * 5 + 3];
        }
        void setTransition(int transition)
        {
            data[currentDepth * 5 + 3] = transition;
        }
        int transitionAtDepth(int stackDepth)
        {
            return data[stackDepth * 5 + 3];
        }

        /**
         * The compiled content id. Needed because we can only access a cursor's content on the way down but we can't
         * attach it until we ascend from the node.
         */
        int contentId()
        {
            return data[currentDepth * 5 + 4];
        }
        void setContentId(int value)
        {
            data[currentDepth * 5 + 4] = value;
        }
        int contentIdAtDepth(int stackDepth)
        {
            return data[stackDepth * 5 + 4];
        }

        ApplyState start()
        {
            int existingFullNode = root;
            currentDepth = 0;

            descendInto(existingFullNode);
            return this;
        }

        /**
         * Returns true if the depth signals mutation cursor is exhausted.
         */
        boolean advanceTo(int depth, int transition, int forcedCopyDepth) throws TrieSpaceExhaustedException
        {
            while (currentDepth > Math.max(0, depth - 1))
            {
                // There are no more children. Ascend to the parent state to continue walk.
                attachAndMoveToParentState(forcedCopyDepth);
            }
            if (depth == -1)
                return true;

            // We have a transition, get child to descend into
            descend(transition);
            return false;
        }

        /**
         * Descend to a child node. Prepares a new entry in the stack for the node.
         */
        void descend(int transition)
        {
            setTransition(transition);
            int existingPreContentNode = getChild(existingPreContentNode(), transition);
            ++currentDepth;
            descendInto(existingPreContentNode);
        }

        private void descendInto(int existingPreContentNode)
        {
            if (currentDepth * 5 >= data.length)
                data = Arrays.copyOf(data, currentDepth * 5 * 2);
            setExistingPreContentNode(existingPreContentNode);

            int existingContentId = NONE;
            int existingPostContentNode;
            if (isLeaf(existingPreContentNode))
            {
                existingContentId = existingPreContentNode;
                existingPostContentNode = NONE;
            }
            else if (offset(existingPreContentNode) == PREFIX_OFFSET)
            {
                existingContentId = getIntVolatile(existingPreContentNode + PREFIX_CONTENT_OFFSET);
                existingPostContentNode = followContentTransition(existingPreContentNode);
            }
            else
                existingPostContentNode = existingPreContentNode;
            setExistingPostContentNode(existingPostContentNode);
            setUpdatedPostContentNode(existingPostContentNode);
            setContentId(existingContentId);
        }

        T getContent()
        {
            int contentId = contentId();
            if (contentId == NONE)
                return null;
            return InMemoryTrie.this.getContent(contentId());
        }

        void setContent(T content, boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            int contentId = contentId();
            if (contentId == NONE)
            {
                if (content != null)
                    setContentId(InMemoryTrie.this.addContent(content));
            }
            else if (content == null)
            {
                releaseContent(contentId);
                setContentId(NONE);
                // At this point we are not deleting branches on the way up, just making sure we don't hold on to
                // references to content.
            }
            else if (content == InMemoryTrie.this.getContent(contentId))
            {
                // no changes, nothing to do
            }
            else if (forcedCopy)
            {
                releaseContent(contentId);
                setContentId(InMemoryTrie.this.addContent(content));
            }
            else
            {
                InMemoryTrie.this.setContent(contentId, content);
            }
        }

        /**
         * Attach a child to the current node.
         */
        private void attachChild(int transition, int child, boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            int updatedPostContentNode = updatedPostContentNode();
            if (isNull(updatedPostContentNode))
                setUpdatedPostContentNode(expandOrCreateChainNode(transition, child));
            else if (forcedCopy)
                setUpdatedPostContentNode(attachChildCopying(updatedPostContentNode,
                                                             existingPostContentNode(),
                                                             transition,
                                                             child));
            else
                setUpdatedPostContentNode(InMemoryTrie.this.attachChild(updatedPostContentNode,
                                                                        transition,
                                                                        child));
        }

        /**
         * Apply the collected content to a node. Converts NONE to a leaf node, and adds or updates a prefix for all
         * others.
         */
        private int applyContent(boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            // Note: the old content id itself is already released by setContent. Here we must release any standalone
            // prefix nodes that may reference it.
            int contentId = contentId();
            final int updatedPostContentNode = updatedPostContentNode();
            final int existingPreContentNode = existingPreContentNode();
            final int existingPostContentNode = existingPostContentNode();

            // applyPrefixChange does not understand leaf nodes, handle upgrade from and to one explicitly.
            if (isNull(updatedPostContentNode))
            {
                if (existingPreContentNode != existingPostContentNode
                    && !isNullOrLeaf(existingPreContentNode)
                    && !isEmbeddedPrefixNode(existingPreContentNode))
                    recycleCell(existingPreContentNode);
                return contentId;   // also fine for contentId == NONE
            }

            if (isLeaf(existingPreContentNode))
                return contentId != NONE
                       ? createPrefixNode(contentId, updatedPostContentNode, true)
                       : updatedPostContentNode;

            return applyPrefixChange(updatedPostContentNode,
                                     existingPreContentNode,
                                     existingPostContentNode,
                                     contentId,
                                     forcedCopy);
        }

        private int applyPrefixChange(int updatedPostPrefixNode,
                                      int existingPrePrefixNode,
                                      int existingPostPrefixNode,
                                      int prefixData,
                                      boolean forcedCopy)
        throws TrieSpaceExhaustedException
        {
            boolean prefixWasPresent = existingPrePrefixNode != existingPostPrefixNode;
            boolean prefixWasEmbedded = prefixWasPresent && isEmbeddedPrefixNode(existingPrePrefixNode);
            if (prefixData == NONE)
            {
                if (prefixWasPresent && !prefixWasEmbedded)
                    recycleCell(existingPrePrefixNode);
                return updatedPostPrefixNode;
            }

            boolean childChanged = updatedPostPrefixNode != existingPostPrefixNode;
            boolean dataChanged = !prefixWasPresent || prefixData != getIntVolatile(existingPrePrefixNode + PREFIX_CONTENT_OFFSET);
            if (!childChanged && !dataChanged)
                return existingPrePrefixNode;

            if (forcedCopy)
            {
                if (!childChanged && prefixWasEmbedded)
                {
                    // If we directly create in this case, we will find embedding is possible and will overwrite the
                    // previous value.
                    // We could create a separate metadata node referencing the child, but in that case we'll
                    // use two nodes while one suffices. Instead, copy the child and embed the new metadata.
                    updatedPostPrefixNode = copyCell(existingPostPrefixNode);
                }
                else if (prefixWasPresent && !prefixWasEmbedded)
                {
                    recycleCell(existingPrePrefixNode);
                    // otherwise cell is already recycled by the recycling of the child
                }
                return createPrefixNode(prefixData, updatedPostPrefixNode, isNull(existingPostPrefixNode));
            }

            // We can't update in-place if there was no preexisting prefix, or if the
            // prefix was embedded and the target node must change.
            if (!prefixWasPresent || prefixWasEmbedded && childChanged)
                return createPrefixNode(prefixData, updatedPostPrefixNode, isNull(existingPostPrefixNode));

            // Otherwise modify in place
            if (childChanged) // to use volatile write but also ensure we don't corrupt embedded nodes
                putIntVolatile(existingPrePrefixNode + PREFIX_POINTER_OFFSET, updatedPostPrefixNode);
            if (dataChanged)
                putIntVolatile(existingPrePrefixNode + PREFIX_CONTENT_OFFSET, prefixData);
            return existingPrePrefixNode;
        }

        /**
         * After a node's children are processed, this is called to ascend from it. This means applying the collected
         * content to the compiled updatedPostContentNode and creating a mapping in the parent to it (or updating if
         * one already exists).
         */
        void attachAndMoveToParentState(int forcedCopyDepth) throws TrieSpaceExhaustedException
        {
            int updatedFullNode = applyContent(currentDepth >= forcedCopyDepth);
            int existingFullNode = existingPreContentNode();
            --currentDepth;

            if (updatedFullNode != existingFullNode)
                attachChild(transition(), updatedFullNode, currentDepth >= forcedCopyDepth);
        }

        /**
         * Ascend and update the root at the end of processing.
         */
        void attachRoot(int forcedCopyDepth) throws TrieSpaceExhaustedException
        {
            int updatedPreContentNode = applyContent(0 >= forcedCopyDepth);
            int existingPreContentNode = existingPreContentNode();
            assert root == existingPreContentNode : "Unexpected change to root. Concurrent trie modification?";
            if (updatedPreContentNode != existingPreContentNode)
            {
                // Only write to root if they are different (value doesn't change, but
                // we don't want to invalidate the value in other cores' caches unnecessarily).
                root = updatedPreContentNode;
            }
        }

        public byte[] getBytes()
        {
            int arrSize = currentDepth;
            byte[] data = new byte[arrSize];
            int pos = 0;
            for (int i = 0; i < currentDepth; ++i)
            {
                int trans = transitionAtDepth(i);
                data[pos++] = (byte) trans;
            }
            return data;
        }

        public byte[] getBytes(Predicate<T> shouldStop)
        {
            if (currentDepth == 0)
                return new byte[0];

            int arrSize = 1;
            int i;
            for (i = currentDepth - 1; i > 0; --i)
            {
                int content = contentIdAtDepth(i);
                if (!isNull(content) && shouldStop.test(InMemoryTrie.this.getContent(content)))
                    break;
                ++arrSize;
            }
            assert i > 0 || arrSize == currentDepth; // if the loop covers the whole stack, the array must cover the full depth

            byte[] data = new byte[arrSize];
            int pos = 0;
            for (; i < currentDepth; ++i)
            {
                int trans = transitionAtDepth(i);
                data[pos++] = (byte) trans;
            }
            return data;
        }

        public ByteComparable.Version byteComparableVersion()
        {
            return byteComparableVersion;
        }
    }

    public interface KeyProducer<T>
    {
        /**
         * Get the bytes of the path leading to this node.
         */
        byte[] getBytes();

        /**
         * Get the bytes of the path leading to this node from the closest ancestor whose content, after any new inserts
         * have been applied, satisfies the given predicate.
         * Note that the predicate is not called for the current position, because its content is not yet prepared when
         * the method is being called.
         */
        byte[] getBytes(Predicate<T> shouldStop);

        ByteComparable.Version byteComparableVersion();
    }

    /**
     * Somewhat similar to {@link Trie.MergeResolver}, this encapsulates logic to be applied whenever new content is
     * being upserted into a {@link InMemoryTrie}. Unlike {@link Trie.MergeResolver}, {@link UpsertTransformer} will be
     * applied no matter if there's pre-existing content for that trie key/path or not.
     *
     * @param <T> The content type for this {@link InMemoryTrie}.
     * @param <U> The type of the new content being applied to this {@link InMemoryTrie}.
     */
    public interface UpsertTransformerWithKeyProducer<T, U>
    {
        /**
         * Called when there's content in the updating trie.
         *
         * @param existing Existing content for this key, or null if there isn't any.
         * @param update   The update, always non-null.
         * @param keyState An interface that can be used to retrieve the path of the value being updated.
         * @return The combined value to use.
         */
        T apply(T existing, U update, KeyProducer<T> keyState);
    }

    /**
     * Somewhat similar to {@link Trie.MergeResolver}, this encapsulates logic to be applied whenever new content is
     * being upserted into a {@link InMemoryTrie}. Unlike {@link Trie.MergeResolver}, {@link UpsertTransformer} will be
     * applied no matter if there's pre-existing content for that trie key/path or not.
     * <p>
     * A version of the above that does not use a {@link KeyProducer}.
     *
     * @param <T> The content type for this {@link InMemoryTrie}.
     * @param <U> The type of the new content being applied to this {@link InMemoryTrie}.
     */
    public interface UpsertTransformer<T, U> extends UpsertTransformerWithKeyProducer<T, U>
    {
        /**
         * Called when there's content in the updating trie.
         *
         * @param existing Existing content for this key, or null if there isn't any.
         * @param update   The update, always non-null.
         * @return The combined value to use. Cannot be null.
         */
        T apply(T existing, U update);

        /**
         * Version of the above that also provides the path of a value being updated.
         *
         * @param existing Existing content for this key, or null if there isn't any.
         * @param update   The update, always non-null.
         * @param keyState An interface that can be used to retrieve the path of the value being updated.
         * @return The combined value to use. Cannot be null.
         */
        default T apply(T existing, U update, KeyProducer<T> keyState)
        {
            return apply(existing, update);
        }
    }

    /**
     * Interface providing features of the mutating node during mutation done using {@link #apply}.
     * Effectively a subset of the {@link Trie.Cursor} interface which only permits operations that are safe to
     * perform before iterating the children of the mutation node to apply the branch mutation.
     *
     * This is mainly used as an argument to predicates that decide when to copy substructure when modifying tries,
     * which enables different kinds of atomicity and consistency guarantees.
     *
     * See the InMemoryTrie javadoc or InMemoryTrieThreadedTest for demonstration of the typical usages and what they
     * achieve.
     */
    public interface NodeFeatures<T>
    {
        /**
         * Whether or not the node has more than one descendant. If a checker needs mutations to be atomic, they can
         * return true when this becomes true.
         */
        boolean isBranching();

        /**
         * The metadata associated with the node. If readers need to see a consistent view (i.e. where older updates
         * cannot be missed if a new one is presented) below some specified point (e.g. within a partition), the checker
         * should return true when it identifies that point.
         */
        T content();
    }

    private static class Mutation<T, U> implements NodeFeatures<U>
    {
        final UpsertTransformerWithKeyProducer<T, U> transformer;
        final Predicate<NodeFeatures<U>> needsForcedCopy;
        final Cursor<U> mutationCursor;
        final InMemoryTrie<T>.ApplyState state;
        int forcedCopyDepth;

        Mutation(UpsertTransformerWithKeyProducer<T, U> transformer,
                 Predicate<NodeFeatures<U>> needsForcedCopy,
                 Cursor<U> mutationCursor,
                 InMemoryTrie<T>.ApplyState state)
        {
            assert mutationCursor.depth() == 0 : "Unexpected non-fresh cursor.";
            assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
            this.transformer = transformer;
            this.needsForcedCopy = needsForcedCopy;
            this.mutationCursor = mutationCursor;
            this.state = state;
        }

        void apply() throws TrieSpaceExhaustedException
        {
            int depth = state.currentDepth;
            while (true)
            {
                if (depth <= forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                applyContent();

                depth = mutationCursor.advance();
                if (state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth))
                    break;
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
            }
        }

        void applyContent() throws TrieSpaceExhaustedException
        {
            U content = mutationCursor.content();
            if (content != null)
            {
                T existingContent = state.getContent();
                T combinedContent = transformer.apply(existingContent, content, state);
                state.setContent(combinedContent, // can be null
                                 state.currentDepth >= forcedCopyDepth); // this is called at the start of processing
            }
        }


        void complete() throws TrieSpaceExhaustedException
        {
            assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
            state.attachRoot(forcedCopyDepth);
        }

        @Override
        public boolean isBranching()
        {
            // This is not very efficient, but we only currently use this option in tests.
            // If it's needed for production use, isBranching should be implemented in the cursor interface.
            Cursor<U> dupe = mutationCursor.tailTrie().cursor(Direction.FORWARD);
            int childDepth = dupe.advance();
            return childDepth > 0 &&
                   dupe.skipTo(childDepth, dupe.incomingTransition() + 1) == childDepth;
        }

        @Override
        public U content()
        {
            return mutationCursor.content();
        }
    }

    /**
     * Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
     * with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
     * @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
     * different than the element type for this memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value. Applied even if there's no pre-existing value in the memtable trie.
     * @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
     * concurrent readers. See NodeFeatures for details.
     */
    public <U> void apply(Trie<U> mutation,
                          final UpsertTransformerWithKeyProducer<T, U> transformer,
                          final Predicate<NodeFeatures<U>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        try
        {
            Mutation<T, U> m = new Mutation<>(transformer,
                                              needsForcedCopy,
                                              mutation.cursor(Direction.FORWARD),
                                              applyState.start());
            m.apply();
            m.complete();
            completeMutation();
        }
        catch (Throwable t)
        {
            abortMutation();
            throw t;
        }
    }

    /**
     * Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
     * with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
     * @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
     * different than the element type for this memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value. Applied even if there's no pre-existing value in the memtable trie.
     * @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
     * concurrent readers. See NodeFeatures for details.
     */
    public <U> void apply(Trie<U> mutation,
                          final UpsertTransformer<T, U> transformer,
                          final Predicate<NodeFeatures<U>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        apply(mutation, (UpsertTransformerWithKeyProducer<T, U>) transformer, needsForcedCopy);
    }

    /**
     * Map-like put method, using the apply machinery above which cannot run into stack overflow. When the correct
     * position in the trie has been reached, the value will be resolved with the given function before being placed in
     * the trie (even if there's no pre-existing content in this trie).
     * @param key the trie path/key for the given value.
     * @param value the value being put in the memtable trie. Note that it can be of type different than the element
     * type for this memtable trie. It's up to the {@code transformer} to return the final value that will stay in
     * the memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
     * even if there's no pre-existing value in the memtable trie.
     */
    public <R> void putSingleton(ByteComparable key,
                                 R value,
                                 UpsertTransformer<T, ? super R> transformer) throws TrieSpaceExhaustedException
    {
        apply(Trie.singleton(key, byteComparableVersion, value), transformer, Predicates.alwaysFalse());
    }

    /**
     * A version of putSingleton which uses recursive put if the last argument is true.
     */
    public <R> void putSingleton(ByteComparable key,
                                 R value,
                                 UpsertTransformer<T, ? super R> transformer,
                                 boolean useRecursive) throws TrieSpaceExhaustedException
    {
        if (useRecursive)
            putRecursive(key, value, transformer);
        else
            putSingleton(key, value, transformer);
    }

    /**
     * Map-like put method, using a fast recursive implementation through the key bytes. May run into stack overflow if
     * the trie becomes too deep. When the correct position in the trie has been reached, the value will be resolved
     * with the given function before being placed in the trie (even if there's no pre-existing content in this trie).
     * @param key the trie path/key for the given value.
     * @param value the value being put in the memtable trie. Note that it can be of type different than the element
     * type for this memtable trie. It's up to the {@code transformer} to return the final value that will stay in
     * the memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
     * even if there's no pre-existing value in the memtable trie.
     */
    public <R> void putRecursive(ByteComparable key, R value, final UpsertTransformer<T, R> transformer) throws TrieSpaceExhaustedException
    {
        try
        {
            int newRoot = putRecursive(root, key.asComparableBytes(byteComparableVersion), value, transformer);
            if (newRoot != root)
                root = newRoot;
            completeMutation();
        }
        catch (Throwable t)
        {
            abortMutation();
            throw t;
        }
    }

    private <R> int putRecursive(int node, ByteSource key, R value, final UpsertTransformer<T, R> transformer) throws TrieSpaceExhaustedException
    {
        int transition = key.next();
        if (transition == ByteSource.END_OF_STREAM)
            return applyContent(node, value, transformer);

        int child = getChild(node, transition);

        int newChild = putRecursive(child, key, value, transformer);
        if (newChild == child)
            return node;

        int skippedContent = followContentTransition(node);
        int attachedChild = !isNull(skippedContent)
                            ? attachChild(skippedContent, transition, newChild)  // Single path, no copying required
                            : expandOrCreateChainNode(transition, newChild);

        return preserveContent(node, skippedContent, attachedChild, false);
    }

    private <R> int applyContent(int node, R value, UpsertTransformer<T, R> transformer) throws TrieSpaceExhaustedException
    {
        if (isNull(node))
            return addContent(transformer.apply(null, value));

        if (isLeaf(node))
        {
            int contentId = node;
            setContent(contentId, transformer.apply(getContent(contentId), value));
            return node;
        }

        if (offset(node) == PREFIX_OFFSET)
        {
            int contentId = getIntVolatile(node + PREFIX_CONTENT_OFFSET);
            setContent(contentId, transformer.apply(getContent(contentId), value));
            return node;
        }
        else
            return createPrefixNode(addContent(transformer.apply(null, value)), node, false);
    }

    private void completeMutation()
    {
        cellAllocator.completeMutation();
        objectAllocator.completeMutation();
    }

    private void abortMutation()
    {
        cellAllocator.abortMutation();
        objectAllocator.abortMutation();
    }

    /**
     * Returns true if the allocation threshold has been reached. To be called by the the writing thread (ideally, just
     * after the write completes). When this returns true, the user should switch to a new trie as soon as feasible.
     * <p>
     * The trie expects up to 10% growth above this threshold. Any growth beyond that may be done inefficiently, and
     * the trie will fail altogether when the size grows beyond 2G - 256 bytes.
     */
    public boolean reachedAllocatedSizeThreshold()
    {
        return allocatedPos >= ALLOCATED_SIZE_THRESHOLD;
    }

    /**
     * For tests only! Advance the allocation pointer (and allocate space) by this much to test behaviour close to
     * full.
     */
    @VisibleForTesting
    int advanceAllocatedPos(int wantedPos) throws TrieSpaceExhaustedException
    {
        while (allocatedPos < wantedPos)
            allocateCell();
        return allocatedPos;
    }

    /**
     * For tests only! Returns the current allocation position.
     */
    @VisibleForTesting
    int getAllocatedPos()
    {
        return allocatedPos;
    }

    /**
     * Returns the off heap size of the memtable trie itself, not counting any space taken by referenced content, or
     * any space that has been allocated but is not currently in use (e.g. recycled cells or preallocated buffer).
     * The latter means we are undercounting the actual usage, but the purpose of this reporting is to decide when
     * to flush out e.g. a memtable and if we include the unused space we would almost always end up flushing out
     * immediately after allocating a large buffer and not having a chance to use it. Counting only used space makes it
     * possible to flush out before making these large allocations.
     */
    public long usedSizeOffHeap()
    {
        return bufferType == BufferType.ON_HEAP ? 0 : usedBufferSpace();
    }

    /**
     * Returns the on heap size of the memtable trie itself, not counting any space taken by referenced content, or
     * any space that has been allocated but is not currently in use (e.g. recycled cells or preallocated buffer).
     * The latter means we are undercounting the actual usage, but the purpose of this reporting is to decide when
     * to flush out e.g. a memtable and if we include the unused space we would almost always end up flushing out
     * immediately after allocating a large buffer and not having a chance to use it. Counting only used space makes it
     * possible to flush out before making these large allocations.
     */
    public long usedSizeOnHeap()
    {
        return usedObjectSpace() +
               REFERENCE_ARRAY_ON_HEAP_SIZE * getBufferIdx(contentCount, CONTENTS_START_SHIFT, CONTENTS_START_SIZE) +
               (bufferType == BufferType.ON_HEAP ? usedBufferSpace() + EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP) +
               REFERENCE_ARRAY_ON_HEAP_SIZE * getBufferIdx(allocatedPos, BUF_START_SHIFT, BUF_START_SIZE);
    }

    private long usedBufferSpace()
    {
        return allocatedPos - cellAllocator.indexCountInPipeline() * CELL_SIZE;
    }

    private long usedObjectSpace()
    {
        return (contentCount - objectAllocator.indexCountInPipeline()) * MemoryLayoutSpecification.SPEC.getReferenceSize();
    }

    /**
     * Returns the amount of memory that has been allocated for various buffers but isn't currently in use.
     * The total on-heap space used by the trie is {@code usedSizeOnHeap() + unusedReservedOnHeapMemory()}.
     */
    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        int bufferOverhead = 0;
        if (bufferType == BufferType.ON_HEAP)
        {
            int pos = this.allocatedPos;
            UnsafeBuffer buffer = getBuffer(pos);
            if (buffer != null)
                bufferOverhead = buffer.capacity() - inBufferOffset(pos);
            bufferOverhead += cellAllocator.indexCountInPipeline() * CELL_SIZE;
        }

        int index = contentCount;
        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> contentArray = contentArrays[leadBit];
        int contentOverhead = ((contentArray != null ? contentArray.length() : 0) - ofs);
        contentOverhead += objectAllocator.indexCountInPipeline();
        contentOverhead *= MemoryLayoutSpecification.SPEC.getReferenceSize();

        return bufferOverhead + contentOverhead;
    }

    public int valuesCount()
    {
        return contentCount;
    }

    /**
     * Release all recycled content references, including the ones waiting in still incomplete recycling lists.
     * This is a test method and can cause null pointer exceptions if used on a live trie.
     * <p>
     * If similar functionality is required for non-test purposes, a version of this should be developed that only
     * releases references on barrier-complete lists.
     */
    @VisibleForTesting
    public void releaseReferencesUnsafe()
    {
        for (int idx : objectAllocator.indexesInPipeline())
            setContent(~idx, null);
    }
}
