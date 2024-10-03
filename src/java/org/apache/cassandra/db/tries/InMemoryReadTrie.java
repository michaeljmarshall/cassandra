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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * In-memory trie built for fast modification and reads executing concurrently with writes from a single mutator thread.
 *
 * This class provides the read-only functionality, expanded in {@link InMemoryTrie} to writes.
 */
public class InMemoryReadTrie<T> extends Trie<T>
{
    /*
    TRIE FORMAT AND NODE TYPES

    The in-memory trie uses five different types of nodes:
     - "leaf" nodes, which have content and no children;
     - single-transition "chain" nodes, which have exactly one child; while each node is a single transition, they are
       called "chain" because multiple such transition are packed in a cell.
     - "sparse" nodes which have between two and six children;
     - "split" nodes for anything above six children;
     - "prefix" nodes that augment one of the other types (except leaf) with content.

    The data for all nodes except leaf ones is stored in a contiguous 'node buffer' and laid out in cells of 32 bytes.
    A cell only contains data for a single type of node, but there is no direct correspondence between cell and node
    in that:
     - a single cell can contain multiple "chain" nodes.
     - a sparse node occupies exactly one cell.
     - a split node occupies a variable number of cells.
     - a prefix node can be placed in the same cell as the node it augments, or in a separate cell.

    Nodes are referenced in that buffer by an integer position/pointer, the 'node pointer'. Note that node pointers are
    not pointing at the beginning of cells, and we call 'pointer offset' the offset of the node pointer to the cell it
    points into. The value of a 'node pointer' is used to decide what kind of node is pointed:

     - If the pointer is negative, we have a leaf node. Since a leaf has no children, we need no data outside of its
       content to represent it, and that content is stored in a 'content list', not in the nodes buffer. The content
       of a particular leaf node is located at the ~pointer position in the content list (~ instead of - so that -1 can
       correspond to position 0).

     - If the 'pointer offset' is smaller than 28, we have a chain node with one transition. The transition character is
       the byte at the position pointed in the 'node buffer', and the child is pointed by:
       - the integer value at offset 28 of the cell pointed if the 'pointer offset' is 27
       - pointer + 1 (which is guaranteed to have offset smaller than 28, i.e. to be a chain node), otherwise
       In other words, a chain cell contains a sequence of characters that leads to the child whose address is at
       offset 28. It may have between 1 and 28 characters depending on the pointer with which the cell is entered.

     - If the 'pointer offset' is 30, we have a sparse node. The data of a sparse node occupies a full cell and is laid
       out as:
       - six pointers to children at offsets 0 to 24
       - six transition characters at offsets 24 to 30
       - an order word stored in the two bytes at offset 30
       To enable in-place addition of children, the pointers and transition characters are not stored ordered.
       Instead, we use an order encoding in the last 2 bytes of the node. The encoding is a base-6 number which
       describes the order of the transitions (least significant digit being the smallest).
       The node must have at least two transitions and the transition at position 0 is never the biggest (we can
       enforce this by choosing for position 0 the smaller of the two transitions a sparse node starts with). This
       allows iteration over the order word (which divides said word by 6 each step) to finish when the result becomes 0.

     - If the 'pointer offset' is 28, the node is a split one. Split nodes are dense, meaning that there is a direct
       mapping between a transition character and the address of the associated pointer, and new children can easily be
       added in place.
       Split nodes occupy multiple cells, and a child is located by traversing 3 layers of pointers:
       - the first pointer is within the top-level cell (the one pointed by the pointer) and points to a "mid" cell.
         The top-level cell has 4 such pointers to "mid" cell, located between offset 16 and 32.
       - the 2nd pointer is within the "mid" cell and points to a "tail" cell. A "mid" cell has 8 such pointers
         occupying the whole cell.
       - the 3rd pointer is with the "tail" cell and is the actual child pointer. Like "mid" cell, there are 8 such
         pointers (so we finally address 4 * 8 * 8 = 256 children).
       To find a child, we thus need to know the index of the pointer to follow within the top-level cell, the index
       of the one in the "mid" cell and the index in the "tail" cell. For that, we split the transition byte in a
       sequence of 2-3-3 bits:
       - the first 2 bits are the index in the top-level cell;
       - the next 3 bits, the index in the "mid" cell;
       - and the last 3 bits the index in the "tail" cell.
       This layout allows the node to use the smaller fixed-size cells (instead of 256*4 bytes for the whole character
       space) and also leaves some room in the head cell (the 16 first bytes) for additional information (which we can
       use to store prefix nodes containing things like deletion times).
       One split node may need up to 1 + 4 + 4*8 cells (1184 bytes) to store all its children.

     - If the pointer offset is 31, we have a prefix node. These are two types:
       -- Embedded prefix nodes occupy the free bytes in a chain or split node. The byte at offset 4 has the offset
          within the 32-byte cell for the augmented node.
       -- Full prefix nodes have 0xFF at offset 4 and a pointer at 28, pointing to the augmented node.
       Both types contain an index for content at offset 0. The augmented node cannot be a leaf or NONE -- in the former
       case the leaf itself contains the content index, in the latter we use a leaf instead.
       The term "node" when applied to these is a bit of a misnomer as they are not presented as separate nodes during
       traversals. Instead, they augment a node, changing only its content. Internally we create a Node object for the
       augmented node and wrap a PrefixNode around it, which changes the `content()` method and routes all other
       calls to the augmented node's methods.

     When building a trie we first allocate the content, then create a chain node leading to it. While we only have
     single transitions leading to a chain node, we can expand that node (attaching a character and using pointer - 1)
     instead of creating a new one. When a chain node already has a child and needs a new one added we change the type
     (i.e. create a new node and remap the parent) to sparse with two children. When a six-child sparse node needs a new
     child, we switch to split.

     Cells can be reused once they are no longer used and cannot be in the state of a concurrently running reader. See
     MemoryAllocationStrategy for details.

     For further descriptions and examples of the mechanics of the trie, see InMemoryTrie.md.
     */

    static final int CELL_SIZE = 32;

    // Biggest cell offset that can contain a pointer.
    static final int LAST_POINTER_OFFSET = CELL_SIZE - 4;

    /*
     Cell offsets used to identify node types (by comparing them to the node 'pointer offset').
     */

    // split node (dense, 2-3-3 transitions), laid out as 4 pointers to "mid" cell, with has 8 pointers to "tail" cell,
    // which has 8 pointers to children
    static final int SPLIT_OFFSET = CELL_SIZE - 4;
    // sparse node, unordered list of up to 6 transition, laid out as 6 transition pointers followed by 6 transition
    // bytes. The last two bytes contain an ordering of the transitions (in base-6) which is used for iteration. On
    // update the pointer is set last, i.e. during reads the node may show that a transition exists and list a character
    // for it, but pointer may still be null.
    static final int SPARSE_OFFSET = CELL_SIZE - 2;
    // min and max offset for a chain node. A cell of chain node is laid out as a pointer at LAST_POINTER_OFFSET,
    // preceded by characters that lead to it. Thus a full chain cell contains CELL_SIZE-4 transitions/chain nodes.
    static final int CHAIN_MIN_OFFSET = 0;
    static final int CHAIN_MAX_OFFSET = CELL_SIZE - 5;
    // Prefix node, an intermediate node augmenting its child node with content.
    static final int PREFIX_OFFSET = CELL_SIZE - 1;

    /*
     Offsets and values for navigating in a cell for particular node type. Those offsets are 'from the node pointer'
     (not the cell start) and can be thus negative since node pointers points towards the end of cells.
     */

    // Limit for the starting cell / sublevel (2 bits -> 4 pointers).
    static final int SPLIT_START_LEVEL_LIMIT = 4;
    // Limit for the other sublevels (3 bits -> 8 pointers).
    static final int SPLIT_OTHER_LEVEL_LIMIT = 8;
    // Bitshift between levels.
    static final int SPLIT_LEVEL_SHIFT = 3;

    static final int SPARSE_CHILD_COUNT = 6;
    // Offset to the first child pointer of a spare node (laid out from the start of the cell)
    static final int SPARSE_CHILDREN_OFFSET = 0 - SPARSE_OFFSET;
    // Offset to the first transition byte of a sparse node (laid out after the child pointers)
    static final int SPARSE_BYTES_OFFSET = SPARSE_CHILD_COUNT * 4 - SPARSE_OFFSET;
    // Offset to the order word of a sparse node (laid out after the children (pointer + transition byte))
    static final int SPARSE_ORDER_OFFSET = SPARSE_CHILD_COUNT * 5 - SPARSE_OFFSET;  // 0

    // Offset of the flag byte in a prefix node. In shared cells, this contains the offset of the next node.
    static final int PREFIX_FLAGS_OFFSET = 4 - PREFIX_OFFSET;
    // Offset of the content id
    static final int PREFIX_CONTENT_OFFSET = 0 - PREFIX_OFFSET;
    // Offset of the next pointer in a non-shared prefix node
    static final int PREFIX_POINTER_OFFSET = LAST_POINTER_OFFSET - PREFIX_OFFSET;

    /**
     * Value used as null for node pointers.
     * No node can use this address (we enforce this by not allowing chain nodes to grow to position 0).
     * Do not change this as the code relies there being a NONE placed in all bytes of the cell that are not set.
     */
    static final int NONE = 0;

    volatile int root;

    /*
     EXPANDABLE DATA STORAGE

     The tries will need more and more space in buffers and content lists as they grow. Instead of using ArrayList-like
     reallocation with copying, which may be prohibitively expensive for large buffers, we use a sequence of
     buffers/content arrays that double in size on every expansion.

     For a given address x the index of the buffer can be found with the following calculation:
        index_of_most_significant_set_bit(x / min_size + 1)
     (relying on sum (2^i) for i in [0, n-1] == 2^n - 1) which can be performed quickly on modern hardware.

     Finding the offset within the buffer is then
        x + min - (min << buffer_index)

     The allocated space starts 256 bytes for the buffer and 16 entries for the content list.

     Note that a buffer is not allowed to split 32-byte cells (code assumes same buffer can be used for all bytes
     inside the cell).
     */

    static final int BUF_START_SHIFT = 8;
    static final int BUF_START_SIZE = 1 << BUF_START_SHIFT;

    static final int CONTENTS_START_SHIFT = 4;
    static final int CONTENTS_START_SIZE = 1 << CONTENTS_START_SHIFT;

    static
    {
        assert BUF_START_SIZE % CELL_SIZE == 0 : "Initial buffer size must fit a full cell.";
    }

    final UnsafeBuffer[] buffers;
    final AtomicReferenceArray<T>[] contentArrays;
    final ByteComparable.Version byteComparableVersion;

    InMemoryReadTrie(ByteComparable.Version byteComparableVersion, UnsafeBuffer[] buffers, AtomicReferenceArray<T>[] contentArrays, int root)
    {
        this.byteComparableVersion = byteComparableVersion;
        this.buffers = buffers;
        this.contentArrays = contentArrays;
        this.root = root;
    }

    /*
     Buffer, content list and cell management
     */
    int getBufferIdx(int pos, int minBufferShift, int minBufferSize)
    {
        return 31 - minBufferShift - Integer.numberOfLeadingZeros(pos + minBufferSize);
    }

    int inBufferOffset(int pos, int bufferIndex, int minBufferSize)
    {
        return pos + minBufferSize - (minBufferSize << bufferIndex);
    }

    UnsafeBuffer getBuffer(int pos)
    {
        int leadBit = getBufferIdx(pos, BUF_START_SHIFT, BUF_START_SIZE);
        return buffers[leadBit];
    }

    int inBufferOffset(int pos)
    {
        int leadBit = getBufferIdx(pos, BUF_START_SHIFT, BUF_START_SIZE);
        return inBufferOffset(pos, leadBit, BUF_START_SIZE);
    }


    /**
     * Pointer offset for a node pointer.
     */
    int offset(int pos)
    {
        return pos & (CELL_SIZE - 1);
    }

    final int getUnsignedByte(int pos)
    {
        return getBuffer(pos).getByte(inBufferOffset(pos)) & 0xFF;
    }

    final int getUnsignedShortVolatile(int pos)
    {
        return getBuffer(pos).getShortVolatile(inBufferOffset(pos)) & 0xFFFF;
    }

    /**
     * Following a pointer must be done using a volatile read to enforce happens-before between reading the node we
     * advance to and the preparation of that node that finishes in a volatile write of the pointer that makes it
     * visible.
     */
    final int getIntVolatile(int pos)
    {
        return getBuffer(pos).getIntVolatile(inBufferOffset(pos));
    }

    /**
     * Get the content for the given content pointer.
     *
     * @param id content pointer, encoded as ~index where index is the position in the content array.
     * @return the current content value.
     */
    T getContent(int id)
    {
        int leadBit = getBufferIdx(~id, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(~id, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        return array.get(ofs);
    }

    /*
     Reading node content
     */

    boolean isNull(int node)
    {
        return node == NONE;
    }

    boolean isLeaf(int node)
    {
        return node < NONE;
    }

    boolean isNullOrLeaf(int node)
    {
        return node <= NONE;
    }

    /**
     * Returns the number of transitions in a chain cell entered with the given pointer.
     */
    private int chainCellLength(int node)
    {
        return LAST_POINTER_OFFSET - offset(node);
    }

    /**
     * Get a node's child for the given transition character
     */
    int getChild(int node, int trans)
    {
        if (isNullOrLeaf(node))
            return NONE;

        node = followContentTransition(node);

        switch (offset(node))
        {
            case SPARSE_OFFSET:
                return getSparseChild(node, trans);
            case SPLIT_OFFSET:
                return getSplitChild(node, trans);
            case CHAIN_MAX_OFFSET:
                if (trans != getUnsignedByte(node))
                    return NONE;
                return getIntVolatile(node + 1);
            default:
                if (trans != getUnsignedByte(node))
                    return NONE;
                return node + 1;
        }
    }

    protected int followContentTransition(int node)
    {
        if (isNullOrLeaf(node))
            return NONE;

        if (offset(node) == PREFIX_OFFSET)
        {
            int b = getUnsignedByte(node + PREFIX_FLAGS_OFFSET);
            if (b < CELL_SIZE)
                node = node - PREFIX_OFFSET + b;
            else
                node = getIntVolatile(node + PREFIX_POINTER_OFFSET);

            assert node >= 0 && offset(node) != PREFIX_OFFSET;
        }
        return node;
    }

    /**
     * Advance as long as the cell pointed to by the given pointer will let you.
     * <p>
     * This is the same as getChild(node, first), except for chain nodes where it would walk the fill chain as long as
     * the input source matches.
     */
    int advance(int node, int first, ByteSource rest)
    {
        if (isNullOrLeaf(node))
            return NONE;

        node = followContentTransition(node);

        switch (offset(node))
        {
            case SPARSE_OFFSET:
                return getSparseChild(node, first);
            case SPLIT_OFFSET:
                return getSplitChild(node, first);
            default:
                // Check the first byte matches the expected
                if (getUnsignedByte(node++) != first)
                    return NONE;
                // Check the rest of the bytes provided by the chain node
                for (int length = chainCellLength(node); length > 0; --length)
                {
                    first = rest.next();
                    if (getUnsignedByte(node++) != first)
                        return NONE;
                }
                // All bytes matched, node is now positioned on the child pointer. Follow it.
                return getIntVolatile(node);
        }
    }

    /**
     * Get the child for the given transition character, knowing that the node is sparse
     */
    int getSparseChild(int node, int trans)
    {
        for (int i = 0; i < SPARSE_CHILD_COUNT; ++i)
        {
            if (getUnsignedByte(node + SPARSE_BYTES_OFFSET + i) == trans)
            {
                int child = getIntVolatile(node + SPARSE_CHILDREN_OFFSET + i * 4);

                // we can't trust the transition character read above, because it may have been fetched before a
                // concurrent update happened, and the update may have managed to modify the pointer by now.
                // However, if we read it now that we have accessed the volatile pointer, it must have the correct
                // value as it is set before the pointer.
                if (child != NONE && getUnsignedByte(node + SPARSE_BYTES_OFFSET + i) == trans)
                    return child;
            }
        }
        return NONE;
    }

    /**
     * Given a transition, returns the corresponding index (within the node cell) of the pointer to the mid cell of
     * a split node.
     */
    int splitNodeMidIndex(int trans)
    {
        // first 2 bits of the 2-3-3 split
        return (trans >> 6) & 0x3;
    }

    /**
     * Given a transition, returns the corresponding index (within the mid cell) of the pointer to the tail cell of
     * a split node.
     */
    int splitNodeTailIndex(int trans)
    {
        // second 3 bits of the 2-3-3 split
        return (trans >> 3) & 0x7;
    }

    /**
     * Given a transition, returns the corresponding index (within the tail cell) of the pointer to the child of
     * a split node.
     */
    int splitNodeChildIndex(int trans)
    {
        // third 3 bits of the 2-3-3 split
        return trans & 0x7;
    }

    /**
     * Get the child for the given transition character, knowing that the node is split
     */
    int getSplitChild(int node, int trans)
    {
        int mid = getSplitCellPointer(node, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        if (isNull(mid))
            return NONE;

        int tail = getSplitCellPointer(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        if (isNull(tail))
            return NONE;
        return getSplitCellPointer(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
    }

    /**
     * Get the content for a given node
     */
    T getNodeContent(int node)
    {
        if (isLeaf(node))
            return getContent(node);

        if (offset(node) != PREFIX_OFFSET)
            return null;

        int index = getIntVolatile(node + PREFIX_CONTENT_OFFSET);
        return (isLeaf(index))
               ? getContent(index)
               : null;
    }

    int splitCellPointerAddress(int node, int childIndex, int subLevelLimit)
    {
        return node - SPLIT_OFFSET + (8 - subLevelLimit + childIndex) * 4;
    }

    int getSplitCellPointer(int node, int childIndex, int subLevelLimit)
    {
        return getIntVolatile(splitCellPointerAddress(node, childIndex, subLevelLimit));
    }

    /**
     * Backtracking state for a cursor.
     *
     * To avoid allocations and pointer-chasing, the backtracking data is stored in a simple int array with
     * BACKTRACK_INTS_PER_ENTRY ints for each level.
     */
    private static class CursorBacktrackingState
    {
        static final int BACKTRACK_INTS_PER_ENTRY = 3;
        static final int BACKTRACK_INITIAL_SIZE = 16;
        private int[] backtrack = new int[BACKTRACK_INITIAL_SIZE * BACKTRACK_INTS_PER_ENTRY];
        int backtrackDepth = 0;

        void addBacktrack(int node, int data, int depth)
        {
            if (backtrackDepth * BACKTRACK_INTS_PER_ENTRY >= backtrack.length)
                backtrack = Arrays.copyOf(backtrack, backtrack.length * 2);
            backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 0] = node;
            backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 1] = data;
            backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 2] = depth;
            ++backtrackDepth;
        }

        int node(int backtrackDepth)
        {
            return backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 0];
        }

        int data(int backtrackDepth)
        {
            return backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 1];
        }

        int depth(int backtrackDepth)
        {
            return backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 2];
        }
    }

    /*
     * Cursor implementation.
     *
     * InMemoryTrie cursors maintain their backtracking state in CursorBacktrackingState where they store
     * information about the node to backtrack to and the transitions still left to take or attempt.
     *
     * This information is different for the different types of node:
     * - for leaf and chain no backtracking is saved (because we know there are no further transitions)
     * - for sparse we store the remainder of the order word
     * - for split we store one entry per sub-level of the 2-3-3 split
     *
     * When the cursor is asked to advance it first checks the current node for children, and if there aren't any
     * (i.e. it is positioned on a leaf node), it goes one level up the backtracking chain, where we are guaranteed to
     * have a remaining child to advance to. When there's nothing to backtrack to, the trie is exhausted.
     */
    class InMemoryCursor extends CursorBacktrackingState implements Cursor<T>
    {
        private int currentNode;
        private int currentFullNode;
        private int incomingTransition;
        private T content;
        private final Direction direction;
        int depth = -1;

        InMemoryCursor(Direction direction)
        {
            this.direction = direction;
            descendInto(root, -1);
        }

        @Override
        public int advance()
        {
            if (isNullOrLeaf(currentNode))
                return backtrack();
            else
                return advanceToFirstChild(currentNode);
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            int node = currentNode;
            if (!isChainNode(node))
                return advance();

            // Jump directly to the chain's child.
            UnsafeBuffer buffer = getBuffer(node);
            int inBufferNode = inBufferOffset(node);
            int bytesJumped = chainCellLength(node) - 1;   // leave the last byte for incomingTransition
            if (receiver != null && bytesJumped > 0)
                receiver.addPathBytes(buffer, inBufferNode, bytesJumped);
            depth += bytesJumped;    // descendInto will add one
            inBufferNode += bytesJumped;

            // inBufferNode is now positioned on the last byte of the chain.
            // Consume it to be the new state's incomingTransition.
            int transition = buffer.getByte(inBufferNode++) & 0xFF;
            // inBufferNode is now positioned on the child pointer.
            int child = buffer.getIntVolatile(inBufferNode);
            return descendInto(child, transition);
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            if (skipDepth > depth)
            {
                // Descent requested. Jump to the given child transition or greater, and backtrack if there's no such.
                assert skipDepth == depth + 1;
                int advancedDepth = advanceToChildWithTarget(currentNode, skipTransition);
                if (advancedDepth < 0)
                    return backtrack();

                assert advancedDepth == skipDepth;
                return advancedDepth;
            }

            // Backtrack until we reach the requested depth. Note that we may have more than one entry for a given
            // depth (split sublevels) and we ascend through them individually.
            while (--backtrackDepth >= 0)
            {
                depth = depth(backtrackDepth);

                if (depth < skipDepth - 1)
                    return advanceToNextChild(node(backtrackDepth), data(backtrackDepth));

                if (depth == skipDepth - 1)
                {
                    int advancedDepth = advanceToNextChildWithTarget(node(backtrackDepth), data(backtrackDepth), skipTransition);
                    if (advancedDepth >= 0)
                        return advancedDepth;
                }
            }
            return exhausted();
        }

        @Override
        public int depth()
        {
            return depth;
        }

        @Override
        public T content()
        {
            return content;
        }

        @Override
        public int incomingTransition()
        {
            return incomingTransition;
        }

        @Override
        public Direction direction()
        {
            return direction;
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return byteComparableVersion;
        }

        @Override
        public Trie<T> tailTrie()
        {
            assert depth >= 0 : "tailTrie called on exhausted cursor";
            return new InMemoryReadTrie<>(byteComparableVersion, buffers, contentArrays, currentFullNode);
        }

        private int exhausted()
        {
            depth = -1;
            incomingTransition = -1;
            currentFullNode = NONE;
            currentNode = NONE;
            content = null;
            return -1;
        }

        private int backtrack()
        {
            if (--backtrackDepth < 0)
                return exhausted();

            depth = depth(backtrackDepth);
            return advanceToNextChild(node(backtrackDepth), data(backtrackDepth));
        }

        private int advanceToFirstChild(int node)
        {
            assert (!isNullOrLeaf(node));

            switch (offset(node))
            {
                case SPLIT_OFFSET:
                    return descendInSplitSublevel(node, SPLIT_START_LEVEL_LIMIT, 0, SPLIT_LEVEL_SHIFT * 2);
                case SPARSE_OFFSET:
                    return nextValidSparseTransition(node, prepareOrderWord(node));
                default:
                    return getChainTransition(node);
            }
        }

        private int advanceToChildWithTarget(int node, int skipTransition)
        {
            if (isNullOrLeaf(node))
                return -1;

            switch (offset(node))
            {
                case SPLIT_OFFSET:
                    return descendInSplitSublevelWithTarget(node, SPLIT_START_LEVEL_LIMIT, 0, SPLIT_LEVEL_SHIFT * 2, skipTransition);
                case SPARSE_OFFSET:
                    return advanceToSparseTransition(node, prepareOrderWord(node), skipTransition);
                default:
                    return advanceToChainTransition(node, skipTransition);
            }
        }

        private int advanceToNextChild(int node, int data)
        {
            assert (!isNullOrLeaf(node));

            switch (offset(node))
            {
                case SPLIT_OFFSET:
                    return nextValidSplitTransition(node, data);
                case SPARSE_OFFSET:
                    return nextValidSparseTransition(node, data);
                default:
                    throw new AssertionError("Unexpected node type in backtrack state.");
            }
        }

        private int advanceToNextChildWithTarget(int node, int data, int transition)
        {
            assert (!isNullOrLeaf(node));

            switch (offset(node))
            {
                case SPLIT_OFFSET:
                    return advanceToSplitTransition(node, data, transition);
                case SPARSE_OFFSET:
                    return advanceToSparseTransition(node, data, transition);
                default:
                    throw new AssertionError("Unexpected node type in backtrack state.");
            }
        }

        /**
         * Descend into the sub-levels of a split node. Advances to the first child and creates backtracking entries
         * for the following ones. We use the bits of trans (lowest non-zero ones) to identify which sub-level an
         * entry refers to.
         *
         * @param node The node or cell id, must have offset SPLIT_OFFSET.
         * @param limit The transition limit for the current sub-level (4 for the start, 8 for the others).
         * @param collected The transition bits collected from the parent chain (e.g. 0x40 after following 1 on the top
         *                  sub-level).
         * @param shift This level's bit shift (6 for start, 3 for mid and 0 for tail).
         * @return the depth reached after descending.
         */
        int descendInSplitSublevel(int node, int limit, int collected, int shift)
        {
            while (true)
            {
                assert offset(node) == SPLIT_OFFSET;
                int childIndex;
                int child = NONE;
                // find the first non-null child
                for (childIndex = direction.select(0, limit - 1);
                     direction.inLoop(childIndex, 0, limit - 1);
                     childIndex += direction.increase)
                {
                    child = getSplitCellPointer(node, childIndex, limit);
                    if (!isNull(child))
                        break;
                }
                // there must be at least one child
                assert childIndex < limit && child != NONE;

                // look for any more valid transitions and add backtracking if found
                maybeAddSplitBacktrack(node, childIndex, limit, collected, shift);

                // add the bits just found
                collected |= childIndex << shift;
                // descend to next sub-level or child
                if (shift == 0)
                    return descendInto(child, collected);
                // continue with next sublevel; same as
                // return descendInSplitSublevel(child + SPLIT_OFFSET, 8, collected, shift - 3)
                node = child;
                limit = SPLIT_OTHER_LEVEL_LIMIT;
                shift -= SPLIT_LEVEL_SHIFT;
            }
        }

        /**
         * As above, but also makes sure that the descend selects a value at least as big as the given
         * {@code minTransition}.
         */
        private int descendInSplitSublevelWithTarget(int node, int limit, int collected, int shift, int minTransition)
        {
            minTransition -= collected;
            if (minTransition >= limit << shift || minTransition < 0)
                return -1;

            while (true)
            {
                assert offset(node) == SPLIT_OFFSET;
                int childIndex;
                int child = NONE;
                boolean isExact = true;
                // find the first non-null child beyond minTransition
                for (childIndex = minTransition >> shift;
                     direction.inLoop(childIndex, 0, limit - 1);
                     childIndex += direction.increase)
                {
                    child = getSplitCellPointer(node, childIndex, limit);
                    if (!isNull(child))
                        break;
                    isExact = false;
                }
                if (!isExact && (childIndex == limit || childIndex == -1))
                    return -1;

                // look for any more valid transitions and add backtracking if found
                maybeAddSplitBacktrack(node, childIndex, limit, collected, shift);

                // add the bits just found
                collected |= childIndex << shift;
                // descend to next sub-level or child
                if (shift == 0)
                    return descendInto(child, collected);

                if (isExact)
                    minTransition -= childIndex << shift;
                else
                    minTransition = direction.select(0, (1 << shift) - 1);

                // continue with next sublevel; same as
                // return descendInSplitSublevelWithTarget(child + SPLIT_OFFSET, 8, collected, shift - 3, minTransition)
                node = child;
                limit = SPLIT_OTHER_LEVEL_LIMIT;
                shift -= SPLIT_LEVEL_SHIFT;
            }
        }

        /**
         * Backtrack to a split sub-level. The level is identified by the lowest non-0 bits in data.
         */
        int nextValidSplitTransition(int node, int data)
        {
            // Note: This is equivalent to return advanceToSplitTransition(node, data, data) but quicker.
            assert data >= 0 && data <= 0xFF;
            int childIndex = splitNodeChildIndex(data);
            if (childIndex != direction.select(0, SPLIT_OTHER_LEVEL_LIMIT - 1))
            {
                maybeAddSplitBacktrack(node,
                                       childIndex,
                                       SPLIT_OTHER_LEVEL_LIMIT,
                                       data & -(1 << (SPLIT_LEVEL_SHIFT * 1)),
                                       SPLIT_LEVEL_SHIFT * 0);
                int child = getSplitCellPointer(node, childIndex, SPLIT_OTHER_LEVEL_LIMIT);
                return descendInto(child, data);
            }
            int tailIndex = splitNodeTailIndex(data);
            if (tailIndex != direction.select(0, SPLIT_OTHER_LEVEL_LIMIT - 1))
            {
                maybeAddSplitBacktrack(node,
                                       tailIndex,
                                       SPLIT_OTHER_LEVEL_LIMIT,
                                       data & -(1 << (SPLIT_LEVEL_SHIFT * 2)),
                                       SPLIT_LEVEL_SHIFT * 1);
                int tail = getSplitCellPointer(node, tailIndex, SPLIT_OTHER_LEVEL_LIMIT);
                return descendInSplitSublevel(tail,
                                              SPLIT_OTHER_LEVEL_LIMIT,
                                              data & -(1 << SPLIT_LEVEL_SHIFT * 1),
                                              SPLIT_LEVEL_SHIFT * 0);
            }
            int midIndex = splitNodeMidIndex(data);
            assert midIndex != direction.select(0, SPLIT_START_LEVEL_LIMIT - 1);
            maybeAddSplitBacktrack(node,
                                   midIndex,
                                   SPLIT_START_LEVEL_LIMIT,
                                   0,
                                   SPLIT_LEVEL_SHIFT * 2);
            int mid = getSplitCellPointer(node, midIndex, SPLIT_START_LEVEL_LIMIT);
            return descendInSplitSublevel(mid,
                                          SPLIT_OTHER_LEVEL_LIMIT,
                                          data & -(1 << SPLIT_LEVEL_SHIFT * 2),
                                          SPLIT_LEVEL_SHIFT * 1);
        }

        /**
         * Backtrack to a split sub-level and advance to given transition if it fits within the sublevel.
         * The level is identified by the lowest non-0 bits in data as above.
         */
        private int advanceToSplitTransition(int node, int data, int skipTransition)
        {
            assert data >= 0 && data <= 0xFF;
            if (direction.lt(skipTransition, data))
                return nextValidSplitTransition(node, data); // already went over the target in lower sublevel, just advance

            int childIndex = splitNodeChildIndex(data);
            if (childIndex != direction.select(0, SPLIT_OTHER_LEVEL_LIMIT - 1))
            {
                int sublevelMask = -(1 << (SPLIT_LEVEL_SHIFT * 1));
                int sublevelShift = SPLIT_LEVEL_SHIFT * 0;
                int sublevelLimit = SPLIT_OTHER_LEVEL_LIMIT;
                return descendInSplitSublevelWithTarget(node, sublevelLimit, data & sublevelMask, sublevelShift, skipTransition);
            }
            int tailIndex = splitNodeTailIndex(data);
            if (tailIndex != direction.select(0, SPLIT_OTHER_LEVEL_LIMIT - 1))
            {
                int sublevelMask = -(1 << (SPLIT_LEVEL_SHIFT * 2));
                int sublevelShift = SPLIT_LEVEL_SHIFT * 1;
                int sublevelLimit = SPLIT_OTHER_LEVEL_LIMIT;
                return descendInSplitSublevelWithTarget(node, sublevelLimit, data & sublevelMask, sublevelShift, skipTransition);
            }
            int sublevelMask = -(1 << 8);
            int sublevelShift = SPLIT_LEVEL_SHIFT * 2;
            int sublevelLimit = SPLIT_START_LEVEL_LIMIT;
            return descendInSplitSublevelWithTarget(node, sublevelLimit, data & sublevelMask, sublevelShift, skipTransition);
        }

        /**
         * Look for any further non-null transitions on this sub-level and, if found, add a backtracking entry.
         */
        private void maybeAddSplitBacktrack(int node, int startAfter, int limit, int collected, int shift)
        {
            int nextChildIndex;
            for (nextChildIndex = startAfter + direction.increase;
                 direction.inLoop(nextChildIndex, 0, limit - 1);
                 nextChildIndex += direction.increase)
            {
                if (!isNull(getSplitCellPointer(node, nextChildIndex, limit)))
                    break;
            }
            if (direction.inLoop(nextChildIndex, 0, limit - 1))
            {
                if (direction.isForward())
                    addBacktrack(node, collected | (nextChildIndex << shift), depth);
                else
                {
                    // The (((x + 1) << shift) - 1) adjustment will put all 1s in all lower bits
                    addBacktrack(node, collected | ((((nextChildIndex + 1) << shift)) - 1), depth);
                }
            }
        }


        private int nextValidSparseTransition(int node, int data)
        {
            // Peel off the next index.
            int index = data % SPARSE_CHILD_COUNT;
            data = data / SPARSE_CHILD_COUNT;

            UnsafeBuffer buffer = getBuffer(node);
            int inBufferNode = inBufferOffset(node);

            // If there are remaining transitions, add backtracking entry.
            if (data != exhaustedOrderWord())
                addBacktrack(node, data, depth);

            // Follow the transition.
            int child = buffer.getIntVolatile(inBufferNode + SPARSE_CHILDREN_OFFSET + index * 4);
            int transition = buffer.getByte(inBufferNode + SPARSE_BYTES_OFFSET + index) & 0xFF;
            return descendInto(child, transition);
        }

        /**
         * Prepare the sparse node order word for iteration. For forward iteration, this means just reading it.
         * For reverse, we also invert the data so that the peeling code above still works.
         */
        int prepareOrderWord(int node)
        {
            int fwdState = getUnsignedShortVolatile(node + SPARSE_ORDER_OFFSET);
            if (direction.isForward())
                return fwdState;
            else
            {
                // Produce an inverted state word.

                // One subtlety is that in forward order we know we can terminate the iteration when the state becomes
                // 0 because 0 cannot be the largest child (we enforce 10 order for the first two children and then can
                // only insert other digits in the word, thus 0 is always preceded by a 1 (not necessarily immediately)
                // in the order word) and thus we can't confuse a completed iteration with one that still has the child
                // at 0 to present.
                // In reverse order 0 can be the last child that needs to be iterated (e.g. for two children the order
                // word is always 10, which is 01 inverted; if we treat it exactly as the forward iteration, we will
                // only list child 1 because we will interpret the state 0 after peeling the first digit as a completed
                // iteration). To know when to stop we must thus use a different marker - since we know 1 is never the
                // last child to be iterated in reverse order (because it is preceded by a 0 in the reversed order
                // word), we can use another 1 as the termination marker. The generated number may not fit a 16-bit word
                // any more, but that does not matter as we don't need to store it.
                // For example, the code below translates 120 to 1021, and to iterate we peel the lower order digits
                // until the iteration state becomes just 1.

                int revState = 1;   // 1 can't be the smallest child
                while (fwdState != 0)
                {
                    revState = revState * SPARSE_CHILD_COUNT + fwdState % SPARSE_CHILD_COUNT;
                    fwdState /= SPARSE_CHILD_COUNT;
                }

                return revState;
            }
        }

        /**
         * Returns the state which marks the exhaustion of the order word.
         */
        int exhaustedOrderWord()
        {
            return direction.select(0, 1);
        }

        private int advanceToSparseTransition(int node, int data, int skipTransition)
        {
            UnsafeBuffer buffer = getBuffer(node);
            int inBufferNode = inBufferOffset(node);
            int index;
            int transition;
            do
            {
                // Peel off the next index.
                index = data % SPARSE_CHILD_COUNT;
                data = data / SPARSE_CHILD_COUNT;
                transition = buffer.getByte(inBufferNode + SPARSE_BYTES_OFFSET + index) & 0xFF;
            }
            while (direction.lt(transition, skipTransition) && data != exhaustedOrderWord());
            if (direction.lt(transition, skipTransition))
                return -1;

            // If there are remaining transitions, add backtracking entry.
            if (data != exhaustedOrderWord())
                addBacktrack(node, data, depth);

            // Follow the transition.
            int child = buffer.getIntVolatile(inBufferNode + SPARSE_CHILDREN_OFFSET + index * 4);
            return descendInto(child, transition);
        }

        private int getChainTransition(int node)
        {
            // No backtracking needed.
            UnsafeBuffer buffer = getBuffer(node);
            int inBufferNode = inBufferOffset(node);
            int transition = buffer.getByte(inBufferNode) & 0xFF;
            int next = node + 1;
            if (offset(next) <= CHAIN_MAX_OFFSET)
                return descendIntoChain(next, transition);
            else
                return descendInto(buffer.getIntVolatile(inBufferNode + 1), transition);
        }

        private int advanceToChainTransition(int node, int skipTransition)
        {
            // No backtracking needed.
            UnsafeBuffer buffer = getBuffer(node);
            int inBufferNode = inBufferOffset(node);
            int transition = buffer.getByte(inBufferNode) & 0xFF;
            if (direction.gt(skipTransition, transition))
                return -1;

            int next = node + 1;
            if (offset(next) <= CHAIN_MAX_OFFSET)
                return descendIntoChain(next, transition);
            else
                return descendInto(buffer.getIntVolatile(inBufferNode + 1), transition);
        }

        int descendInto(int child, int transition)
        {
            ++depth;
            incomingTransition = transition;
            content = getNodeContent(child);
            currentFullNode = child;
            currentNode = followContentTransition(child);
            return depth;
        }

        int descendIntoChain(int child, int transition)
        {
            ++depth;
            incomingTransition = transition;
            content = null;
            currentFullNode = child;
            currentNode = child;
            return depth;
        }
    }

    private boolean isChainNode(int node)
    {
        return !isNullOrLeaf(node) && offset(node) <= CHAIN_MAX_OFFSET;
    }

    public InMemoryCursor cursor(Direction direction)
    {
        return new InMemoryCursor(direction);
    }

    /*
     Direct read methods
     */

    /**
     * Get the content mapped by the specified key.
     * Fast implementation using integer node addresses.
     */
    @Override
    public T get(ByteComparable path)
    {
        int n = root;
        ByteSource source = path.asComparableBytes(byteComparableVersion);
        while (!isNull(n))
        {
            int c = source.next();
            if (c == ByteSource.END_OF_STREAM)
                return getNodeContent(n);

            n = advance(n, c, source);
        }

        return null;
    }

    public boolean isEmpty()
    {
        return isNull(root);
    }

    /**
     * Override of dump to provide more detailed printout that includes the type of each node in the trie.
     * We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
     */
    @Override
    public String dump(Function<T, String> contentToString)
    {
        InMemoryCursor source = cursor(Direction.FORWARD);
        class TypedNodesCursor implements Cursor<String>
        {
            @Override
            public int advance()
            {
                return source.advance();
            }


            @Override
            public int advanceMultiple(TransitionsReceiver receiver)
            {
                return source.advanceMultiple(receiver);
            }

            @Override
            public int skipTo(int skipDepth, int skipTransition)
            {
                return source.skipTo(skipDepth, skipTransition);
            }

            @Override
            public int depth()
            {
                return source.depth();
            }

            @Override
            public int incomingTransition()
            {
                return source.incomingTransition();
            }

            @Override
            public Direction direction()
            {
                return source.direction();
            }

            @Override
            public ByteComparable.Version byteComparableVersion()
            {
                return source.byteComparableVersion();
            }

            @Override
            public Trie<String> tailTrie()
            {
                throw new AssertionError();
            }

            @Override
            public String content()
            {
                String type = null;
                int node = source.currentNode;
                if (!isNullOrLeaf(node))
                {
                    switch (offset(node))
                    {
                        case SPARSE_OFFSET:
                            type = "[SPARSE]";
                            break;
                        case SPLIT_OFFSET:
                            type = "[SPLIT]";
                            break;
                        case PREFIX_OFFSET:
                            throw new AssertionError("Unexpected prefix as cursor currentNode.");
                        default:
                            type = "[CHAIN]";
                            break;
                    }
                }
                T content = source.content();
                if (content != null)
                {
                    if (type != null)
                        return contentToString.apply(content) + " -> " + type;
                    else
                        return contentToString.apply(content);
                }
                else
                    return type;
            }
        }
        return process(new TrieDumper<>(Function.identity()), new TypedNodesCursor());
    }

    /**
     * For use in debugging, dump info about the given node.
     */
    @SuppressWarnings("unused")
    String dumpNode(int node)
    {
        if (isNull(node))
            return "NONE";
        else if (isLeaf(node))
            return "~" + (~node);
        else
        {
            StringBuilder builder = new StringBuilder();
            builder.append(node + " ");
            switch (offset(node))
            {
                case SPARSE_OFFSET:
                {
                    builder.append("Sparse: ");
                    for (int i = 0; i < SPARSE_CHILD_COUNT; ++i)
                    {
                        int child = getIntVolatile(node + SPARSE_CHILDREN_OFFSET + i * 4);
                        if (child != NONE)
                            builder.append(String.format("%02x", getUnsignedByte(node + SPARSE_BYTES_OFFSET + i)))
                                   .append(" -> ")
                                   .append(child)
                                   .append('\n');
                    }
                    break;
                }
                case SPLIT_OFFSET:
                {
                    builder.append("Split: ");
                    for (int i = 0; i < SPLIT_START_LEVEL_LIMIT; ++i)
                    {
                        int child = getIntVolatile(node - (SPLIT_START_LEVEL_LIMIT - 1 - i) * 4);
                        if (child != NONE)
                            builder.append(Integer.toBinaryString(i))
                                   .append(" -> ")
                                   .append(child)
                                   .append('\n');
                    }
                    break;
                }
                case PREFIX_OFFSET:
                {
                    builder.append("Prefix: ");
                    int flags = getUnsignedByte(node + PREFIX_FLAGS_OFFSET);
                    final int content = getIntVolatile(node + PREFIX_CONTENT_OFFSET);
                    builder.append(content < 0 ? "~" + (~content) : "" + content);
                    int child = followContentTransition(node);
                    builder.append(" -> ")
                           .append(child);
                    break;
                }
                default:
                {
                    builder.append("Chain: ");
                    for (int i = 0; i < chainCellLength(node); ++i)
                        builder.append(String.format("%02x", getUnsignedByte(node + i)));
                    builder.append(" -> ")
                           .append(getIntVolatile(node + chainCellLength(node)));
                    break;
                }
            }
            return builder.toString();
        }
    }
}
