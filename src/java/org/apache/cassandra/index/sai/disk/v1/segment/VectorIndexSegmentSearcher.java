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
package org.apache.cassandra.index.sai.disk.v1.segment;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.NeighborQueue;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseFixedBitSet;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.VectorQueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.vector.DiskAnn;
import org.apache.cassandra.index.sai.disk.v1.vector.NeighborQueueRowIdIterator;
import org.apache.cassandra.index.sai.disk.v1.vector.OptimizeFor;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.disk.v1.vector.RowIdToPrimaryKeyWithScoreIterator;
import org.apache.cassandra.index.sai.disk.v1.vector.RowIdWithScore;
import org.apache.cassandra.index.sai.disk.v1.vector.SegmentRowIdOrdinalPairs;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.VectorMemoryIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.IntArrayPostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.AtomicRatio;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Executes ANN search against a vector graph for an individual index segment.
 */
public class VectorIndexSegmentSearcher extends IndexSegmentSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final DiskAnn graph;
    private final int globalBruteForceRows;
    private final AtomicRatio actualExpectedRatio = new AtomicRatio();
    private final ThreadLocal<SparseFixedBitSet> cachedBitSets;
    private final OptimizeFor optimizeFor;
    private final ColumnMetadata column;

    VectorIndexSegmentSearcher(SSTableContext sstableContext,
                               PerColumnIndexFiles perIndexFiles,
                               SegmentMetadata segmentMetadata,
                               StorageAttachedIndex index) throws IOException
    {
        super(sstableContext.primaryKeyMapFactory, perIndexFiles, segmentMetadata, index);
        SSTableId sstableId = sstableContext.sstable.descriptor.id;
        graph = new DiskAnn(segmentMetadata.componentMetadatas, perIndexFiles, index.indexWriterConfig(), sstableId);
        cachedBitSets = ThreadLocal.withInitial(() -> new SparseFixedBitSet(graph.size()));
        globalBruteForceRows = Integer.MAX_VALUE;
        optimizeFor = index.indexWriterConfig().getOptimizeFor();
        column = index.termType().columnMetadata();
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CloseableIterator<PrimaryKeyWithScore> orderBy(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context) throws IOException
    {
        int limit = context.vectorContext().limit();

        if (logger.isTraceEnabled())
            logger.trace(index.identifier().logMessage("Searching on expression '{}'..."), exp);

        if (exp.getIndexOperator() != Expression.IndexOperator.ANN)
            throw new IllegalArgumentException(index.identifier().logMessage("Unsupported expression during ANN index query: " + exp));

        int topK = optimizeFor.topKFor(limit);

        float[] queryVector = index.termType().decomposeVector(exp.lower().value.raw.duplicate());
        CloseableIterator<RowIdWithScore> result = searchInternal(keyRange, queryVector, limit, topK);
        return toScoreSortedIterator(result);
    }

    /**
     * Return bit set we need to search the graph; otherwise return posting list to bypass the graph
     */
    private CloseableIterator<RowIdWithScore> searchInternal(AbstractBounds<PartitionPosition> keyRange, float[] queryVector, int topK, int limit) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // not restricted
            if (RangeUtil.coversFullRing(keyRange))
            {
                int expectedNodesVisited = expectedNodesVisited(limit, graph.size(), graph.size());
                IntConsumer nodesVisitedConsumer = nodesVisited -> updateExpectedNodes(nodesVisited, expectedNodesVisited);
                return graph.search(queryVector, topK, limit, new Bits.MatchAllBits(Integer.MAX_VALUE), nodesVisitedConsumer);
            }

            // it will return the next row id if given key is not found.
            long minSSTableRowId = primaryKeyMap.ceiling(keyRange.left.getToken());
            // If we didn't find the first key, we won't find the last primary key either
            if (minSSTableRowId < 0)
                return CloseableIterator.empty();
            long maxSSTableRowId = getMaxSSTableRowId(primaryKeyMap, keyRange.right);

            if (minSSTableRowId > maxSSTableRowId)
                return CloseableIterator.empty();

            // if it covers entire segment, skip bit set
            if (minSSTableRowId <= metadata.minSSTableRowId && maxSSTableRowId >= metadata.maxSSTableRowId)
            {
                int expectedNodesVisited = expectedNodesVisited(limit, graph.size(), graph.size());
                IntConsumer nodesVisitedConsumer = nodesVisited -> updateExpectedNodes(nodesVisited, expectedNodesVisited);
                return graph.search(queryVector, topK, limit, new Bits.MatchAllBits(Integer.MAX_VALUE), nodesVisitedConsumer);
            }

            minSSTableRowId = Math.max(minSSTableRowId, metadata.minSSTableRowId);
            maxSSTableRowId = min(maxSSTableRowId, metadata.maxSSTableRowId);

            // If num of matches are not bigger than limit, skip ANN.
            // (nRows should not include shadowed rows, but context doesn't break those out by segment,
            // so we will live with the inaccuracy.)
            int nRows = Math.toIntExact(maxSSTableRowId - minSSTableRowId + 1);
            int maxBruteForceRows = min(globalBruteForceRows, maxBruteForceRows(limit, nRows, graph.size()));
            logger.trace("Search range covers {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                         nRows, maxBruteForceRows, graph.size(), limit);
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                          nRows, maxBruteForceRows, graph.size(), limit);
            if (nRows <= maxBruteForceRows)
            {
                SegmentRowIdOrdinalPairs segmentOrdinalPairs = new SegmentRowIdOrdinalPairs(Math.toIntExact(nRows));
                try (var ordinalsView = graph.getOrdinalsView())
                {
                    for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
                    {
                        int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                        int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                        if (ordinal >= 0)
                            segmentOrdinalPairs.add(segmentRowId, ordinal);
                    }
                }
                return orderByBruteForce(queryVector, segmentOrdinalPairs);
            }

            // create a bitset of ordinals corresponding to the rows in the given key range
            SparseFixedBitSet bits = bitSetForSearch();
            boolean hasMatches = false;
            try (var ordinalsView = graph.getOrdinalsView())
            {
                for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
                {
                    int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                    int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                    if (ordinal >= 0)
                    {
                        bits.set(ordinal);
                        hasMatches = true;
                    }
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            if (!hasMatches)
                return CloseableIterator.empty();

            int expectedNodesVisited = expectedNodesVisited(limit, bits.cardinality(), graph.size());
            IntConsumer nodesVisitedConsumer = nodesVisited -> updateExpectedNodes(nodesVisited, expectedNodesVisited);
            return graph.search(queryVector, topK, limit, bits, nodesVisitedConsumer);
        }
    }

    private long getMaxSSTableRowId(PrimaryKeyMap primaryKeyMap, PartitionPosition right)
    {
        // if the right token is the minimum token, there is no upper bound on the keyRange and
        // we can save a lookup by using the maxSSTableRowId
        if (right.isMinimum())
            return metadata.maxSSTableRowId;

        long max = primaryKeyMap.floor(right.getToken());
        if (max < 0)
            return metadata.maxSSTableRowId;
        return max;
    }

    private SparseFixedBitSet bitSetForSearch()
    {
        SparseFixedBitSet bits = cachedBitSets.get();
        bits.clear();
        return bits;
    }

    /**
     * Produces a correct ranking of the rows in the given segment. Because this graph does not have compressed
     * vectors, read all vectors and put them into a priority queue to rank them lazily. It is assumed that the whole
     * PQ will often not be needed.
     */
    private CloseableIterator<RowIdWithScore> orderByBruteForce(float[] queryVector, SegmentRowIdOrdinalPairs segmentOrdinalPairs)
    {
        if (segmentOrdinalPairs.size() == 0)
            return CloseableIterator.empty();

        // TODO implement the two pass brute force search where we first score by compressed vectors and then
        NeighborSimilarity.ExactScoreFunction esf = graph.getExactScoreFunction(queryVector);
        NeighborQueue scoredRowIds = segmentOrdinalPairs.mapToSegmentRowIdScoreHeap(esf);
        // TODO metrics? columnQueryMetrics.onBruteForceNodesReranked(segmentOrdinalPairs.size());
        return new NeighborQueueRowIdIterator(scoredRowIds);
    }

    @Override
    public CloseableIterator<PrimaryKeyWithScore> orderResultsBy(QueryContext context, List<PrimaryKey> primaryKeys, Expression expression) throws IOException
    {
        int limit = context.vectorContext().limit();
        // VSTODO would it be better to do a binary search to find the boundaries?
        List<PrimaryKey> keysInRange = primaryKeys.stream()
                                                  .dropWhile(k -> k.compareTo(metadata.minKey) < 0)
                                                  .takeWhile(k -> k.compareTo(metadata.maxKey) <= 0)
                                                  .collect(Collectors.toList());
        if (keysInRange.isEmpty())
            return CloseableIterator.empty();

        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // the iterator represents keys from the whole table -- we'll only pull of those that
            // are from our own token range, so we can use row ids to order the results by vector similarity.
            SegmentRowIdOrdinalPairs segmentOrdinalPairs = new SegmentRowIdOrdinalPairs(keysInRange.size());
            try (var ordinalsView = graph.getOrdinalsView())
            {
                for (PrimaryKey primaryKey : keysInRange)
                {
                    long sstableRowId = primaryKeyMap.rowIdFromPrimaryKey(primaryKey);
                    // skip rows that are not in our segment (or more preciesely, have no vectors that were indexed)
                    // or are not in this segment (exactRowIdForPrimaryKey returns a negative value for not found)
                    if (sstableRowId < metadata.minSSTableRowId)
                        continue;

                    // if sstable row id has exceeded current ANN segment, stop
                    if (sstableRowId > metadata.maxSSTableRowId)
                        break;

                    int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                    // VSTODO now that we know the size of keys evaluated, is it worth doing the brute
                    // force check eagerly to potentially skip the PK to sstable row id to ordinal lookup?
                    int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                    if (ordinal >= 0)
                        segmentOrdinalPairs.add(segmentRowId, ordinal);
                }
            }

            int topK = optimizeFor.topKFor(limit);
            float[] queryVector = index.termType().decomposeVector(expression.lower().value.raw.duplicate());

            if (shouldUseBruteForce(topK, limit, segmentOrdinalPairs.size()))
            {
                return toScoreSortedIterator(orderByBruteForce(queryVector, segmentOrdinalPairs));
            }

            SparseFixedBitSet bits = bitSetForSearch();
            segmentOrdinalPairs.forEachOrdinal(bits::set);
            // else ask the index to perform a search limited to the bits we created
            int expectedNodesVisited = expectedNodesVisited(limit, segmentOrdinalPairs.size(), graph.size());
            IntConsumer nodesVisitedConsumer = nodesVisited -> updateExpectedNodes(nodesVisited, expectedNodesVisited);
            CloseableIterator<RowIdWithScore> result = graph.search(queryVector, topK, limit, bits, nodesVisitedConsumer);
            return toScoreSortedIterator(result);
        }
    }

    private boolean shouldUseBruteForce(int topK, int limit, int numRows)
    {
        // if we have a small number of results then let TopK processor do exact NN computation
        int maxBruteForceRows = min(globalBruteForceRows, maxBruteForceRows(topK, numRows, graph.size()));
        logger.trace("SAI materialized {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                     numRows, maxBruteForceRows, graph.size(), limit);
        Tracing.trace("SAI materialized {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                      numRows, maxBruteForceRows, graph.size(), limit);
        return numRows <= maxBruteForceRows;
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodes = expectedNodesVisited(limit, nPermittedOrdinals, graphSize);
        // ANN index will do a bunch of extra work besides the full comparisons (performing PQ similarity for each edge);
        // brute force from sstable will also do a bunch of extra work (going through trie index to look up row).
        // VSTODO I'm not sure which one is more expensive (and it depends on things like sstable chunk cache hit ratio)
        // so I'm leaving it as a 1:1 ratio for now.
        return max(limit, expectedNodes);
    }

    private int expectedNodesVisited(int limit, int nPermittedOrdinals, int graphSize)
    {
        double observedRatio = actualExpectedRatio.getUpdateCount() >= 10 ? actualExpectedRatio.get() : 1.0;
        return (int) (observedRatio * VectorMemoryIndex.expectedNodesVisited(limit, nPermittedOrdinals, graphSize));
    }

    private void updateExpectedNodes(int actualNodesVisited, int expectedNodesVisited)
    {
        assert expectedNodesVisited >= 0 : expectedNodesVisited;
        assert actualNodesVisited >= 0 : actualNodesVisited;
        if (actualNodesVisited >= 1000 && actualNodesVisited > 2 * expectedNodesVisited || expectedNodesVisited > 2 * actualNodesVisited)
            logger.warn("Predicted visiting {} nodes, but actually visited {}", expectedNodesVisited, actualNodesVisited);
        actualExpectedRatio.update(actualNodesVisited, expectedNodesVisited);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("index", index).toString();
    }

    @Override
    public void close() throws IOException
    {
        graph.close();
    }

    private CloseableIterator<PrimaryKeyWithScore> toScoreSortedIterator(CloseableIterator<RowIdWithScore> rowIdIterator) throws IOException
    {
        if (!rowIdIterator.hasNext())
        {
            FileUtils.closeQuietly(rowIdIterator);
            return CloseableIterator.empty();
        }

        return new RowIdToPrimaryKeyWithScoreIterator(column, primaryKeyMapFactory, rowIdIterator, metadata.rowIdOffset);
    }

    private static class BitsOrPostingList
    {
        private final Bits bits;
        private final int expectedNodesVisited;
        private final PostingList postingList;

        public BitsOrPostingList(@Nullable Bits bits, int expectedNodesVisited)
        {
            this.bits = bits;
            this.expectedNodesVisited = expectedNodesVisited;
            this.postingList = null;
        }

        public BitsOrPostingList(@Nullable Bits bits)
        {
            this.bits = bits;
            this.postingList = null;
            this.expectedNodesVisited = -1;
        }

        public BitsOrPostingList(PostingList postingList)
        {
            this.bits = null;
            this.postingList = Preconditions.checkNotNull(postingList);
            this.expectedNodesVisited = -1;
        }

        @Nullable
        public Bits getBits()
        {
            Preconditions.checkState(!skipANN());
            return bits;
        }

        public PostingList postingList()
        {
            Preconditions.checkState(skipANN());
            return postingList;
        }

        public boolean skipANN()
        {
            return postingList != null;
        }
    }
}
