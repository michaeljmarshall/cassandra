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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Triple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.ParallelCommandProcessor;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.InMemoryPartitionIterator;
import org.apache.cassandra.index.sai.utils.InMemoryUnfilteredPartitionIterator;
import org.apache.cassandra.index.sai.utils.PartitionInfo;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static java.lang.Math.min;

/**
 * Processor that scans all rows from given partitions and selects rows with top-k scores based on indexes.
 *
 * TODO update me based on what I really do
 * This processor performs the following steps:
 * - collect rows with score into PriorityQueue that sorts rows based on score. If there are multiple vector indexes,
 *   the final score is the sum of all vector index scores.
 * - remove rows with the lowest scores from PQ if PQ size exceeds limit
 * - return rows from PQ in primary key order to client
 *
 * Note that recall will be lower with paging, because:
 * - page size is used as limit
 * - for the first query, coordinator returns global top page-size rows within entire ring
 * - for the subsequent queries, coordinators returns global top page-size rows withom range from last-returned-row to max token
 */
public class TopKProcessor
{
    protected static final Logger logger = LoggerFactory.getLogger(TopKProcessor.class);
    private static final LocalAwareExecutorService PARALLEL_EXECUTOR = getExecutor();
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final ReadCommand command;
    private final IndexContext indexContext;
    private final RowFilter.Expression expression;
    private final VectorFloat<?> queryVector;

    private final int limit;

    public TopKProcessor(ReadCommand command)
    {
        this.command = command;

        Pair<IndexContext, RowFilter.Expression> annIndexAndExpression = findTopKIndexContext();
        Preconditions.checkNotNull(annIndexAndExpression);

        this.indexContext = annIndexAndExpression.left;
        this.expression = annIndexAndExpression.right;
        if (expression.operator() == Operator.ANN)
            this.queryVector = vts.createFloatVector(TypeUtil.decomposeVector(indexContext, expression.getIndexValue().duplicate()));
        else
            this.queryVector = null;
        this.limit = command.limits().count();
    }

    /**
     * Executor to use for parallel index reads.
     * Defined by -Dcassandra.index_read.parallele=true/false, true by default.
     *
     * INDEX_READ uses 2 * cpus threads by default but can be overridden with -Dcassandra.index_read.parallel_thread_num=<value>
     *
     * @return stage to use, default INDEX_READ
     */
    private static LocalAwareExecutorService getExecutor()
    {
        boolean isParallel = CassandraRelevantProperties.USE_PARALLEL_INDEX_READ.getBoolean();

        if (isParallel)
        {
            int numThreads = CassandraRelevantProperties.PARALLEL_INDEX_READ_NUM_THREADS.isPresent()
                                ? CassandraRelevantProperties.PARALLEL_INDEX_READ_NUM_THREADS.getInt()
                                : FBUtilities.getAvailableProcessors() * 2;
            return SharedExecutorPool.SHARED.newExecutor(numThreads, maximumPoolSize -> {}, "request", "IndexParallelRead");
        }
        else
            return ImmediateExecutor.INSTANCE;
    }

    /**
     * Filter given partitions and keep the rows with highest scores. In case of {@link UnfilteredPartitionIterator},
     * all tombstones will be kept. Caller must close the supplied iterator.
     */
    public <U extends Unfiltered, R extends BaseRowIterator<U>, P extends BasePartitionIterator<R>> BasePartitionIterator<?> filter(P partitions)
    {
        // filterInternal consumes the partitions iterator and creates a new one. Use a try-with-resources block
        // to ensure the original iterator is closed. We do not expect exceptions from filterInternal, but if they
        // happen, we want to make sure the original iterator is closed to prevent leaking resources, which could
        // compound the effect of an exception.
        try (partitions)
        {
            return filterInternal(partitions);
        }
    }

    private <U extends Unfiltered, R extends BaseRowIterator<U>, P extends BasePartitionIterator<R>> BasePartitionIterator<?> filterInternal(P partitions)
    {
        // priority queue ordered by score in descending order
        PriorityQueue<Triple<PartitionInfo, Row, Float>> topK = new PriorityQueue<>(limit, Comparator.comparing((Triple<PartitionInfo, Row, Float> t) -> t.getRight()).reversed());
        // to store top-k results in primary key order
        TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition = new TreeMap<>(Comparator.comparing(p -> p.key));
        Comparator<Triple<PartitionInfo, Row, ByteBuffer>> comparator = Comparator.comparing(Triple::getRight, (a, b) ->
                                                                                                               TypeUtil.compare(a, b, indexContext.getValidator()));
        if (expression.operator() == Operator.SORT_DESC)
            comparator = comparator.reversed();
        var sorter = new PriorityQueue<>(limit, comparator);

        if (PARALLEL_EXECUTOR != ImmediateExecutor.INSTANCE && partitions instanceof ParallelCommandProcessor) {
            ParallelCommandProcessor pIter = (ParallelCommandProcessor) partitions;
            var commands = pIter.getUninitializedCommands();
            List<CompletableFuture<PartitionResults>> results = new ArrayList<>(commands.size());

            int count = commands.size();
            for (var command: commands) {
                CompletableFuture<PartitionResults> future = new CompletableFuture<>();
                results.add(future);

                // run last command immediately, others in parallel (if possible)
                count--;
                var executor = count == 0 ? ImmediateExecutor.INSTANCE : PARALLEL_EXECUTOR;

                executor.maybeExecuteImmediately(() -> {
                    try (var partitionRowIterator = pIter.commandToIterator(command.left(), command.right()))
                    {
                        future.complete(partitionRowIterator == null ? null : processPartition(partitionRowIterator));
                    }
                    catch (Throwable t)
                    {
                        future.completeExceptionally(t);
                    }
                });
            }

            for (CompletableFuture<PartitionResults> triplesFuture: results) {
                PartitionResults pr;
                try
                {
                    pr = triplesFuture.join();
                }
                catch (CompletionException t)
                {
                    if (t.getCause() instanceof AbortedOperationException)
                        throw (AbortedOperationException) t.getCause();
                    throw t;
                }
                if (pr == null)
                    continue;
                topK.addAll(pr.rows);
                for (var uf: pr.tombstones)
                    addUnfiltered(unfilteredByPartition, pr.partitionInfo, uf);
            }
        } else if (partitions instanceof StorageAttachedIndexSearcher.ScoreOrderedResultRetriever) {
            var retriever = (StorageAttachedIndexSearcher.ScoreOrderedResultRetriever) partitions;
            // FilteredPartitions does not implement ParallelizablePartitionIterator.
            // Realistically, this won't benefit from parallelizm as these are coming from in-memory/memtable data.
            int rowsMatched = 0;
            // Check rowsMatched first to prevent fetching one more partition than needed.
            while (rowsMatched < limit && retriever.hasNext())
            {
                // have to close to move to the next partition, otherwise hasNext() fails
                try (var partitionRowIterator = retriever.next())
                {
                    var iter = (StorageAttachedIndexSearcher.ScoreOrderedResultRetriever.PrimaryKeyIterator) partitionRowIterator;
                    PartitionResults pr = processPartition(iter, iter.primaryKeyWithSortKey, retriever);
                    rowsMatched += pr.rows.size();
                    for (var row : pr.rows)
                        addUnfiltered(unfilteredByPartition, row.getLeft(), row.getMiddle());
                    for (var uf : pr.tombstones)
                        addUnfiltered(unfilteredByPartition, pr.partitionInfo, uf);
                }
            }
        } else {
            // FilteredPartitions does not implement ParallelizablePartitionIterator.
            // Realistically, this won't benefit from parallelizm as these are coming from in-memory/memtable data.
            while (partitions.hasNext())
            {
                // have to close to move to the next partition, otherwise hasNext() fails
                try (var partitionRowIterator = partitions.next())
                {
                    if (queryVector != null)
                    {
                        PartitionResults pr = processPartition(partitionRowIterator);
                        topK.addAll(pr.rows);
                        for (var uf: pr.tombstones)
                            addUnfiltered(unfilteredByPartition, pr.partitionInfo, uf);
                    }
                    else
                    {
                        while (partitionRowIterator.hasNext())
                        {
                            Row row = (Row) partitionRowIterator.next();
                            sorter.add(Triple.of(PartitionInfo.create(partitionRowIterator), row, row.getCell(expression.column()).buffer()));
                        }
                    }

                }
            }
        }

        // reorder rows in partition/clustering order
        int min = Math.max(topK.size(), sorter.size());
        final int numResults = min(min, limit);
        for (int i = 0; i < numResults; i++) {
            var triple = queryVector != null ? topK.poll() : sorter.poll();
            addUnfiltered(unfilteredByPartition, triple.getLeft(), triple.getMiddle());
        }

        if (partitions instanceof PartitionIterator)
            return new InMemoryPartitionIterator(command, unfilteredByPartition);
        return new InMemoryUnfilteredPartitionIterator(command, unfilteredByPartition);
    }

    private class PartitionResults {
        final PartitionInfo partitionInfo;
        final SortedSet<Unfiltered> tombstones = new TreeSet<>(command.metadata().comparator);
        final List<Triple<PartitionInfo, Row, Float>> rows = new ArrayList<>();

        PartitionResults(PartitionInfo partitionInfo) {
            this.partitionInfo = partitionInfo;
        }

        void addTombstone(Unfiltered uf)
        {
            tombstones.add(uf);
        }

        void addRow(Triple<PartitionInfo, Row, Float> triple) {
            rows.add(triple);
        }
    }

    /**
     * Processes a single partition, calculating scores for rows and extracting tombstones.
     */
    private PartitionResults processPartition(BaseRowIterator<?> partitionRowIterator) {
        // Compute key and static row score once per partition
        DecoratedKey key = partitionRowIterator.partitionKey();
        Row staticRow = partitionRowIterator.staticRow();
        PartitionInfo partitionInfo = PartitionInfo.create(partitionRowIterator);
        float keyAndStaticScore = getScoreForRow(key, staticRow);
        var pr = new PartitionResults(partitionInfo);

        while (partitionRowIterator.hasNext())
        {
            Unfiltered unfiltered = partitionRowIterator.next();
            // Always include tombstones for coordinator. It relies on ReadCommand#withMetricsRecording to throw
            // TombstoneOverwhelmingException to prevent OOM.
            if (unfiltered.isRangeTombstoneMarker())
            {
                pr.addTombstone(unfiltered);
                continue;
            }

            Row row = (Row) unfiltered;
            float rowScore = getScoreForRow(null, row);
            pr.addRow(Triple.of(partitionInfo, row, keyAndStaticScore + rowScore));
        }

        return pr;
    }

    /**
     * Processes a single partition, calculating scores for rows and extracting tombstones.
     */
    private PartitionResults processPartition(BaseRowIterator<?> partitionRowIterator, PrimaryKeyWithSortKey key,
                                              StorageAttachedIndexSearcher.ScoreOrderedResultRetriever retriever) {
        PartitionInfo partitionInfo = PartitionInfo.create(partitionRowIterator);
        var pr = new PartitionResults(partitionInfo);

        if (!partitionRowIterator.hasNext())
            return pr;

        Unfiltered unfiltered = partitionRowIterator.next();
        assert !partitionRowIterator.hasNext() : "Only one row should be returned";
        // Always include tombstones for coordinator. It relies on ReadCommand#withMetricsRecording to throw
        // TombstoneOverwhelmingException to prevent OOM.
        if (unfiltered.isRangeTombstoneMarker())
        {
            pr.addTombstone(unfiltered);
            return pr;
        }

        Row row = (Row) unfiltered;
        if (retriever.shouldInclude(key, row))
            // TODO figure out better way to organize this data, score doesn't matter to us here.
            pr.addRow(Triple.of(partitionInfo, row, 0f));

        return pr;
    }

    private void addUnfiltered(SortedMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition, PartitionInfo partitionInfo, Unfiltered unfiltered)
    {
        var map = unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator));
        map.add(unfiltered);
    }

    /**
     * Sum the scores from different vector indexes for the row
     */
    private float getScoreForRow(DecoratedKey key, Row row)
    {
        ColumnMetadata column = indexContext.getDefinition();

        if (column.isPrimaryKeyColumn() && key == null)
            return 0;

        if (column.isStatic() && !row.isStatic())
            return 0;

        if ((column.isClusteringColumn() || column.isRegular()) && row.isStatic())
            return 0;

        ByteBuffer value = indexContext.getValueOf(key, row, FBUtilities.nowInSeconds());
        if (value != null)
        {
            var vector = vts.createFloatVector(TypeUtil.decomposeVector(indexContext, value));
            return indexContext.getIndexWriterConfig().getSimilarityFunction().compare(vector, queryVector);
        }
        return 0;
    }


    private Pair<IndexContext, RowFilter.Expression> findTopKIndexContext()
    {
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(command.metadata());

        for (RowFilter.Expression expression : command.rowFilter().getExpressions())
        {
            StorageAttachedIndex sai = findVectorIndexFor(cfs.indexManager, expression);
            if (sai != null)
            {
                return Pair.create(sai.getIndexContext(), expression);
            }
        }

        return null;
    }

    @Nullable
    private StorageAttachedIndex findVectorIndexFor(SecondaryIndexManager sim, RowFilter.Expression e)
    {
        if (e.operator() != Operator.ANN && e.operator() != Operator.SORT_ASC && e.operator() != Operator.SORT_DESC)
            return null;

        Optional<Index> index = sim.getBestIndexFor(e);
        return (StorageAttachedIndex) index.filter(i -> i instanceof StorageAttachedIndex).orElse(null);
    }
}
