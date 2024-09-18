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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MultiRangeReadCommand;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RowWithSourceTable;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.TermIterator;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

import static java.lang.Math.max;

public class QueryController implements Plan.Executor, Plan.CostEstimator
{
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    /**
     * Controls whether we optimize query plans.
     * 0 disables the optimizer. As a side effect, hybrid ANN queries will default to FilterSortOrder.SCAN_THEN_FILTER.
     * 1 enables the optimizer and tells the optimizer to respect the intersection clause limit.
     * Higher values enable the optimizer and disable the hard intersection clause limit.
     * Note: the config is not final to simplify testing.
     */
    @VisibleForTesting
    public static int QUERY_OPT_LEVEL = Integer.getInteger("cassandra.sai.query.optimization.level", 1);

    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final Orderer orderer;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;
    private final IndexFeatureSet indexFeatureSet;
    private final List<DataRange> ranges;
    private final AbstractBounds<PartitionPosition> mergeRange;

    private final PrimaryKey.Factory keyFactory;
    private final PrimaryKey firstPrimaryKey;

    final Plan.Factory planFactory;

    /**
     * Holds the primary key iterators for indexed expressions in the query (i.e. leaves of the expression tree).
     * We will construct the final iterator from those.
     * We need a MultiMap because the same Expression can occur more than once in a query.
     * <p>
     * Longer explanation why this is needed:
     * In order to construct a Plan for a query, we need predicate selectivity estimates. But at the moment
     * of writing this code, the only way to estimate an index predicate selectivity is to look at the posting
     * list(s) in the index, by obtaining a {@link RangeIterator} and callling {@link RangeIterator#getMaxKeys()} on it.
     * Hence, we need to create the iterators before creating the Plan.
     * But later when we assemble the final key iterator according to the optimized Plan, we need those iterators
     * again. In order to avoid recreating them, which would be costly, we just keep them here in this map.
     */
    private final Multimap<Expression, RangeIterator> keyIterators = ArrayListMultimap.create();

    private final Map<IndexContext, QueryView> queryViews = new HashMap<>();

    static
    {
        logger.info(String.format("Query plan optimization is %s (level = %d)",
                                  QUERY_OPT_LEVEL > 0 ? "enabled" : "disabled",
                                  QUERY_OPT_LEVEL));
    }

    @VisibleForTesting
    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           IndexFeatureSet indexFeatureSet,
                           QueryContext queryContext,
                           TableQueryMetrics tableQueryMetrics)
    {
        this(cfs, command, null, indexFeatureSet, queryContext, tableQueryMetrics);
    }

    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           Orderer orderer,
                           IndexFeatureSet indexFeatureSet,
                           QueryContext queryContext,
                           TableQueryMetrics tableQueryMetrics)
    {
        this.cfs = cfs;
        this.command = command;
        this.orderer = orderer;
        this.queryContext = queryContext;
        this.tableQueryMetrics = tableQueryMetrics;
        this.indexFeatureSet = indexFeatureSet;
        this.ranges = dataRanges(command);
        DataRange first = ranges.get(0);
        DataRange last = ranges.get(ranges.size() - 1);
        this.mergeRange = ranges.size() == 1 ? first.keyRange() : first.keyRange().withNewRight(last.keyRange().right);

        this.keyFactory = PrimaryKey.factory(cfs.metadata().comparator, indexFeatureSet);
        this.firstPrimaryKey = keyFactory.createTokenOnly(mergeRange.left.getToken());
        var tableMetrics = new Plan.TableMetrics(estimateTotalAvailableRows(ranges),
                                                 avgCellsPerRow(),
                                                 avgRowSizeInBytes(),
                                                 cfs.getLiveSSTables().size());
        this.planFactory = new Plan.Factory(tableMetrics, this);
    }

    public PrimaryKey.Factory primaryKeyFactory()
    {
        return keyFactory;
    }

    public PrimaryKey firstPrimaryKey()
    {
        return firstPrimaryKey;
    }


    public TableMetadata metadata()
    {
        return command.metadata();
    }

    RowFilter.FilterElement filterOperation()
    {
        // NOTE: we cannot remove the order by filter expression here yet because it is used in the FilterTree class
        // to filter out shadowed rows.
        return this.command.rowFilter().root();
    }

    /**
     * @return token ranges used in the read command
     */
    List<DataRange> dataRanges()
    {
        return ranges;
    }

    /**
     * Note: merged range may contain subrange that no longer belongs to the local node after range movement.
     * It should only be used as an optimization to reduce search space. Use {@link #dataRanges()} instead to filter data.
     *
     * @return merged token range
     */
    AbstractBounds<PartitionPosition> mergeRange()
    {
        return mergeRange;
    }

    /**
     * @return indexed {@code ColumnContext} if index is found; otherwise return non-indexed {@code ColumnContext}.
     */
    public IndexContext getContext(RowFilter.Expression expression)
    {
        StorageAttachedIndex index = getBestIndexFor(expression);

        if (index != null)
            return index.getIndexContext();

        return new IndexContext(cfs.metadata().keyspace,
                                cfs.metadata().name,
                                cfs.metadata().id,
                                cfs.metadata().partitionKeyType,
                                cfs.metadata().comparator,
                                expression.column(),
                                determineIndexTargetType(expression),
                                null,
                                cfs);
    }

    /**
     * Determines the {@link IndexTarget.Type} for the expression. In this case we are only interested in map types and
     * the operator being used in the expression.
     */
    public static IndexTarget.Type determineIndexTargetType(RowFilter.Expression expression)
    {
        AbstractType<?> type  = expression.column().type;
        IndexTarget.Type indexTargetType = IndexTarget.Type.SIMPLE;
        if (type.isCollection() && type.isMultiCell())
        {
            CollectionType<?> collection = ((CollectionType<?>) type);
            if (collection.kind == CollectionType.Kind.MAP)
            {
                Operator operator = expression.operator();
                switch (operator)
                {
                    case EQ:
                    case NEQ:
                    case LT:
                    case LTE:
                    case GT:
                    case GTE:
                        indexTargetType = IndexTarget.Type.KEYS_AND_VALUES;
                        break;
                    case CONTAINS:
                    case NOT_CONTAINS:
                        indexTargetType = IndexTarget.Type.VALUES;
                        break;
                    case CONTAINS_KEY:
                    case NOT_CONTAINS_KEY:
                        indexTargetType = IndexTarget.Type.KEYS;
                        break;
                    default:
                        throw new InvalidRequestException("Invalid operator " + operator + " for map type");
                }
            }
        }
        return indexTargetType;
    }

    /**
     * Get an iterator over the rows for this partition key. Builds a search view that includes all memtables and all
     * {@link SSTableSet#LIVE} sstables.
     * @param key
     * @param executionController
     * @return
     */
    public UnfilteredRowIterator getPartition(PrimaryKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        SinglePartitionReadCommand partition = getPartitionReadCommand(key, executionController);
        return partition.queryMemtableAndDisk(cfs, executionController);
    }

    /**
     * Get an iterator over the rows for this partition key. Restrict the search to the specified view.
     * @param key
     * @param executionController
     * @return
     */
    public UnfilteredRowIterator getPartition(PrimaryKey key, ColumnFamilyStore.ViewFragment view, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        SinglePartitionReadCommand partition = getPartitionReadCommand(key, executionController);

        // Class to transform the row to include its source table.
        Function<Object, Transformation<BaseRowIterator<?>>> rowTransformer = (Object sourceTable) -> new Transformation<>()
        {
            @Override
            protected Row applyToRow(Row row)
            {
                return new RowWithSourceTable(row, sourceTable);
            }
        };

        return partition.queryMemtableAndDisk(cfs, view, rowTransformer, executionController);
    }

    public SinglePartitionReadCommand getPartitionReadCommand(PrimaryKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        return SinglePartitionReadCommand.create(cfs.metadata(),
                                                 command.nowInSec(),
                                                 command.columnFilter(),
                                                 RowFilter.NONE,
                                                 DataLimits.NONE,
                                                 key.partitionKey(),
                                                 makeFilter(key));
    }

    Plan buildPlan()
    {
        Plan.KeysIteration keysIterationPlan = buildKeysIterationPlan();
        Plan.RowsIteration rowsIteration = planFactory.fetch(keysIterationPlan);
        rowsIteration = planFactory.recheckFilter(command.rowFilter(), rowsIteration);
        rowsIteration = planFactory.limit(rowsIteration, command.limits().rows());

        Plan optimizedPlan;
        optimizedPlan = QUERY_OPT_LEVEL > 0
                        ? rowsIteration.optimize()
                        : rowsIteration;
        optimizedPlan = RangeIntersectionIterator.INTERSECTION_CLAUSE_LIMIT > 0 && QUERY_OPT_LEVEL <= 1
                        ? optimizedPlan.limitIntersectedClauses(RangeIntersectionIterator.INTERSECTION_CLAUSE_LIMIT)
                        : optimizedPlan;

        if (optimizedPlan.contains(node -> node instanceof Plan.AnnIndexScan))
            queryContext.setFilterSortOrder(QueryContext.FilterSortOrder.SCAN_THEN_FILTER);
        if (optimizedPlan.contains(node -> node instanceof Plan.KeysSort))
            queryContext.setFilterSortOrder(QueryContext.FilterSortOrder.SEARCH_THEN_ORDER);

        if (logger.isTraceEnabled())
            logger.trace("Query execution plan:\n" + optimizedPlan.toStringRecursive());

        if (Tracing.isTracing())
        {
            Tracing.trace("Query execution plan:\n" + optimizedPlan.toStringRecursive());
            List<Plan.IndexScan> origIndexScans = keysIterationPlan.nodesOfType(Plan.IndexScan.class);
            List<Plan.IndexScan> selectedIndexScans = optimizedPlan.nodesOfType(Plan.IndexScan.class);
            Tracing.trace("Selecting {} {} of {} out of {} indexes",
                          selectedIndexScans.size(),
                          selectedIndexScans.size() > 1 ? "indexes with cardinalities" : "index with cardinality",
                          selectedIndexScans.stream().map(s -> "" + ((long) s.expectedKeys())).collect(Collectors.joining(", ")),
                          origIndexScans.size());
        }
        return optimizedPlan;
    }

    private Plan.KeysIteration buildKeysIterationPlan()
    {
        // Remove the ORDER BY filter expression from the filter tree, as it is added below.
        var filterElement = filterOperation().filter(e -> !Orderer.isFilterExpressionOrderer(e));
        Plan.KeysIteration keysIterationPlan = Operation.Node.buildTree(this, filterElement)
                                                             .analyzeTree(this)
                                                             .plan(this);

        // Because the orderer has a specific queue view
        if (orderer != null)
            keysIterationPlan = planFactory.sort(keysIterationPlan, orderer);

        assert keysIterationPlan != planFactory.everything; // This would mean we have no WHERE nor ANN clauses at all
        return keysIterationPlan;
    }


    public Iterator<? extends PrimaryKey> buildIterator(Plan plan)
    {
        try
        {
            Plan.KeysIteration keysIteration = plan.firstNodeOfType(Plan.KeysIteration.class);
            assert keysIteration != null : "No index scan found";
            return keysIteration.execute(this);
        }
        finally
        {
            // Because we optimize the plan, it is possible that there exist iterators that we
            // constructed but which weren't used by the final plan.
            // Let's close them here, so they don't hold the resources.
            closeUnusedIterators();
        }
    }

    /**
     * Creates an iterator over keys of rows that match given WHERE predicate.
     * Does not cache the iterator!
     */
    private RangeIterator buildIterator(Expression predicate)
    {
        QueryView view = getQueryView(predicate.context);
        return TermIterator.build(predicate, view.referencedIndexes, mergeRange, queryContext, false, Integer.MAX_VALUE);
    }

    /**
     * Creates a consistent view of indexes.
     * Invocations are memorized - multiple calls for the same context return the same view.
     * The views are kept for the lifetime of this {@code QueryController}.
     */
    QueryView getQueryView(IndexContext context)
    {
        return queryViews.computeIfAbsent(context,
                                          c -> new QueryView.Builder(c, mergeRange, queryContext).build());

    }


    private float avgCellsPerRow()
    {
        long cells = 0;
        long rows = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            rows += sstable.getTotalRows();
            cells += sstable.getEstimatedCellPerPartitionCount().mean() * sstable.getEstimatedCellPerPartitionCount().count();
        }
        return rows == 0 ? 0.0f : ((float) cells) / rows;
    }

    private float avgRowSizeInBytes()
    {
        long totalLength = 0;
        long rows = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            rows += sstable.getTotalRows();
            totalLength += sstable.uncompressedLength();
        }
        return rows == 0 ? 0.0f : ((float) totalLength) / rows;
    }


    public FilterTree buildFilter()
    {
        return Operation.Node.buildTree(this, filterOperation()).analyzeTree(this).filterTree();
    }

    /**
     * Build a {@link Plan} from the given list of expressions by applying given operation (OR/AND).
     * Building of such builder involves index search, results of which are persisted in the internal resources list
     *
     * @param builder The plan node builder which receives the built index scans
     * @param expressions The expressions to build the plan from
     */
    public void buildPlanForExpressions(Plan.Builder builder, Collection<Expression> expressions)
    {
        Operation.OperationType op = builder.type;
        assert !expressions.isEmpty() : "expressions should not be empty for " + op + " in " + command.rowFilter().root();

        // VSTODO move ANN out of expressions and into its own abstraction? That will help get generic ORDER BY support
        Collection<Expression> exp = expressions.stream().filter(e -> e.operation != Expression.Op.ORDER_BY).collect(Collectors.toList());

        // we cannot use indexes with OR if we have a mix of indexed and non-indexed columns (see CNDB-10142)
        if (op == Operation.OperationType.OR && !exp.stream().allMatch(e -> e.context.isIndexed()))
        {
            builder.add(planFactory.everything);
            return;
        }

        for (Expression expression : expressions)
        {
            if (expression.context.isIndexed())
            {
                long expectedMatchingRowCount = Math.min(estimateMatchingRowCount(expression), planFactory.tableMetrics.rows);
                builder.add(planFactory.indexScan(expression, expectedMatchingRowCount));
            }
        }
    }

    @Override
    public Iterator<? extends PrimaryKey> getKeysFromIndex(Expression predicate)
    {
        Collection<RangeIterator> rangeIterators = keyIterators.get(predicate);
        // This will be non-empty only if we created the iterator as part of the query planning process.
        if (!rangeIterators.isEmpty())
        {
            RangeIterator iterator = rangeIterators.iterator().next();
            keyIterators.remove(predicate, iterator);  // remove so we never accidentally reuse the same iterator
            return iterator;
        }

        return buildIterator(predicate);
    }

    /**
     * Use the configured {@link Orderer} to create an iterator that sorts the whole table by a specific column.
     */
    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> getTopKRows(Expression predicate, int softLimit)
    {
        List<CloseableIterator<PrimaryKeyWithSortKey>> memtableResults = new ArrayList<>();
        try
        {
            QueryView view = getQueryView(orderer.context);
            for (MemtableIndex index : view.memtableIndexes)
                memtableResults.addAll(index.orderBy(queryContext, orderer, predicate, mergeRange, softLimit));

            var totalRows = view.getTotalSStableRows();
            SSTableSearcher searcher = index -> index.orderBy(orderer, predicate, mergeRange, queryContext, softLimit, totalRows);
            var sstableResults = searchSSTables(view, searcher);
            sstableResults.addAll(memtableResults);
            return MergeIterator.getNonReducingCloseable(sstableResults, orderer.getComparator());
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(memtableResults);
            throw t;
        }
    }

    /**
     * Use the configured {@link Orderer} to sort the rows from the given source iterator.
     */
    public CloseableIterator<PrimaryKeyWithSortKey> getTopKRows(RangeIterator source, int softLimit)
    {
        try
        {
            if (!source.hasNext())
            {
                FileUtils.closeQuietly(source);
                return CloseableIterator.emptyIterator();
            }
            List<PrimaryKey> primaryKeys = new ArrayList<>();
            while (source.hasNext())
                primaryKeys.add(source.next());
            var result = getTopKRows(primaryKeys, softLimit);
            // We cannot close the source iterator eagerly because it produces partially loaded PrimaryKeys
            // that might not be needed until a deeper search into the ordering index, which happens after
            // we exit this block.
            return CloseableIterator.withOnClose(result, source);
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(source);
            throw t;
        }
    }

    private CloseableIterator<PrimaryKeyWithSortKey> getTopKRows(List<PrimaryKey> sourceKeys, int softLimit)
    {
        Tracing.logAndTrace(logger, "SAI predicates produced {} keys", sourceKeys.size());
        QueryView view = getQueryView(orderer.context);
        var memtableResults = view.memtableIndexes.stream()
                                                               .map(index -> index.orderResultsBy(queryContext,
                                                                                                  sourceKeys,
                                                                                                  orderer,
                                                                                                  softLimit))
                                                               .collect(Collectors.toList());
        try
        {
            var totalRows = view.getTotalSStableRows();
            SSTableSearcher ssTableSearcher = index -> index.orderResultsBy(queryContext,
                                                                            sourceKeys,
                                                                            orderer,
                                                                            softLimit,
                                                                            totalRows);
            var sstableScoredPrimaryKeyIterators = searchSSTables(view, ssTableSearcher);
            sstableScoredPrimaryKeyIterators.addAll(memtableResults);
            return MergeIterator.getNonReducingCloseable(sstableScoredPrimaryKeyIterators, orderer.getComparator());
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(memtableResults);
            throw t;
        }

    }


    @FunctionalInterface
    interface SSTableSearcher
    {
        List<CloseableIterator<PrimaryKeyWithSortKey>> search(SSTableIndex index) throws Exception;
    }

    /**
     * Create the list of iterators over {@link PrimaryKeyWithSortKey} from the given {@link QueryView}.
     * @param queryView The view to use to create the iterators.
     * @return The list of iterators over {@link PrimaryKeyWithSortKey}.
     */
    private List<CloseableIterator<PrimaryKeyWithSortKey>> searchSSTables(QueryView queryView, SSTableSearcher searcher)
    {
        List<CloseableIterator<PrimaryKeyWithSortKey>> results = new ArrayList<>();
        for (var index : queryView.referencedIndexes)
        {
            try
            {
                var iterators = searcher.search(index);
                results.addAll(iterators);
            }
            catch (Throwable ex)
            {
                // Close any iterators that were successfully opened before the exception
                FileUtils.closeQuietly(results);
                if (logger.isDebugEnabled() && !(ex instanceof AbortedOperationException))
                {
                    var msg = String.format("Failed search on index %s, aborting query.", index.getSSTable());
                    logger.debug(index.getIndexContext().logMessage(msg), ex);
                }
                throw Throwables.cleaned(ex);
            }
        }
        return results;
    }

    public IndexFeatureSet indexFeatureSet()
    {
        return indexFeatureSet;
    }

    public Orderer getOrderer()
    {
        return orderer;
    }

    /**
     * Returns whether this query is selecting the {@link PrimaryKey}.
     * The query selects the key if any of the following statements is true:
     *  1. The query is not row-aware
     *  2. The table associated with the query is not using clustering keys
     *  3. The clustering index filter for the command wants the row.
     *
     *  Item 3 is important in paged queries where the {@link ClusteringIndexSliceFilter} for
     *  subsequent paged queries may not select rows that are returned by the index
     *  search because that is initially partition based.
     *
     * @param key The {@link PrimaryKey} to be tested
     * @return true if the key is selected by the query
     */
    public boolean selects(PrimaryKey key)
    {
        return !indexFeatureSet.isRowAware() ||
               key.hasEmptyClustering() ||
               command.clusteringIndexFilter(key.partitionKey()).selects(key.clustering());
    }

    private StorageAttachedIndex getBestIndexFor(RowFilter.Expression expression)
    {
        return cfs.indexManager.getBestIndexFor(expression, StorageAttachedIndex.class).orElse(null);
    }

    // Note: This method assumes that the selects method has already been called for the
    // key to avoid having to (potentially) call selects twice
    private ClusteringIndexFilter makeFilter(PrimaryKey key)
    {
        ClusteringIndexFilter clusteringIndexFilter = command.clusteringIndexFilter(key.partitionKey());

        if (!indexFeatureSet.isRowAware() || key.hasEmptyClustering())
            return clusteringIndexFilter;
        else
            return new ClusteringIndexNamesFilter(FBUtilities.singleton(key.clustering(), cfs.metadata().comparator),
                                                  clusteringIndexFilter.isReversed());
    }

    /**
     * Used to release all resources and record metrics when query finishes.
     */
    public void finish()
    {
        closeUnusedIterators();
        closeQueryViews();
        if (tableQueryMetrics != null) tableQueryMetrics.record(queryContext);
    }

    private void closeUnusedIterators()
    {
        Iterator<Map.Entry<Expression, RangeIterator>> entries = keyIterators.entries().iterator();
        while (entries.hasNext())
        {
            FileUtils.closeQuietly(entries.next().getValue());
            entries.remove();
        }
    }

    private void closeQueryViews()
    {
        Iterator<Map.Entry<IndexContext, QueryView>> entries = queryViews.entrySet().iterator();
        while (entries.hasNext())
        {
            entries.next().getValue().close();
            entries.remove();
        }
    }

    /**
     * Returns the {@link DataRange} list covered by the specified {@link ReadCommand}.
     *
     * @param command a read command
     * @return the data ranges covered by {@code command}
     */
    private static List<DataRange> dataRanges(ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            SinglePartitionReadCommand cmd = (SinglePartitionReadCommand) command;
            DecoratedKey key = cmd.partitionKey();
            return Lists.newArrayList(new DataRange(new Bounds<>(key, key), cmd.clusteringIndexFilter()));
        }
        else if (command instanceof PartitionRangeReadCommand)
        {
            PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;
            return Lists.newArrayList(cmd.dataRange());
        }
        else if (command instanceof MultiRangeReadCommand)
        {
            MultiRangeReadCommand cmd = (MultiRangeReadCommand) command;
            return cmd.ranges();
        }
        else
        {
            throw new AssertionError("Unsupported read command type: " + command.getClass().getName());
        }
    }

    /**
     * Returns the total count of rows in the sstables which overlap with any of the given ranges
     * and all live memtables.
     */
    private long estimateTotalAvailableRows(List<DataRange> ranges)
    {
        long rows = 0;

        for (Memtable memtable : cfs.getAllMemtables())
            rows += Memtable.estimateRowCount(memtable);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            for (DataRange range : ranges)
                if (RangeUtil.intersects(sstable, range.keyRange()))
                    rows += sstable.getTotalRows();

        return rows;
    }

    /**
     * Estimates how many rows match the predicate.
     * There are no guarantees. The returned value may come with a significant estimation error.
     * You must not rely on this except for query optimization purposes.
     */
    private long estimateMatchingRowCount(Expression predicate)
    {
        switch (predicate.getOp())
        {
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
            case NOT_EQ:
            case NOT_CONTAINS_KEY:
            case NOT_CONTAINS_VALUE:
            case RANGE:
                return (indexFeatureSet.hasTermsHistogram())
                       ? estimateMatchingRowCountUsingHistograms(predicate)
                       : estimateMatchingRowCountUsingIndex(predicate);
            default:
                return estimateMatchingRowCountUsingIndex(predicate);
        }
    }

    /**
     * Estimates the number of matching rows by consulting the terms histograms on the indexes.
     * This is faster but the histograms are not available on indexes before V6.
     */
    private long estimateMatchingRowCountUsingHistograms(Expression predicate)
    {
        assert indexFeatureSet.hasTermsHistogram();
        var context = predicate.context;

        Collection<MemtableIndex> memtables = context.getLiveMemtables().values();
        long rowCount = 0;
        for (MemtableIndex index : memtables)
            rowCount += index.estimateMatchingRowsCount(predicate, mergeRange);

        var queryView = context.getView();
        for (SSTableIndex index : queryView.getIndexes())
            rowCount += index.estimateMatchingRowsCount(predicate, mergeRange);

        return rowCount;
    }

    /**
     * Legacy way of estimating predicate selectivity.
     * Runs the search on the index and returns the size of the iterator.
     * Caches the iterator for future use, to avoid doing search twice.
     */
    private long estimateMatchingRowCountUsingIndex(Expression predicate)
    {
        // For older indexes we don't have histograms, so we need to construct the iterator
        // and ask for the posting list size.
        RangeIterator iterator = buildIterator(predicate);

        // We're not going to consume the iterator here, so memorize it for future uses.
        // It can be used when executing the plan.
        keyIterators.put(predicate, iterator);
        return iterator.getMaxKeys();
    }


    @Override
    public double estimateAnnSearchCost(Orderer ordering, int limit, long candidates)
    {
        Preconditions.checkArgument(limit > 0, "limit must be > 0");

        IndexContext context = ordering.context;
        Collection<MemtableIndex> memtables = context.getLiveMemtables().values();
        View queryView = context.getView();

        double cost = 0;
        for (MemtableIndex index : memtables)
        {
            // FIXME convert nodes visited to search cost
            int memtableCandidates = (int) Math.min(Integer.MAX_VALUE, candidates);
            cost += ((VectorMemtableIndex) index).estimateAnnNodesVisited(limit, memtableCandidates);
        }

        long totalRows = 0;
        for (SSTableIndex index : queryView.getIndexes())
            totalRows += index.getSSTable().getTotalRows();

        for (SSTableIndex index : queryView.getIndexes())
        {
            for (Segment segment : index.getSegments())
            {
                if (!segment.intersects(mergeRange))
                    continue;
                int segmentLimit = segment.proportionalAnnLimit(limit, totalRows);
                int segmentCandidates = max(1, (int) (candidates * (double) segment.metadata.numRows / totalRows));
                cost += segment.estimateAnnSearchCost(segmentLimit, segmentCandidates);
            }
        }
        return cost;
    }
}
