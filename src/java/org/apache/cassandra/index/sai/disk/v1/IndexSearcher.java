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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RowIdWithMeta;
import org.apache.cassandra.index.sai.utils.RowIdToPrimaryKeyWithSortKeyIterator;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Abstract reader for individual segments of an on-disk index.
 *
 * Accepts shared resources (token/offset file readers), and uses them to perform lookups against on-disk data
 * structures.
 */
public abstract class IndexSearcher implements Closeable, SegmentOrdering
{
    protected final PrimaryKeyMap.Factory primaryKeyMapFactory;
    final PerIndexFiles indexFiles;
    protected final SegmentMetadata metadata;
    final IndexDescriptor indexDescriptor;
    protected final IndexContext indexContext;

    protected IndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                            PerIndexFiles perIndexFiles,
                            SegmentMetadata segmentMetadata,
                            IndexDescriptor indexDescriptor,
                            IndexContext indexContext)
    {
        this.primaryKeyMapFactory = primaryKeyMapFactory;
        this.indexFiles = perIndexFiles;
        this.metadata = segmentMetadata;
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
    }

    /**
     * @return memory usage of underlying on-disk data structure
     */
    public abstract long indexFileCacheSize();

    /**
     * Search on-disk index synchronously
     *
     * @param expression   to filter on disk index
     * @param keyRange     key range specific in read command, used by ANN index
     * @param queryContext to track per sstable cache and per query metrics
     * @param defer        create the iterator in a deferred state
     * @param limit        the num of rows to returned, used by ANN index
     * @return {@link RangeIterator} that matches given expression
     */
    public abstract RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, boolean defer, int limit) throws IOException;

    /**
     * Order the on-disk index synchronously and produce an iterator in score order
     *
     * @param expression   to filter on disk index
     * @param keyRange     key range specific in read command, used by ANN index
     * @param queryContext to track per sstable cache and per query metrics
     * @param limit        the num of rows to returned, used by ANN index
     * @return an iterator of {@link PrimaryKeyWithSortKey} in score order
     */
    public CloseableIterator<? extends PrimaryKeyWithSortKey> orderBy(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, int limit) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    protected RangeIterator toPrimaryKeyIterator(PostingList postingList, QueryContext queryContext) throws IOException
    {
        if (postingList == null || postingList.size() == 0)
            return RangeIterator.empty();

        IndexSearcherContext searcherContext = new IndexSearcherContext(metadata.minKey,
                                                                        metadata.maxKey,
                                                                        metadata.minSSTableRowId,
                                                                        metadata.maxSSTableRowId,
                                                                        metadata.segmentRowIdOffset,
                                                                        queryContext,
                                                                        postingList.peekable());
        return new PostingListRangeIterator(indexContext, primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(), searcherContext);
    }

    protected CloseableIterator<? extends PrimaryKeyWithSortKey> toMetaSortedIterator(CloseableIterator<? extends RowIdWithMeta> rowIdIterator, QueryContext queryContext) throws IOException
    {
        if (rowIdIterator == null || !rowIdIterator.hasNext())
            return CloseableIterator.emptyIterator();

        IndexSearcherContext searcherContext = new IndexSearcherContext(metadata.minKey,
                                                                        metadata.maxKey,
                                                                        metadata.minSSTableRowId,
                                                                        metadata.maxSSTableRowId,
                                                                        metadata.segmentRowIdOffset,
                                                                        queryContext,
                                                                        null);
        return new RowIdToPrimaryKeyWithSortKeyIterator(indexContext,
                                                        rowIdIterator,
                                                        primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(),
                                                        searcherContext);
    }

    /** Create a sublist of the keys within (inclusive) this segment's bounds */
    protected List<PrimaryKey> getKeysInRange(List<PrimaryKey> keys)
    {
        int minIndex = findBoundaryIndex(keys, true);
        int maxIndex = findBoundaryIndex(keys, false);
        return keys.subList(minIndex, maxIndex);
    }

    private int findBoundaryIndex(List<PrimaryKey> keys, boolean findMin)
    {
        // The minKey and maxKey are sometimes just partition keys (not primary keys), so binarySearch
        // may not return the index of the least/greatest match.
        var key = findMin ? metadata.minKey : metadata.maxKey;
        int index = Collections.binarySearch(keys, key);
        if (index < 0)
            return -index - 1;
        if (findMin)
        {
            while (index > 0 && keys.get(index - 1).equals(key))
                index--;
        }
        else
        {
            while (index < keys.size() - 1 && keys.get(index + 1).equals(key))
                index++;
            // We must include the PrimaryKey at the boundary
            index++;
        }
        return index;
    }
}
