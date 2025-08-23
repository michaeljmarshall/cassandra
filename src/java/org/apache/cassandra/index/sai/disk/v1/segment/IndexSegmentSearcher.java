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

import java.io.Closeable;
import java.io.IOException;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingListRangeIterator;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PeekablePostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Abstract reader for individual segments of an on-disk index.
 * <p>
 * Accepts shared resources (token/offset file readers), and uses them to perform lookups against on-disk data
 * structures.
 */
public abstract class IndexSegmentSearcher implements SegmentOrdering, Closeable
{
    final PrimaryKeyMap.Factory primaryKeyMapFactory;
    final PerColumnIndexFiles indexFiles;
    final SegmentMetadata metadata;
    final StorageAttachedIndex index;

    IndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                         PerColumnIndexFiles perIndexFiles,
                         SegmentMetadata segmentMetadata,
                         StorageAttachedIndex index)
    {
        this.primaryKeyMapFactory = primaryKeyMapFactory;
        this.indexFiles = perIndexFiles;
        this.metadata = segmentMetadata;
        this.index = index;
    }

    public static IndexSegmentSearcher open(SSTableContext sstableContext,
                                            PerColumnIndexFiles indexFiles,
                                            SegmentMetadata segmentMetadata,
                                            StorageAttachedIndex index) throws IOException
    {
        if (index.termType().isVector())
            return new VectorIndexSegmentSearcher(sstableContext, indexFiles, segmentMetadata, index);
        else if (index.termType().isLiteral())
            return new LiteralIndexSegmentSearcher(sstableContext.primaryKeyMapFactory, indexFiles, segmentMetadata, index);
        else
            return new NumericIndexSegmentSearcher(sstableContext.primaryKeyMapFactory, indexFiles, segmentMetadata, index);
    }

    /**
     * @return memory usage of underlying on-disk data structure
     */
    public abstract long indexFileCacheSize();

    /**
     * Search on-disk index synchronously.
     *
     * @param expression to filter on disk index
     * @param queryContext to track per sstable cache and per query metrics
     *
     * @return {@link KeyRangeIterator} with matches for the given expression
     */
    public abstract KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext) throws IOException;

    /**
     * Order the rows by the given Orderer.  Used for ORDER BY clause when
     * (1) the WHERE predicate is either a partition restriction or a range restriction on the index,
     * (2) there is no WHERE predicate, or
     * (3) the planner determines it is better to post-filter the ordered results by the predicate.
     *
     * @param orderer      the object containing the ordering logic
     * @param slice        optional predicate to get a slice of the index
     * @param keyRange     key range specific in read command, used by ANN index
     * @param queryContext to track per sstable cache and per query metrics
     * @param limit        the initial num of rows to returned, used by ANN index. More rows may be requested if filtering throws away more than expected!
     * @return an iterator of {@link PrimaryKeyWithScore} in score order
     */
//    public CloseableIterator<PrimaryKeyWithScore> orderBy(Orderer orderer, Expression slice, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, int limit) throws IOException
    public CloseableIterator<PrimaryKeyWithScore> orderBy(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context) throws IOException
    {
        throw new UnsupportedOperationException();
    }


    KeyRangeIterator toPrimaryKeyIterator(PostingList postingList, QueryContext queryContext) throws IOException
    {
        if (postingList == null || postingList.size() == 0)
            return KeyRangeIterator.empty();

        IndexSegmentSearcherContext searcherContext = new IndexSegmentSearcherContext(metadata.minKey,
                                                                                      metadata.maxKey,
                                                                                      metadata.rowIdOffset,
                                                                                      queryContext,
                                                                                      PeekablePostingList.makePeekable(postingList));

        return new PostingListRangeIterator(index.identifier(), primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(), searcherContext);
    }
}
