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

package org.apache.cassandra.index.sai;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.disk.EmptyIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMapIterator;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeAntiJoinIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.SSTableWatcher;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * SSTableIndex is created for each column index on individual sstable to track per-column indexer.
 */
public class SSTableIndex
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIndex.class);

    // sort sstable index by first key then last key
    public static final Comparator<SSTableIndex> COMPARATOR = Comparator.comparing((SSTableIndex s) -> s.getSSTable().first)
                                                                        .thenComparing(s -> s.getSSTable().last)
                                                                        .thenComparing(s -> s.getSSTable().descriptor.id, SSTableIdFactory.COMPARATOR);

    private final SSTableContext sstableContext;
    private final IndexContext indexContext;
    private final SSTableReader sstable;
    private final SearchableIndex searchableIndex;
    private final IndexComponents.ForRead perIndexComponents;

    private final AtomicInteger references = new AtomicInteger(1);
    private final AtomicBoolean indexWasDropped = new AtomicBoolean(false);

    public SSTableIndex(SSTableContext sstableContext, IndexComponents.ForRead perIndexComponents)
    {
        assert perIndexComponents.context().getValidator() != null;
        this.perIndexComponents = perIndexComponents;
        this.searchableIndex = createSearchableIndex(sstableContext, perIndexComponents);

        this.sstableContext = sstableContext.sharedCopy(); // this line must not be before any code that may throw
        this.indexContext = perIndexComponents.context();
        this.sstable = sstableContext.sstable;
    }

    private static SearchableIndex createSearchableIndex(SSTableContext sstableContext, IndexComponents.ForRead perIndexComponents)
    {
        if (CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.getBoolean())
        {
            logger.info("Creating dummy (empty) index searcher for sstable {} as SAI index reads are disabled", sstableContext.sstable.descriptor);
            return new EmptyIndex();
        }

        return perIndexComponents.version().onDiskFormat().newSearchableIndex(sstableContext, perIndexComponents);
    }

    public IndexContext getIndexContext()
    {
        return indexContext;
    }

    /**
     * Returns the concrete on-disk perIndex components used by this index instance.
     */
    public IndexComponents.ForRead usedPerIndexComponents()
    {
        return perIndexComponents;
    }

    public SSTableContext getSSTableContext()
    {
        return sstableContext;
    }

    public List<Segment> getSegments()
    {
        return searchableIndex.getSegments();
    }

    public long indexFileCacheSize()
    {
        return searchableIndex.indexFileCacheSize();
    }

    /**
     * @return number of indexed rows, note that rows may have been updated or removed in sstable.
     */
    public long getRowCount()
    {
        return searchableIndex.getRowCount();
    }

    public long estimateMatchingRowsCount(Expression predicate, AbstractBounds<PartitionPosition> keyRange)
    {
        return searchableIndex.estimateMatchingRowsCount(predicate, keyRange);
    }

    /**
     * @return total size of per-column SAI components, in bytes
     */
    public long sizeOfPerColumnComponents()
    {
        return perIndexComponents.liveSizeOnDiskInBytes();
    }

    /**
     * @return total size of per-sstable SAI components, in bytes
     */
    public long sizeOfPerSSTableComponents()
    {
        return sstableContext.usedPerSSTableComponents().liveSizeOnDiskInBytes();
    }

    /**
     * @return the smallest possible sstable row id in this index.
     */
    public long minSSTableRowId()
    {
        return searchableIndex.minSSTableRowId();
    }

    /**
     * @return the largest possible sstable row id in this index.
     */
    public long maxSSTableRowId()
    {
        return searchableIndex.maxSSTableRowId();
    }

    public ByteBuffer minTerm()
    {
        return searchableIndex.minTerm();
    }

    public ByteBuffer maxTerm()
    {
        return searchableIndex.maxTerm();
    }

    public DecoratedKey minKey()
    {
        return searchableIndex.minKey();
    }

    public DecoratedKey maxKey()
    {
        return searchableIndex.maxKey();
    }

    public RangeIterator search(Expression expression,
                                AbstractBounds<PartitionPosition> keyRange,
                                QueryContext context,
                                boolean defer,
                                int limit) throws IOException
    {
        if (expression.getOp().isNonEquality())
        {
            // For NEQ, NOT_CONTAINS_KEY, NOT_CONTAINS_VALUE we return everything minus the keys matching
            // the expression.
            //
            // keys k such that row(k) not contains v = (all keys) \ (keys k such that row(k) contains v)
            //
            // Note that we will not match rows in other indexes,
            // so this can return false positives, but they are not a problem as post-filtering would get rid of them.
            // We could not safely substract the keys matched in other indexes as indexes may contain false positives
            // caused by deletes and updates.
            Expression negExpression = expression.negated();
            RangeIterator allKeys = allSSTableKeys(keyRange);
            RangeIterator matchedKeys = searchableIndex.search(negExpression, keyRange, context, defer, Integer.MAX_VALUE);
            return RangeAntiJoinIterator.create(allKeys, matchedKeys);
        }

        return searchableIndex.search(expression, keyRange, context, defer, limit);
    }

    public List<CloseableIterator<PrimaryKeyWithSortKey>> orderBy(Orderer orderer,
                                                                  Expression predicate,
                                                                  AbstractBounds<PartitionPosition> keyRange,
                                                                  QueryContext context,
                                                                  int limit,
                                                                  long totalRows) throws IOException
    {
        return searchableIndex.orderBy(orderer, predicate, keyRange, context, limit, totalRows);
    }

    public void populateSegmentView(SimpleDataSet dataSet)
    {
        searchableIndex.populateSystemView(dataSet, sstable);
    }

    public Version getVersion()
    {
        return perIndexComponents.version();
    }

    public IndexFeatureSet indexFeatureSet()
    {
        return getVersion().onDiskFormat().indexFeatureSet();
    }

    public SSTableReader getSSTable()
    {
        return sstable;
    }

    public boolean reference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
            {
                return true;
            }
        }
    }

    public boolean isReleased()
    {
        return references.get() <= 0;
    }

    public boolean isEmpty()
    {
        return searchableIndex instanceof EmptyIndex;
    }

    public void release()
    {
        int n = references.decrementAndGet();

        if (n == 0)
        {
            FileUtils.closeQuietly(searchableIndex);
            sstableContext.close();

            /*
             * When SSTable is removed, storage-attached index components will be automatically removed by LogTransaction.
             * We only remove index components explicitly in case of index corruption or index rebuild if immutable
             * components are not in use.
             */
            if (indexWasDropped.get())
                SSTableWatcher.instance.onIndexDropped(perIndexComponents.forWrite());
        }
    }

    /**
     * Indicates that this index has been dropped by the user, and so the underlying files can be safely removed.
     */
    public void markIndexDropped()
    {
        indexWasDropped.getAndSet(true);
        release();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableIndex other = (SSTableIndex)o;
        return Objects.equal(sstableContext, other.sstableContext) && Objects.equal(indexContext, other.indexContext);
    }

    public int hashCode()
    {
        return Objects.hashCode(sstableContext, indexContext);
    }

    public List<CloseableIterator<PrimaryKeyWithSortKey>> orderResultsBy(QueryContext context, List<PrimaryKey> keys, Orderer orderer, int limit, long totalRows) throws IOException
    {
        return searchableIndex.orderResultsBy(context, keys, orderer, limit, totalRows);
    }

    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("column", indexContext.getColumnName())
                          .add("sstable", sstable.descriptor)
                          .add("totalRows", sstable.getTotalRows())
                          .toString();
    }

    protected final RangeIterator allSSTableKeys(AbstractBounds<PartitionPosition> keyRange) throws IOException
    {
        return PrimaryKeyMapIterator.create(sstableContext, keyRange);
    }
}
