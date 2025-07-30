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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.nio.ByteBuffer;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * A PrimaryKey with one piece of metadata. Subclasses define the metadata, and to prevent unnecessary boxing, the
 * metadata is not referenced in this calss. The metadata is not used to determine equality or hash code, but it is used
 * to compare the PrimaryKey objects.
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public abstract class PrimaryKeyWithScore implements Comparable<PrimaryKeyWithScore>
{
    protected final IndexContext context;
    private final PrimaryKey primaryKey;
    // Either a Memtable reference or an SSTableId reference
    private final Object sourceTable;

    private final float indexScore;

    protected PrimaryKeyWithSortKey(IndexContext context, Memtable sourceTable, PrimaryKey primaryKey, float indexScore)
    {
        this.context = context;
        this.sourceTable = sourceTable;
        this.primaryKey = primaryKey;
        this.indexScore = indexScore;
    }

    protected PrimaryKeyWithSortKey(IndexContext context, SSTableId sourceTable, PrimaryKey primaryKey, float indexScore)
    {
        this.context = context;
        this.sourceTable = sourceTable;
        this.primaryKey = primaryKey;
        this.indexScore = indexScore;
    }

    public PrimaryKey primaryKey()
    {
        return primaryKey;
    }

    public boolean isIndexDataValid(Row row, int nowInSecs)
    {
        assert context.getDefinition().isRegular() : "Only regular columns are supported, got " + context.getDefinition();
        var cell = row.getCell(context.getDefinition());
        if (!cell.isLive(nowInSecs))
            return false;
        assert cell instanceof CellWithSourceTable : "Expected CellWithSource, got " + cell.getClass();
        return sourceTable.equals(((CellWithSourceTable<?>) cell).sourceTable())
               && isIndexDataEqualToLiveData(cell.buffer());
    }

    @Override
    public int compareTo(PrimaryKeyWithScore o)
    {
        // Descending order
        return Float.compare(o.indexScore, indexScore);
    }
}
