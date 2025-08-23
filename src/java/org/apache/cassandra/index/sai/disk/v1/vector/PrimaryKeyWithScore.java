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

import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.utils.CellWithSourceTable;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * A PrimaryKey with one piece of metadata. Subclasses define the metadata, and to prevent unnecessary boxing, the
 * metadata is not referenced in this calss. The metadata is not used to determine equality or hash code, but it is used
 * to compare the PrimaryKey objects.
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public class PrimaryKeyWithScore implements Comparable<PrimaryKeyWithScore>
{
    protected final ColumnMetadata columnMetadata;
    private final PrimaryKey primaryKey;
    // Either a Memtable reference or an SSTableId reference
    private final Object sourceTable;

    private final float indexScore;

    public PrimaryKeyWithScore(ColumnMetadata columnMetadata, Memtable sourceTable, PrimaryKey primaryKey, float indexScore)
    {
        this.columnMetadata = columnMetadata;
        this.sourceTable = sourceTable;
        this.primaryKey = primaryKey;
        this.indexScore = indexScore;
    }

    public PrimaryKeyWithScore(ColumnMetadata columnMetadata, SSTableId sourceTable, PrimaryKey primaryKey, float indexScore)
    {
        this.columnMetadata = columnMetadata;
        this.sourceTable = sourceTable;
        this.primaryKey = primaryKey;
        this.indexScore = indexScore;
    }

    public PrimaryKey primaryKey()
    {
        return primaryKey;
    }

    public boolean isIndexDataValid(Row row, long nowInSecs)
    {
        assert columnMetadata.isRegular() : "Only regular columns are supported, got " + columnMetadata;
        var cell = row.getCell(columnMetadata);
        if (!cell.isLive(nowInSecs))
            return false;
        assert cell instanceof CellWithSourceTable : "Expected CellWithSource, got " + cell.getClass();
        boolean x = sourceTable.equals(((CellWithSourceTable<?>) cell).sourceTable());
        System.out.println("isIndexDataValid: " + x + " " + sourceTable + " " + ((CellWithSourceTable<?>) cell).sourceTable());
        return x;
    }

    @Override
    public int compareTo(PrimaryKeyWithScore o)
    {
        // Descending order
        return Float.compare(o.indexScore, indexScore);
    }
}
