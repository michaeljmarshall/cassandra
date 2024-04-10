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

package org.apache.cassandra.index.sai.utils;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.io.sstable.SSTableId;

public class RowIdWithScore extends RowIdWithMeta implements Comparable<RowIdWithScore>
{
    private final float[] queryVector;
    private final float score;

    public RowIdWithScore(int segmentRowId, float[] queryVector, float score)
    {
        super(segmentRowId);
        this.queryVector = queryVector;
        this.score = score;
    }

    // TODO keep or make class Comparable?
    public float getScore()
    {
        return score;
    }

    @Override
    public int compareTo(RowIdWithScore o)
    {
        // Compare descending always for vector
        return Float.compare(o.score, score);
    }

    @Override
    protected PrimaryKeyWithSortKey wrapPrimaryKey(IndexContext indexContext, SSTableId<?> sstableId, PrimaryKey primaryKey)
    {
        return new PrimaryKeyWithScore(indexContext, sstableId, primaryKey, queryVector, score);
    }
}
