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

import java.nio.ByteBuffer;

import org.apache.cassandra.index.sai.IndexContext;

public class PrimaryKeyWithScore extends PrimaryKeyWithSortKey
{
    private final float[] queryVector;
    private final float indexScore;

    public PrimaryKeyWithScore(IndexContext context, PrimaryKey primaryKey, float[] queryVector, float indexScore)
    {
        super(context, primaryKey);
        this.queryVector = queryVector;
        this.indexScore = indexScore;
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (!(o instanceof PrimaryKeyWithScore))
            throw new IllegalArgumentException("Cannot compare PrimaryKeyWithScore with " + o.getClass().getSimpleName());

        // Sort by score in descending order
        return Float.compare(((PrimaryKeyWithScore) o).indexScore, indexScore);
    }

    @Override
    protected boolean isIndexDataValid(ByteBuffer value)
    {
        // Accept the Primary Key if its score, which comes from its source vector index, is greater than the score
        // of the row read from storage. If the score is less than the score of the row read from storage,
        // then it might not be in the global top k.
        var vector = TypeUtil.decomposeVector(context, value);
        var realScore = context.getIndexWriterConfig().getSimilarityFunction().compare(queryVector, vector);
        return indexScore < realScore + 0.0001f;
    }
}
