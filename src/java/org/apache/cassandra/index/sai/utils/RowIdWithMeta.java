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

abstract public class RowIdWithMeta
{
    // TODO isn't a segment row id int? Why is it long everywhere?
    private final long segmentRowId;

    public RowIdWithMeta(long segmentRowId)
    {
        this.segmentRowId = segmentRowId;
    }

    public final long getSegmentRowId()
    {
        return segmentRowId;
    }

    public PrimaryKeyWithSortKey buildPrimaryKeyWithSortKey(IndexContext indexContext, PrimaryKeyMap primaryKeyMap, long segmentRowIdOffset)
    {
        var pk = primaryKeyMap.primaryKeyFromRowId(segmentRowIdOffset + segmentRowId);
        return wrapPrimaryKey(indexContext, pk);
    }

    abstract protected PrimaryKeyWithSortKey wrapPrimaryKey(IndexContext indexContext, PrimaryKey primaryKey);
}
