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
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class PrimaryKeyWithByteComparable extends PrimaryKeyWithSortKey
{
    private final ByteComparable byteComparable;

    public PrimaryKeyWithByteComparable(IndexContext context, PrimaryKey primaryKey, ByteComparable byteComparable)
    {
        super(context, primaryKey);
        this.byteComparable = byteComparable;
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (!(o instanceof PrimaryKeyWithByteComparable))
            throw new IllegalArgumentException("Cannot compare PrimaryKeyWithByteComparable with " + o.getClass().getSimpleName());

        // TODO is this the right version?
        return ByteComparable.compare(byteComparable, ((PrimaryKeyWithByteComparable) o).byteComparable, ByteComparable.Version.OSS41);
    }

    @Override
    protected boolean isIndexDataValid(ByteBuffer value)
    {
        if (value == null)
            return false;
        var wrapped = ByteComparable.fixedLength(value);
        // TODO is the version valid? how efficient is this comparison?
        return ByteComparable.compare(byteComparable, wrapped, ByteComparable.Version.OSS41) == 0;
    }
}
