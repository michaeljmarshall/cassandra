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

package org.apache.cassandra.index.sai.disk.v4;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class V4OnDiskFormat extends V3OnDiskFormat
{
    public static final V4OnDiskFormat instance = new V4OnDiskFormat();

    @Override
    public IndexSearcher newIndexSearcher(SSTableContext sstableContext,
                                          IndexContext indexContext,
                                          PerIndexFiles indexFiles,
                                          SegmentMetadata segmentMetadata) throws IOException
    {
        if (indexContext.isLiteral())
            return new V4InvertedIndexSearcher(sstableContext.primaryKeyMapFactory(), indexFiles, segmentMetadata, sstableContext.indexDescriptor, indexContext);
        return super.newIndexSearcher(sstableContext, indexContext, indexFiles, segmentMetadata);
    }

    @Override
    public ByteComparable encode(ByteBuffer input, AbstractType<?> type)
    {
        // TODO how important is the terminator? A terminator is used for range queries to simplify LT, EQ, and GT
        //  bounds by preventing prefix matches. However, we don't support range queries on text fields. Is that something
        //  we want to support?
        // If we don't want range queries on text fields, then it's not necessary because encoding with
        // the type's comparator will prevent weakly prefix free trie encodings that will meet out needs.
        return TypeUtil.isUTF8OrAscii(type) || TypeUtil.isFrozen(type)
               ? version -> ByteSource.appendTerminator(ByteSource.of(input, version), ByteSource.TERMINATOR)
               : TypeUtil.asComparableBytes(input, type);
    }

    // Note: we do not need to override the unescape method because we still unescape all terms that go in the trie.
}