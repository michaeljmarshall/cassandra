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
import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.InvertedIndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class V4InvertedIndexSearcher extends InvertedIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    V4InvertedIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                            PerIndexFiles perIndexFiles,
                            SegmentMetadata segmentMetadata,
                            IndexDescriptor indexDescriptor,
                            IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext, Version.DB);
    }

    @Override
    protected PostingList searchPosting(Expression exp, QueryContext context)
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp().isEquality() || exp.getOp() == Expression.Op.MATCH)
        {
            final ByteComparable term = V4OnDiskFormat.instance.encode(exp.lower.value.encoded, indexContext.getValidator());
            QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            return reader.exactMatch(term, listener, context);
        }
        else if (exp.getOp() == Expression.Op.RANGE)
        {
            var someType = indexContext.getValidator();
            if (!(someType instanceof CompositeType))
                throw new IllegalStateException("Only composite types are supported for range queries for now");
            var type = (CompositeType) someType;

            // TODO how do we make this generic so we don't explicitly reference the ByteBufferAccessor.instance here?
            ByteComparable lower = null;
            if (exp.lower != null)
            {
                var terminator = exp.lower.inclusive ? ByteSource.TERMINATOR : ByteSource.GT_NEXT_COMPONENT;
                lower = v -> type.asComparableBytes(ByteBufferAccessor.instance, exp.lower.value.raw, v, terminator);
            }
            ByteComparable upper = null;
            if (exp.upper != null)
            {
                var terminator = exp.upper.inclusive ? ByteSource.TERMINATOR : ByteSource.LT_NEXT_COMPONENT;
                upper = v -> type.asComparableBytes(ByteBufferAccessor.instance, exp.upper.value.raw, v, terminator);
            }

            QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            return reader.rangeMatch(lower, upper, listener, context);
        }
        throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression: " + exp));
    }
}
