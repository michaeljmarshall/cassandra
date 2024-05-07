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

package org.apache.cassandra.index.sai.view;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class RangeTermTree implements TermTree
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected final ByteBuffer min, max;
    protected final AbstractType<?> comparator;
    private final Map<Version, IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>>> rangeTrees;

    private RangeTermTree(ByteBuffer min, ByteBuffer max, Map<Version, IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>>> rangeTrees, AbstractType<?> comparator)
    {
        this.min = min;
        this.max = max;
        this.rangeTrees = rangeTrees;
        this.comparator = comparator;
    }

    public Set<SSTableIndex> search(Expression e)
    {
        Set<SSTableIndex> result = new HashSet<>();
        rangeTrees.forEach((version, rangeTree) -> {
            // Before DB, range queries on map entries required special encoding.
            if (Version.DB.onOrAfter(version))
            {
                ByteBuffer minTerm = e.lower == null ? min : e.lower.value.encoded;
                ByteBuffer maxTerm = e.upper == null ? max : e.upper.value.encoded;
                result.addAll(rangeTree.search(Interval.create(new Term(minTerm, comparator),
                                                               new Term(maxTerm, comparator),
                                                               null)));
            }
            else
            {
                ByteBuffer minTerm = e.lower == null ? min : e.getLowerBound();
                ByteBuffer maxTerm = e.upper == null ? max : e.getUpperBound();
                result.addAll(rangeTree.search(Interval.create(new Term(minTerm, comparator),
                                                               new Term(maxTerm, comparator),
                                                               null)));
            }
        });
        return result;
    }

    static class Builder extends TermTree.Builder
    {
        // Because different indexes can have different encodings, we must track the versions of the indexes
        final Map<Version, List<Interval<Term, SSTableIndex>>> intervalsByVersion = new HashMap<>();

        protected Builder(AbstractType<?> comparator)
        {
            super(comparator);
        }

        public void addIndex(SSTableIndex index)
        {
            Interval<Term, SSTableIndex> interval =
                    Interval.create(new Term(index.minTerm(), comparator), new Term(index.maxTerm(), comparator), index);

            if (logger.isTraceEnabled())
            {
                IndexContext context = index.getIndexContext();
                logger.trace(context.logMessage("Adding index for SSTable {} with minTerm={} and maxTerm={}..."), 
                                                index.getSSTable().descriptor, 
                                                index.minTerm() != null ? comparator.compose(index.minTerm()) : null,
                                                index.maxTerm() != null ? comparator.compose(index.maxTerm()) : null);
            }

            intervalsByVersion.compute(index.getVersion(), (__, list) ->
            {
                if (list == null)
                    list = new ArrayList<>();
                list.add(interval);
                return list;
            });
        }

        public TermTree build()
        {
            Map<Version, IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>>> trees = new HashMap<>();
            intervalsByVersion.forEach((version, intervals) -> trees.put(version, IntervalTree.build(intervals)));
            return new RangeTermTree(min, max, trees, comparator);
        }
    }

    /**
     * This is required since IntervalTree doesn't support custom Comparator
     * implementations and relied on items to be comparable which "raw" terms are not.
     */
    protected static class Term implements Comparable<Term>
    {
        private final ByteBuffer term;
        private final AbstractType<?> comparator;

        Term(ByteBuffer term, AbstractType<?> comparator)
        {
            this.term = term;
            this.comparator = comparator;
        }

        public int compareTo(Term o)
        {
            if (term == null && o.term == null)
                return 0;
            if (term == null)
                return -1;
            if (o.term == null)
                return 1;
            return TypeUtil.compare(term, o.term, comparator);
        }
    }
}
