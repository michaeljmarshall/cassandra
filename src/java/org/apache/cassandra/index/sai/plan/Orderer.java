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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumSet;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.TypeUtil;

public class Orderer
{
    public enum Order
    {
        ASC, DESC
    }

    final static EnumSet<Operator> ORDER_BY_OPERATORS = EnumSet.of(Operator.ANN,
                                                                           Operator.SORT_ASC,
                                                                           Operator.SORT_DESC);

    public final IndexContext context;
    public final Operator operator;
    public final float[] vector;


    public Orderer(IndexContext context, Operator operator, ByteBuffer term)
    {
        this.context = context;
        assert ORDER_BY_OPERATORS.contains(operator) : "Invalid operator for order by clause " + operator;
        this.operator = operator;
        this.vector = context.getValidator().isVector() ? TypeUtil.decomposeVector(context.getValidator(), term) : null;
    }

    private Orderer(IndexContext context, RowFilter.Expression expression)
    {
        this(context, expression.operator(), expression.getIndexValue());
    }

//
//    public Order getOrder()
//    {
//        return order;
//    }
//
//    @Override
//    public String toString()
//    {
//        return field + " " + order;
//    }

    @Nullable
    public static Orderer from(SecondaryIndexManager indexManager, RowFilter filter)
    {
        var expressions = filter.root().expressions().stream().filter(exp -> ORDER_BY_OPERATORS.contains(exp.operator())).collect(Collectors.toList());
        if (expressions.isEmpty())
            return null;
        var orderRowFilter = expressions.get(0);
        var index = indexManager.getBestIndexFor(orderRowFilter, StorageAttachedIndex.class)
                                .orElseThrow(() -> new IllegalStateException("No index found for order by clause"));
        return new Orderer(index.getIndexContext(), orderRowFilter);
    }

    public static boolean isFilterExpressionOrderer(RowFilter.Expression expression)
    {
        return ORDER_BY_OPERATORS.contains(expression.operator());
    }
}
