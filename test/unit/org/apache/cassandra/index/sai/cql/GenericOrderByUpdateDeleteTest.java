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

package org.apache.cassandra.index.sai.cql;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.QueryController;

public class GenericOrderByUpdateDeleteTest extends SAITester
{

    @Before
    public void setup() throws Throwable
    {
        // Enable the optimizer by default. If there are any tests that need to disable it, they can do so explicitly.
        QueryController.QUERY_OPT_LEVEL = 1;
    }

    @Test
    public void testTextOverwrittenRowsInSameMemtableOrSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, str_val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val) VALUES (0, 'a')");
        execute("INSERT INTO %s (pk, str_val) VALUES (1, 'b')");
        execute("INSERT INTO %s (pk, str_val) VALUES (0, 'c')");

        beforeAndAfterFlush(() -> {
            assertRowsInBothOrder("SELECT pk FROM %s ORDER BY str_val", 10, row(1), row(0));
        });
    }

    @Test
    public void testIntOverwrittenRowsInSameMemtableOrSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, str_val int)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val) VALUES (0, 1)");
        execute("INSERT INTO %s (pk, str_val) VALUES (1, 2)");
        execute("INSERT INTO %s (pk, str_val) VALUES (0, 3)");

        beforeAndAfterFlush(() -> {
            assertRowsInBothOrder("SELECT pk FROM %s ORDER BY str_val", 10, row(1), row(0));
        });
    }

    private void assertRowsInBothOrder(String query, int limit, Object[]... rowsInAscendingOrder)
    {
        assertRows(execute(query + " ASC LIMIT " + limit), rowsInAscendingOrder);
        assertRows(execute(query + " DESC LIMIT " + limit), reverse(rowsInAscendingOrder));
    }

    private static Object[][] reverse(Object[][] rows)
    {
        Object[][] reversed = new Object[rows.length][];
        for (int i = 0; i < rows.length; i++)
        {
            reversed[i] = rows[rows.length - i - 1];
        }
        return reversed;
    }

}