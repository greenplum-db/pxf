package org.greenplum.pxf.plugins.jdbc.partitioning;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EnumPartitionTest {
    private DbProduct dbProduct = null;

    private final String COL_RAW = "col";
    private final String QUOTE = "\"";
    private final String COL = QUOTE + COL_RAW + QUOTE;

    @Test
    public void testNormal() throws Exception {
        EnumPartition partition = new EnumPartition(COL_RAW, "enum1");
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " = 'enum1'",
            constraint
        );
    }

    @Test
    public void testExcluded1() throws Exception {
        EnumPartition partition = new EnumPartition(COL_RAW, new String[]{"enum1"});
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " <> 'enum1'",
            constraint
        );
    }

    @Test
    public void testExcluded2() throws Exception {
        EnumPartition partition = new EnumPartition(COL_RAW, new String[]{"enum1", "enum2"});
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " <> 'enum1' AND " + COL + " <> 'enum2'",
            constraint
        );
    }

    @Test
    public void testExcluded3() throws Exception {
        EnumPartition partition = new EnumPartition(COL_RAW, new String[]{"enum1", "enum2", "enum3"});
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " <> 'enum1' AND " + COL + " <> 'enum2' AND " + COL + " <> 'enum3'",
            constraint
        );
    }

    @Test(expected = AssertionError.class)
    public void testInvalidValueNull() throws Exception {
        new EnumPartition(COL_RAW, (String)null);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidColumnNullValue() throws Exception {
        new EnumPartition(null, "enum1");
    }

    @Test(expected = AssertionError.class)
    public void testInvalidExcludedNull() throws Exception {
        new EnumPartition(COL_RAW, (String[])null);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidColumnNullExcluded() throws Exception {
        new EnumPartition(null, new String[]{"enum1"});
    }

    @Test(expected = AssertionError.class)
    public void testInvalidExcludedZeroLength() throws Exception {
        new EnumPartition(COL_RAW, new String[]{});
    }

    @Test(expected = AssertionError.class)
    public void testInvalidNullQuoteString() throws Exception {
        EnumPartition partition = new EnumPartition(COL_RAW, "enum1");
        partition.toSqlConstraint(null, dbProduct);
    }
}
