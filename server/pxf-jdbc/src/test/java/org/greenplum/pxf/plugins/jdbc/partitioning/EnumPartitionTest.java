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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EnumPartitionTest {

    private DbProduct dbProduct = null;

    private final String COL_RAW = "col";
    private final String QUOTE = "\"";

    @Test
    public void testNormal() {
        EnumPartition partition = new EnumPartition(COL_RAW, "enum1");
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals("\"col\" = 'enum1'", constraint);
    }

    @Test
    public void testExcluded1() {
        EnumPartition partition = new EnumPartition(COL_RAW, new String[]{"enum1"});
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals("( \"col\" <> 'enum1' )", constraint);
    }

    @Test
    public void testExcluded2() {
        EnumPartition partition = new EnumPartition(COL_RAW, new String[]{"enum1", "enum2"});
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals("( \"col\" <> 'enum1' AND \"col\" <> 'enum2' )", constraint);
    }

    @Test
    public void testExcluded3() {
        EnumPartition partition = new EnumPartition(COL_RAW, new String[]{"enum1", "enum2", "enum3"});
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals("( \"col\" <> 'enum1' AND \"col\" <> 'enum2' AND \"col\" <> 'enum3' )", constraint);
    }

    @Test
    public void testInvalidValueNull() {
        assertThrows(RuntimeException.class,
            () -> new EnumPartition(COL_RAW, (String) null));
    }

    @Test
    public void testInvalidColumnNull() {
        assertThrows(RuntimeException.class,
            () -> new EnumPartition(null, "enum1"));
    }

    @Test
    public void testInvalidExcludedNull() {
        assertThrows(RuntimeException.class,
            () -> new EnumPartition(COL_RAW, (String[]) null));
    }

    @Test
    public void testInvalidColumnNullExcluded() {
        assertThrows(RuntimeException.class,
            () -> new EnumPartition(null, new String[]{"enum1"}));
    }

    @Test
    public void testInvalidExcludedZeroLength() {
        assertThrows(RuntimeException.class,
            () -> new EnumPartition(COL_RAW, new String[]{}));
    }

    @Test
    public void testInvalidNullQuoteString() {
        EnumPartition partition = new EnumPartition(COL_RAW, "enum1");
        assertThrows(RuntimeException.class,
            () -> partition.toSqlConstraint(null, dbProduct));
    }
}
