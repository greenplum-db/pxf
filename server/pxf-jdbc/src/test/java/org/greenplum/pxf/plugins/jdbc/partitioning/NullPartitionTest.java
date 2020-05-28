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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NullPartitionTest {

    private DbProduct dbProduct = null;

    private final String COL_RAW = "col";
    private final String QUOTE = "\"";
    private final String COL = QUOTE + COL_RAW + QUOTE;

    @Test
    public void testNormal() {
        NullPartition partition = new NullPartition(COL_RAW);
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(COL + " IS NULL", constraint);
        assertTrue(partition.isNull());
    }

    @Test
    public void testIsNotNull() {
        NullPartition partition = new NullPartition(COL_RAW, false);
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(COL + " IS NOT NULL", constraint);
    }

    @Test
    public void testInvalidColumn() {
        assertThrows(RuntimeException.class,
            () -> new NullPartition(null));
    }

    @Test
    public void testInvalidNullQuoteString() {
        NullPartition partition = new NullPartition(COL_RAW);
        assertThrows(RuntimeException.class,
            () -> partition.toSqlConstraint(null, dbProduct));
    }
}
