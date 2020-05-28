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

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DatePartitionTest {

    private DbProduct dbProduct = DbProduct.POSTGRES;

    private final String COL_RAW = "col";
    private final String QUOTE = "\"";
    private final String COL = QUOTE + COL_RAW + QUOTE;

    @Test
    public void testNormal() {
        DatePartition partition = new DatePartition(COL_RAW, LocalDate.parse("2000-01-01"), LocalDate.parse("2000-01-02"));
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " >= date'2000-01-01' AND " + COL + " < date'2000-01-02'",
            constraint
        );
        assertEquals(COL_RAW, partition.getColumn());
    }

    @Test
    public void testRightBounded() {
        DatePartition partition = new DatePartition(COL_RAW, null, LocalDate.parse("2000-01-01"));
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " < date'2000-01-01'",
            constraint
        );
    }

    @Test
    public void testLeftBounded() {
        DatePartition partition = new DatePartition(COL_RAW, LocalDate.parse("2000-01-01"), null);
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " >= date'2000-01-01'",
            constraint
        );
    }

    @Test
    public void testSpecialDateValue() {
        DatePartition partition = new DatePartition(COL_RAW, LocalDate.parse("0001-01-01"), LocalDate.parse("1970-01-02"));
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " >= date'0001-01-01' AND " + COL + " < date'1970-01-02'",
            constraint
        );
    }

    @Test
    public void testInvalidBothBoundariesNull() {
        Exception ex = assertThrows(RuntimeException.class,
            () -> new DatePartition(COL_RAW, null, null));
        assertEquals("Both boundaries cannot be null", ex.getMessage());
    }

    @Test
    public void testInvalidColumnNull() {
        assertThrows(RuntimeException.class,
            () -> new DatePartition(null, LocalDate.parse("2000-01-01"), LocalDate.parse("2000-01-02")));
    }

    @Test
    public void testInvalidEqualBoundaries() {
        assertThrows(RuntimeException.class,
            () -> new DatePartition(COL_RAW, LocalDate.parse("2000-01-01"), LocalDate.parse("2000-01-01")));
    }

    @Test
    public void testInvalidNullQuoteString() {
        DatePartition partition = new DatePartition(COL_RAW, LocalDate.parse("2000-01-01"), LocalDate.parse("2000-01-02"));
        assertThrows(RuntimeException.class,
            () -> partition.toSqlConstraint(null, dbProduct));
    }

    @Test
    public void testInvalidNullDbProduct() {
        DatePartition partition = new DatePartition(COL_RAW, LocalDate.parse("2000-01-01"), LocalDate.parse("2000-01-02"));
        assertThrows(RuntimeException.class,
            () -> partition.toSqlConstraint(COL, null));
    }
}
