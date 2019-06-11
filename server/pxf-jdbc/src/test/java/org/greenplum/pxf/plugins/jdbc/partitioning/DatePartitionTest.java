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

import org.greenplum.pxf.plugins.jdbc.partitioning.DatePartition;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static org.junit.Assert.assertEquals;

public class DatePartitionTest {
    private DbProduct dbProduct = DbProduct.POSTGRES;

    private final String COL_RAW = "col";
    private final String QUOTE = "\"";
    private final String COL = QUOTE + COL_RAW + QUOTE;

    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    @Test
    public void testNormal() throws Exception {
        DatePartition partition = new DatePartition(COL_RAW, getCalendar("2000-01-01"), getCalendar("2000-01-02"));
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " >= date'2000-01-01' AND " + COL + " < date'2000-01-02'",
            constraint
        );
    }

    @Test
    public void testNormalWithEndIncluded() throws Exception {
        DatePartition partition = new DatePartition(COL_RAW, getCalendar("2000-01-01"), getCalendar("2000-01-02"), true);
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " >= date'2000-01-01' AND " + COL + " <= date'2000-01-02'",
            constraint
        );
    }

    @Test
    public void testRightBounded() throws Exception {
        DatePartition partition = new DatePartition(COL_RAW, null, getCalendar("2000-01-01"));
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " < date'2000-01-01'",
            constraint
        );
    }

    @Test
    public void testRightBoundedWithEndIncluded() throws Exception {
        DatePartition partition = new DatePartition(COL_RAW, null, getCalendar("2000-01-01"), true);
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " < date'2000-01-01'",
            constraint
        );
    }

    @Test
    public void testLeftBounded() throws Exception {
        DatePartition partition = new DatePartition(COL_RAW, getCalendar("2000-01-01"), null);
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " > date'2000-01-01'",
            constraint
        );
    }

    @Test
    public void testLeftBoundedWithEndIncluded() throws Exception {
        DatePartition partition = new DatePartition(COL_RAW, getCalendar("2000-01-01"), null, true);
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " > date'2000-01-01'",
            constraint
        );
    }

    @Test
    public void testEqualBoundaries() throws Exception {
        DatePartition partition = new DatePartition(COL_RAW, getCalendar("2000-01-01"), getCalendar("2000-01-01"));
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " = date'2000-01-01'",
            constraint
        );
    }

    @Test
    public void testSpecialDateValue() throws Exception {
        DatePartition partition = new DatePartition(COL_RAW, getCalendar("0001-01-01"), getCalendar("1970-01-02"));
        String constraint = partition.toSqlConstraint(QUOTE, dbProduct);

        assertEquals(
            COL + " >= date'0001-01-01' AND " + COL + " < date'1970-01-02'",
            constraint
        );
    }

    @Test(expected = AssertionError.class)
    public void testInvalidBothBoundariesNull() throws Exception {
        new DatePartition(COL_RAW, null, null);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidColumnNull() throws Exception {
        new DatePartition(null, getCalendar("2000-01-01"), getCalendar("2000-01-02"));
    }

    @Test(expected = AssertionError.class)
    public void testInvalidNullQuoteString() throws Exception {
        DatePartition partition = new DatePartition(COL_RAW, getCalendar("2000-01-01"), getCalendar("2000-01-02"));
        partition.toSqlConstraint(null, dbProduct);
    }

    /**
     * Get Calendar object for the given date in format 'yyyy-MM-dd'
     * @param date
     * @return Calendar
     * @throws ParseException
     */
    private Calendar getCalendar(String date) throws ParseException {
        Calendar result = Calendar.getInstance();
        result.setTime(df.parse(date));
        return result;
    }
}
