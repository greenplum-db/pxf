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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DatePartitionTestGenerate {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testPartitionByDateIntervalDay() throws Exception {
        final String COLUMN = "col";
        final String RANGE = "2008-01-01:2008-01-11";
        final String INTERVAL = "1:day";

        List<DatePartition> parts = DatePartition.generate(COLUMN, RANGE, INTERVAL);

        assertEquals(12, parts.size());
        assertDatePartitionEquals(parts.get(0), null, Date.valueOf("2008-01-01"));
        assertDatePartitionEquals(parts.get(1), Date.valueOf("2008-01-01"), Date.valueOf("2008-01-02"));
        assertDatePartitionEquals(parts.get(5), Date.valueOf("2008-01-05"), Date.valueOf("2008-01-06"));
        assertDatePartitionEquals(parts.get(10), Date.valueOf("2008-01-10"), Date.valueOf("2008-01-11"));
        assertDatePartitionEquals(parts.get(11), Date.valueOf("2008-01-11"), null);
    }

    @Test
    public void testPartitionByDateIntervalMonth() throws Exception {
        final String COLUMN = "col";
        final String RANGE = "2008-01-01:2008-12-31";
        final String INTERVAL = "1:month";

        List<DatePartition> parts = DatePartition.generate(COLUMN, RANGE, INTERVAL);

        assertEquals(13, parts.size());
        assertDatePartitionEquals(parts.get(0), null, Date.valueOf("2008-01-01"));
        assertDatePartitionEquals(parts.get(1), Date.valueOf("2008-01-01"), Date.valueOf("2008-02-01"));
        assertDatePartitionEquals(parts.get(6), Date.valueOf("2008-06-01"), Date.valueOf("2008-07-01"));
        assertDatePartitionEquals(parts.get(11), Date.valueOf("2008-11-01"), Date.valueOf("2008-12-01"));
        assertDatePartitionEquals(parts.get(12), Date.valueOf("2008-12-01"), null);
    }

    @Test
    public void testPartitionByDateIntervalYear() throws Exception {
        final String COLUMN = "col";
        final String RANGE = "2008-02-03:2018-02-02";
        final String INTERVAL = "1:year";

        List<DatePartition> parts = DatePartition.generate(COLUMN, RANGE, INTERVAL);

        assertEquals(11, parts.size());
        assertDatePartitionEquals(parts.get(0), null, Date.valueOf("2008-02-03"));
        assertDatePartitionEquals(parts.get(1), Date.valueOf("2008-02-03"), Date.valueOf("2009-02-03"));
        assertDatePartitionEquals(parts.get(6), Date.valueOf("2013-02-03"), Date.valueOf("2014-02-03"));
        assertDatePartitionEquals(parts.get(9), Date.valueOf("2016-02-03"), Date.valueOf("2017-02-03"));
        assertDatePartitionEquals(parts.get(10), Date.valueOf("2017-02-03"), null);
    }

    @Test
    public void testPartitionByDateIntervalYearIncomplete() throws Exception {
        final String COLUMN = "col";
        final String RANGE = "2008-02-03:2010-01-15";
        final String INTERVAL = "1:year";

        List<DatePartition> parts = DatePartition.generate(COLUMN, RANGE, INTERVAL);

        assertEquals(3, parts.size());
        assertDatePartitionEquals(parts.get(0), null, Date.valueOf("2008-02-03"));
        assertDatePartitionEquals(parts.get(1), Date.valueOf("2008-02-03"), Date.valueOf("2009-02-03"));
        assertDatePartitionEquals(parts.get(2), Date.valueOf("2009-02-03"), null);
    }

    @Test
    public void testRangeDateFormatInvalid() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        final String COLUMN = "col";
        final String RANGE = "2008/01/01:2009-01-01";
        final String INTERVAL = "1:month";

        DatePartition.generate(COLUMN, RANGE, INTERVAL);
    }

    @Test
    public void testIntervalValueInvalid() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        final String COLUMN = "col";
        final String RANGE = "2008-01-01:2009-01-01";
        final String INTERVAL = "-1:month";

        DatePartition.generate(COLUMN, RANGE, INTERVAL);
    }

    @Test
    public void testIntervalTypeInvalid() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        final String COLUMN = "col";
        final String RANGE = "2008-01-01:2011-01-01";
        final String INTERVAL = "6:hour";

        DatePartition.generate(COLUMN, RANGE, INTERVAL);
    }

    @Test
    public void testIntervalTypeMissingInvalid() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        final String COLUMN = "col";
        final String RANGE = "2008-01-01:2011-01-01";
        final String INTERVAL = "6";

        DatePartition.generate(COLUMN, RANGE, INTERVAL);
    }

    @Test
    public void testRangeMissingEndInvalid() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        final String COLUMN = "col";
        final String RANGE = "2008-01-01";
        final String INTERVAL = "1:year";

        DatePartition.generate(COLUMN, RANGE, INTERVAL);
    }

    @Test
    public void testRangeDateSwappedInvalid() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        final String COLUMN = "col";
        final String RANGE = "2008-01-01:2001-01-01";
        final String INTERVAL = "1:month";

        DatePartition.generate(COLUMN, RANGE, INTERVAL);
    }

    /**
     * Assert fragment metadata and given range of dates match.
     * @param fragment
     * @param rangeStart (null is allowed)
     * @param rangeEnd (null is allowed)
     */
    private void assertDatePartitionEquals(DatePartition partition, Date rangeStart, Date rangeEnd) {
        Date[] boundaries = partition.getBoundaries();
        assertEquals(rangeStart, boundaries[0]);
        assertEquals(rangeEnd, boundaries[1]);
    }
}
