package org.greenplum.pxf.plugins.jdbc;

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

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.jdbc.utils.ByteUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcPartitionFragmenterTest {
    RequestContext context;

    @Before
    public void setUp() throws Exception {
        context = mock(RequestContext.class);
        when(context.getDataSource()).thenReturn("sales");
    }

    // No partition

    @Test
    public void testNoPartition() throws Exception {

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(1, fragments.size());
    }

    // DATE

    @Test
    public void testPartitionByDateIntervalDay() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2008-01-11");
        when(context.getOption("INTERVAL")).thenReturn("1:day");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(10, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), Date.valueOf("2008-01-01"), Date.valueOf("2008-01-02"));
        assertFragmentRangeEquals(fragments.get(5), Date.valueOf("2008-01-06"), Date.valueOf("2008-01-07"));
        assertFragmentRangeEquals(fragments.get(9), Date.valueOf("2008-01-10"), Date.valueOf("2008-01-11"));
    }

    @Test
    public void testPartionByDateIntervalMonth() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(context.getOption("INTERVAL")).thenReturn("1:month");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(12, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), Date.valueOf("2008-01-01"), Date.valueOf("2008-02-01"));
        assertFragmentRangeEquals(fragments.get(5), Date.valueOf("2008-06-01"), Date.valueOf("2008-07-01"));
        assertFragmentRangeEquals(fragments.get(11), Date.valueOf("2008-12-01"), Date.valueOf("2009-01-01"));
    }

    @Test
    public void testPartitionByDateIntervalMonthRangeSwapped() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2001-01-01");
        when(context.getOption("INTERVAL")).thenReturn("1:month");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(0, fragments.size());
    }

    @Test
    public void testPartionByDateIntervalYear() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-02-03:2018-02-03");
        when(context.getOption("INTERVAL")).thenReturn("1:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(10, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), Date.valueOf("2008-02-03"), Date.valueOf("2009-02-03"));
        assertFragmentRangeEquals(fragments.get(5), Date.valueOf("2013-02-03"), Date.valueOf("2014-02-03"));
        assertFragmentRangeEquals(fragments.get(9), Date.valueOf("2017-02-03"), Date.valueOf("2018-02-03"));
    }

    @Test
    public void testPartionByDateIntervalYearIncomplete() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-02-03:2010-01-15");
        when(context.getOption("INTERVAL")).thenReturn("1:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(2, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), Date.valueOf("2008-02-03"), Date.valueOf("2009-02-03"));
        assertFragmentRangeEquals(fragments.get(1), Date.valueOf("2009-02-03"), Date.valueOf("2010-01-15"));
    }

    @Test
    public void testPartionByDateIntervalYearRangeInfiniteLeft() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn(":2000-01-01:2010-01-01");
        when(context.getOption("INTERVAL")).thenReturn("5:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(3, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), Long.MAX_VALUE, Date.valueOf("2000-01-01").getTime());
        assertFragmentRangeEquals(fragments.get(1), Date.valueOf("2000-01-01"), Date.valueOf("2005-01-01"));
        assertFragmentRangeEquals(fragments.get(2), Date.valueOf("2005-01-01"), Date.valueOf("2010-01-01"));
    }

    @Test
    public void testPartionByDateIntervalYearRangeInfiniteRight() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2000-01-01:2010-01-01:");
        when(context.getOption("INTERVAL")).thenReturn("5:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(3, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), Date.valueOf("2000-01-01"), Date.valueOf("2005-01-01"));
        assertFragmentRangeEquals(fragments.get(1), Date.valueOf("2005-01-01"), Date.valueOf("2010-01-01"));
        assertFragmentRangeEquals(fragments.get(2), Date.valueOf("2010-01-01").getTime(), Long.MIN_VALUE);
    }

    @Test
    public void testPartionByDateIntervalYearRangeInfiniteBoth() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn(":2000-01-01:2010-01-01:");
        when(context.getOption("INTERVAL")).thenReturn("5:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(4, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), Long.MAX_VALUE, Date.valueOf("2000-01-01").getTime());
        assertFragmentRangeEquals(fragments.get(1), Date.valueOf("2000-01-01"), Date.valueOf("2005-01-01"));
        assertFragmentRangeEquals(fragments.get(2), Date.valueOf("2005-01-01"), Date.valueOf("2010-01-01"));
        assertFragmentRangeEquals(fragments.get(3), Date.valueOf("2010-01-01").getTime(), Long.MIN_VALUE);
    }

    // INT

    @Test
    public void testPartionByInt() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("year:int");
        when(context.getOption("RANGE")).thenReturn("2001:2012");
        when(context.getOption("INTERVAL")).thenReturn("2");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(6, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), 2001, 2003);
        assertFragmentRangeEquals(fragments.get(3), 2007, 2009);
        assertFragmentRangeEquals(fragments.get(5), 2011, 2012);
    }

    @Test
    public void testPartitionByIntRangeSwapped() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("year:int");
        when(context.getOption("RANGE")).thenReturn("2013:2012");
        when(context.getOption("INTERVAL")).thenReturn("2");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(0, fragments.size());
    }

    @Test
    public void testPartionByIntIncomplete() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("year:int");
        when(context.getOption("RANGE")).thenReturn("2001:2012");
        when(context.getOption("INTERVAL")).thenReturn("7");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(2, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), 2001, 2008);
        assertFragmentRangeEquals(fragments.get(1), 2008, 2012);
    }

    @Test
    public void testPartionByIntRangeInfiniteLeft() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("year:int");
        when(context.getOption("RANGE")).thenReturn(":2000:2010");
        when(context.getOption("INTERVAL")).thenReturn("5");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(3, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), Long.MAX_VALUE, 2000);
        assertFragmentRangeEquals(fragments.get(1), 2000, 2005);
        assertFragmentRangeEquals(fragments.get(2), 2005, 2010);
    }

    @Test
    public void testPartionByIntRangeInfiniteRight() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("year:int");
        when(context.getOption("RANGE")).thenReturn("2000:2010:");
        when(context.getOption("INTERVAL")).thenReturn("5");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(3, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), 2000, 2005);
        assertFragmentRangeEquals(fragments.get(1), 2005, 2010);
        assertFragmentRangeEquals(fragments.get(2), 2010, Long.MIN_VALUE);
    }

    @Test
    public void testPartionByIntRangeInfiniteBoth() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("year:int");
        when(context.getOption("RANGE")).thenReturn(":2000:2010:");
        when(context.getOption("INTERVAL")).thenReturn("5");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(4, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), Long.MAX_VALUE, 2000);
        assertFragmentRangeEquals(fragments.get(1), 2000, 2005);
        assertFragmentRangeEquals(fragments.get(2), 2005, 2010);
        assertFragmentRangeEquals(fragments.get(3), 2010, Long.MIN_VALUE);
    }

    // ENUM

    @Test
    public void testPartionByEnum() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("level:enum");
        when(context.getOption("RANGE")).thenReturn("excellent:good:general:bad");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(4, fragments.size());
        assertEquals("excellent", new String(fragments.get(0).getMetadata()));
        assertEquals("bad", new String(fragments.get(3).getMetadata()));
    }

    @Test
    public void testPartitionByEnumSingleValue() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("level:enum");
        when(context.getOption("RANGE")).thenReturn("100");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(1, fragments.size());
    }

    // Invalid cases

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionByTypeInvalid() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("level:float");
        when(context.getOption("RANGE")).thenReturn("100:200");

        new JdbcPartitionFragmenter().initialize(context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionByFormatInvalid() throws Exception {

        //PARTITION_BY must be comma-delimited string
        when(context.getOption("PARTITION_BY")).thenReturn("level-enum");
        when(context.getOption("RANGE")).thenReturn("100:200");

        new JdbcPartitionFragmenter().initialize(context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRangeDateFormatInvalid() throws Exception {

        //date string must be yyyy-MM-dd
        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008/01/01:2009-01-01");
        when(context.getOption("INTERVAL")).thenReturn("1:month");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        fragment.getFragments();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIntervalValueInvalid() throws Exception {

        //INTERVAL must be greater than 0
        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(context.getOption("INTERVAL")).thenReturn("-1:month");

        new JdbcPartitionFragmenter().initialize(context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIntervalTypeInvalid() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2011-01-01");
        when(context.getOption("INTERVAL")).thenReturn("6:hour");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        fragment.getFragments();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIntervalTypeMissingInvalid() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2011-01-01");
        when(context.getOption("INTERVAL")).thenReturn("6");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        fragment.getFragments();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRangeMissingEndInvalid() throws Exception {
        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01");
        when(context.getOption("INTERVAL")).thenReturn("1:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        fragment.getFragments();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRangeMissingInvalid() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("year:int");
        when(context.getOption("INTERVAL")).thenReturn("1");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        fragment.getFragments();
    }

    /**
     * Assert fragment metadata and given range match.
     * @param fragment
     * @param rangeStart
     * @param rangeEnd
     */
    private void assertFragmentRangeEquals(Fragment fragment, long rangeStart, long rangeEnd) {
        byte[][] splitMetadata = ByteUtil.splitBytes(fragment.getMetadata());
        long[] actual = {ByteUtil.toLong(splitMetadata[0]), ByteUtil.toLong(splitMetadata[1])};

        assertEquals(rangeStart, actual[0]);
        assertEquals(rangeEnd, actual[1]);
    }

    /**
     * Assert fragment metadata and given range of dates match.
     * @param fragment
     * @param rangeStart
     * @param rangeEnd
     */
    private void assertFragmentRangeEquals(Fragment fragment, Date rangeStart, Date rangeEnd) {
        byte[][] splitMetadata = ByteUtil.splitBytes(fragment.getMetadata());
        long[] actual = {ByteUtil.toLong(splitMetadata[0]), ByteUtil.toLong(splitMetadata[1])};

        assertEquals(rangeStart, new Date(actual[0]));
        assertEquals(rangeEnd, new Date(actual[1]));
    }
}
