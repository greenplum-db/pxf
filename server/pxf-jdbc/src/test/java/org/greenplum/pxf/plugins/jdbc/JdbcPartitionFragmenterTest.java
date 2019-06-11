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

import org.apache.commons.lang.SerializationUtils;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.jdbc.partitioning.DatePartition;
import org.greenplum.pxf.plugins.jdbc.partitioning.EnumPartition;
import org.greenplum.pxf.plugins.jdbc.partitioning.IntPartition;
import org.greenplum.pxf.plugins.jdbc.partitioning.JdbcFragmentMetadata;
import org.greenplum.pxf.plugins.jdbc.partitioning.NullPartition;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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

        assertEquals(13, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), null, Date.valueOf("2008-01-01"));
        assertFragmentRangeEquals(fragments.get(1), Date.valueOf("2008-01-01"), Date.valueOf("2008-01-02"));
        assertFragmentRangeEquals(fragments.get(5), Date.valueOf("2008-01-05"), Date.valueOf("2008-01-06"));
        assertFragmentRangeEquals(fragments.get(10), Date.valueOf("2008-01-10"), Date.valueOf("2008-01-11"));
        assertFragmentRangeEquals(fragments.get(11), Date.valueOf("2008-01-11"), null);
        assertFragmentNullEquals(fragments.get(12), true);
    }

    @Test
    public void testPartionByDateIntervalMonth() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2008-12-31");
        when(context.getOption("INTERVAL")).thenReturn("1:month");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(15, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), null, Date.valueOf("2008-01-01"));
        assertFragmentRangeEquals(fragments.get(1), Date.valueOf("2008-01-01"), Date.valueOf("2008-02-01"));
        assertFragmentRangeEquals(fragments.get(6), Date.valueOf("2008-06-01"), Date.valueOf("2008-07-01"));
        assertFragmentRangeEquals(fragments.get(12), Date.valueOf("2008-12-01"), Date.valueOf("2008-12-31"));
        assertFragmentRangeEquals(fragments.get(13), Date.valueOf("2008-12-31"), null);
        assertFragmentNullEquals(fragments.get(14), true);
    }

    @Test
    public void testPartionByDateIntervalYear() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-02-03:2018-02-02");
        when(context.getOption("INTERVAL")).thenReturn("1:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(13, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), null, Date.valueOf("2008-02-03"));
        assertFragmentRangeEquals(fragments.get(1), Date.valueOf("2008-02-03"), Date.valueOf("2009-02-03"));
        assertFragmentRangeEquals(fragments.get(6), Date.valueOf("2013-02-03"), Date.valueOf("2014-02-03"));
        assertFragmentRangeEquals(fragments.get(10), Date.valueOf("2017-02-03"), Date.valueOf("2018-02-02"));
        assertFragmentRangeEquals(fragments.get(11), Date.valueOf("2018-02-02"), null);
        assertFragmentNullEquals(fragments.get(12), true);
    }

    @Test
    public void testPartionByDateIntervalYearIncomplete() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-02-03:2010-01-15");
        when(context.getOption("INTERVAL")).thenReturn("1:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(5, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), null, Date.valueOf("2008-02-03"));
        assertFragmentRangeEquals(fragments.get(1), Date.valueOf("2008-02-03"), Date.valueOf("2009-02-03"));
        assertFragmentRangeEquals(fragments.get(2), Date.valueOf("2009-02-03"), Date.valueOf("2010-01-15"));
        assertFragmentRangeEquals(fragments.get(3), Date.valueOf("2010-01-15"), null);
        assertFragmentNullEquals(fragments.get(4), true);
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

        assertEquals(9, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), null, 2001L);
        assertFragmentRangeEquals(fragments.get(1), 2001L, 2002L);
        assertFragmentRangeEquals(fragments.get(3), 2005L, 2006L);
        assertFragmentRangeEquals(fragments.get(6), 2011L, 2012L);
        assertFragmentRangeEquals(fragments.get(7), 2012L, null);
        assertFragmentNullEquals(fragments.get(8), true);
    }

    @Test
    public void testPartionByIntIncomplete() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("year:int");
        when(context.getOption("RANGE")).thenReturn("2001:2012");
        when(context.getOption("INTERVAL")).thenReturn("7");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(5, fragments.size());
        assertFragmentRangeEquals(fragments.get(0), null, 2001L);
        assertFragmentRangeEquals(fragments.get(1), 2001L, 2007L);
        assertFragmentRangeEquals(fragments.get(2), 2008L, 2012L);
        assertFragmentRangeEquals(fragments.get(3), 2012L, null);
        assertFragmentNullEquals(fragments.get(4), true);
    }

    // ENUM

    @Test
    public void testPartionByEnum() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("level:enum");
        when(context.getOption("RANGE")).thenReturn("excellent:good:general:bad");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(6, fragments.size());
        assertFragmentEnumEquals(fragments.get(0), "excellent");
        assertFragmentEnumEquals(fragments.get(1), "good");
        assertFragmentEnumEquals(fragments.get(3), "bad");
        assertFragmentEnumEquals(fragments.get(4), new String[]{"excellent", "good", "general", "bad"});
        assertFragmentNullEquals(fragments.get(5), true);
    }

    @Test
    public void testPartitionByEnumSingleValue() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("level:enum");
        when(context.getOption("RANGE")).thenReturn("100");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(3, fragments.size());
        assertFragmentEnumEquals(fragments.get(0), "100");
        assertFragmentEnumEquals(fragments.get(1), new String[]{"100"});
        assertFragmentNullEquals(fragments.get(2), true);
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

    @Test(expected = IllegalArgumentException.class)
    public void testRangeDateSwappedInvalid() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2001-01-01");
        when(context.getOption("INTERVAL")).thenReturn("1:month");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        fragment.getFragments();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRangeIntSwappedInvalid() throws Exception {

        when(context.getOption("PARTITION_BY")).thenReturn("year:int");
        when(context.getOption("RANGE")).thenReturn("2013:2012");
        when(context.getOption("INTERVAL")).thenReturn("2");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        fragment.getFragments();
    }

    /**
     * Assert fragment metadata and given range match.
     * @param fragment
     * @param rangeStart (null is allowed)
     * @param rangeEnd (null is allowed)
     */
    private void assertFragmentRangeEquals(Fragment fragment, Long rangeStart, Long rangeEnd) {
        JdbcFragmentMetadata fragmentMetadata = JdbcFragmentMetadata.class.cast(SerializationUtils.deserialize(fragment.getMetadata()));
        IntPartition intPartition = IntPartition.class.cast(fragmentMetadata);

        Long[] boundaries = intPartition.getBoundaries();
        assertEquals(rangeStart, boundaries[0]);
        assertEquals(rangeEnd, boundaries[1]);
    }

    /**
     * Assert fragment metadata and given range of dates match.
     * @param fragment
     * @param rangeStart (null is allowed)
     * @param rangeEnd (null is allowed)
     */
    private void assertFragmentRangeEquals(Fragment fragment, Date rangeStart, Date rangeEnd) {
        JdbcFragmentMetadata fragmentMetadata = JdbcFragmentMetadata.class.cast(SerializationUtils.deserialize(fragment.getMetadata()));
        DatePartition datePartition = DatePartition.class.cast(fragmentMetadata);

        Date[] boundaries = datePartition.getBoundaries();
        assertEquals(rangeStart, boundaries[0]);
        assertEquals(rangeEnd, boundaries[1]);
    }

    /**
     * Assert fragment metadata and given enum (string) match
     * @param fragment
     * @param enumValue
     */
    private void assertFragmentEnumEquals(Fragment fragment, String enumValue) {
        JdbcFragmentMetadata fragmentMetadata = JdbcFragmentMetadata.class.cast(SerializationUtils.deserialize(fragment.getMetadata()));
        EnumPartition enumPartition = EnumPartition.class.cast(fragmentMetadata);

        assertEquals(enumValue, enumPartition.getValue());
        assertNull(enumPartition.getExcluded());
    }

    private void assertFragmentEnumEquals(Fragment fragment, String[] excludedValues) {
        JdbcFragmentMetadata fragmentMetadata = JdbcFragmentMetadata.class.cast(SerializationUtils.deserialize(fragment.getMetadata()));
        EnumPartition enumPartition = EnumPartition.class.cast(fragmentMetadata);

        assertNull(enumPartition.getValue());
        assertArrayEquals(excludedValues, enumPartition.getExcluded());
    }

    /**
     * Assert fragment metadata is a partition of type NULL and its NULL constraint type matches the given one
     * @param fragment
     * @param isNull
     */
    private void assertFragmentNullEquals(Fragment fragment, boolean isNull) {
        JdbcFragmentMetadata fragmentMetadata = JdbcFragmentMetadata.class.cast(SerializationUtils.deserialize(fragment.getMetadata()));
        NullPartition nullPartition = NullPartition.class.cast(fragmentMetadata);

        assertEquals(isNull, nullPartition.isNull());
    }
}
