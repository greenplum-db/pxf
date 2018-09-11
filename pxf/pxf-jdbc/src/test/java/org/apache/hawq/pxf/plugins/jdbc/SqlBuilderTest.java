package org.apache.hawq.pxf.plugins.jdbc;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.*;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.io.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validate SQL string generated by the {@link JdbcPartitionFragmenter#buildFragmenterSql} method
 * and the {@link WhereSQLBuilder#buildWhereSQL} method.
 */
public class SqlBuilderTest {
    private static final Log LOG = LogFactory.getLog(SqlBuilderTest.class);
    static final String DB_PRODUCT = "mysql";
    static final String ORIGINAL_SQL = "select * from sales";
    InputData inputData;

    @Before
    public void setup() throws Exception {
        LOG.info("SqlBuilderTest.setup()");
    }

    @After
    public void cleanup() throws Exception {
        LOG.info("SqlBuilderTest.cleanup()");
    }

    @Test
    public void testIdFilter() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        // id = 1
        when(inputData.getFilterString()).thenReturn("a0c20s1d1o5");

        WhereSQLBuilder builder = new WhereSQLBuilder(inputData);
        StringBuilder sb = new StringBuilder();
        builder.buildWhereSQL(DB_PRODUCT, sb);
        assertEquals(" WHERE id = 1", sb.toString());
    }

    @Test
    public void testDateAndAmtFilter() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        // cdate > '2008-02-01' and cdate < '2008-12-01' and amt > 1200
        when(inputData.getFilterString()).thenReturn("a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0");

        WhereSQLBuilder builder = new WhereSQLBuilder(inputData);
        StringBuilder sb = new StringBuilder();
        builder.buildWhereSQL(DB_PRODUCT, sb);
        assertEquals(" WHERE cdate > DATE('2008-02-01') AND cdate < DATE('2008-12-01') AND amt > 1200"
                , sb.toString());
    }

    @Test
    public void testUnsupportedOperationFilter() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        // IN 'bad'
        when(inputData.getFilterString()).thenReturn("a3c25s3dbado10");

        WhereSQLBuilder builder = new WhereSQLBuilder(inputData);
        StringBuilder sb = new StringBuilder();
        builder.buildWhereSQL(DB_PRODUCT, sb);
        assertEquals("", sb.toString());
    }

    @Test
    public void testUnsupportedLogicalFilter() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        // cdate > '2008-02-01' or amt < 1200
        when(inputData.getFilterString()).thenReturn("a1c25s10d2008-02-01o2a2c20s4d1200o2l1");

        WhereSQLBuilder builder = new WhereSQLBuilder(inputData);
        StringBuilder sb = new StringBuilder();
        builder.buildWhereSQL(DB_PRODUCT, sb);
        assertEquals("", sb.toString());
    }

    @Test
    public void testDatePartition() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(false);
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("2:month");
        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(6, fragments.size());

        // Partition: cdate >= 2008-01-01 and cdate < 2008-03-01
        when(inputData.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());
        StringBuilder sb = new StringBuilder(ORIGINAL_SQL);
        JdbcPartitionFragmenter.buildFragmenterSql(inputData, DB_PRODUCT, sb);
        assertEquals(ORIGINAL_SQL + " WHERE cdate >= DATE('2008-01-01') AND cdate < DATE('2008-03-01')", sb.toString());
    }

    @Test
    public void testFilterAndPartition() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        when(inputData.getFilterString()).thenReturn("a0c20s1d5o2"); //id>5
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("grade:enum");
        when(inputData.getUserProperty("RANGE")).thenReturn("excellent:good:general:bad");

        StringBuilder sb = new StringBuilder(ORIGINAL_SQL);
        WhereSQLBuilder builder = new WhereSQLBuilder(inputData);
        builder.buildWhereSQL(DB_PRODUCT, sb);
        assertEquals(ORIGINAL_SQL + " WHERE id > 5", sb.toString());

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();

        // Partition: id > 5 and grade = 'excellent'
        when(inputData.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        JdbcPartitionFragmenter.buildFragmenterSql(inputData, DB_PRODUCT, sb);
        assertEquals(ORIGINAL_SQL + " WHERE id > 5 AND grade = 'excellent'", sb.toString());
    }

    @Test
    public void testNoPartition() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(false);
        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(1, fragments.size());

        when(inputData.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        StringBuilder sb = new StringBuilder(ORIGINAL_SQL);
        JdbcPartitionFragmenter.buildFragmenterSql(inputData, DB_PRODUCT, sb);
        assertEquals(ORIGINAL_SQL, sb.toString());
    }


    private void prepareConstruction() throws Exception {
        inputData = mock(InputData.class);
        when(inputData.getDataSource()).thenReturn("sales");


        ArrayList<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 1, "date", null));
        columns.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 2, "float8", null));
        columns.add(new ColumnDescriptor("grade", DataType.TEXT.getOID(), 3, "text", null));
        when(inputData.getTupleDescription()).thenReturn(columns);
        when(inputData.getColumn(0)).thenReturn(columns.get(0));
        when(inputData.getColumn(1)).thenReturn(columns.get(1));
        when(inputData.getColumn(2)).thenReturn(columns.get(2));
        when(inputData.getColumn(3)).thenReturn(columns.get(3));

    }
}
