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
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.io.DataType;

import java.util.List;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SQLQueryBuilderTest {
    @Before
    public void setup() throws Exception {
        context = mock(RequestContext.class);
        when(context.getDataSource()).thenReturn("sales");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 1, "date", null));
        columns.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 2, "float8", null));
        columns.add(new ColumnDescriptor("grade", DataType.TEXT.getOID(), 3, "text", null));
        when(context.getTupleDescription()).thenReturn(columns);
        when(context.getColumn(0)).thenReturn(columns.get(0));
        when(context.getColumn(1)).thenReturn(columns.get(1));
        when(context.getColumn(2)).thenReturn(columns.get(2));
        when(context.getColumn(3)).thenReturn(columns.get(3));

        databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(databaseMetaData.getExtraNameCharacters()).thenReturn("");
        when(databaseMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(databaseMetaData.getIdentifierQuoteString()).thenReturn("\"");
    }

    @Test
    public void testIdFilter() throws Exception {
        when(context.hasFilter()).thenReturn(true);
        // id = 1
        when(context.getFilterString()).thenReturn("a0c20s1d1o5");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, databaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE id = 1", query);
    }

    @Test
    public void testDateAndAmtFilter() throws Exception {
        when(context.hasFilter()).thenReturn(true);
        // cdate > '2008-02-01' and cdate < '2008-12-01' and amt > 1200
        when(context.getFilterString()).thenReturn("a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, databaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE cdate > DATE('2008-02-01') AND cdate < DATE('2008-12-01') AND amt > 1200", query);
    }

    @Test
    public void testUnsupportedOperationFilter() throws Exception {
        when(context.hasFilter()).thenReturn(true);
        // IN 'bad'
        when(context.getFilterString()).thenReturn("a3c25s3dbado10");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, databaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(SQL, query);
    }

    @Test
    public void testUnsupportedLogicalFilter() throws Exception {
        when(context.hasFilter()).thenReturn(true);
        // cdate > '2008-02-01' or amt < 1200
        when(context.getFilterString()).thenReturn("a1c25s10d2008-02-01o2a2c20s4d1200o2l1");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, databaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(SQL, query);
    }

    @Test
    public void testDatePartition() throws Exception {
        when(context.hasFilter()).thenReturn(false);

        when(context.getOption("PARTITION_BY")).thenReturn("cdate:date");
        when(context.getOption("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(context.getOption("INTERVAL")).thenReturn("2:month");
        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(6, fragments.size());
        // Partition: cdate >= 2008-01-01 and cdate < 2008-03-01
        when(context.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, databaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE cdate >= DATE('2008-01-01') AND cdate < DATE('2008-03-01')", query);
    }

    @Test
    public void testFilterAndPartition() throws Exception {
        when(context.hasFilter()).thenReturn(true);
        // id > 5
        when(context.getFilterString()).thenReturn("a0c20s1d5o2");

        when(context.getOption("PARTITION_BY")).thenReturn("grade:enum");
        when(context.getOption("RANGE")).thenReturn("excellent:good:general:bad");
        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();
        // Fragment 0: grade = 'excellent'
        when(context.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, databaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE id > 5 AND grade = 'excellent'", query);
    }

    @Test
    public void testNoPartition() throws Exception {
        when(context.hasFilter()).thenReturn(false);

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(1, fragments.size());
        when(context.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, databaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(SQL, query);
    }

    @Test
    public void testIdMixedCase() throws Exception {
        RequestContext localContext = mock(RequestContext.class);
        when(localContext.getDataSource()).thenReturn("sales");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cDate", DataType.DATE.getOID(), 1, "date", null));
        when(localContext.getTupleDescription()).thenReturn(columns);
        when(localContext.getColumn(0)).thenReturn(columns.get(0));
        when(localContext.getColumn(1)).thenReturn(columns.get(1));

        DatabaseMetaData localDatabaseMetaData = mock(DatabaseMetaData.class);
        when(localDatabaseMetaData.supportsMixedCaseIdentifiers()).thenReturn(false);
        when(localDatabaseMetaData.getExtraNameCharacters()).thenReturn("");
        when(localDatabaseMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(localDatabaseMetaData.getIdentifierQuoteString()).thenReturn("\"");

        when(localContext.hasFilter()).thenReturn(false);

        String localSQL = "SELECT \"id\", \"cDate\" FROM sales";

        SQLQueryBuilder builder = new SQLQueryBuilder(localContext, localDatabaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(localSQL, query);
    }

    @Test
    public void testIdMixedCaseWithFilter() throws Exception {
        RequestContext localContext = mock(RequestContext.class);
        when(localContext.getDataSource()).thenReturn("sales");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cDate", DataType.DATE.getOID(), 1, "date", null));
        when(localContext.getTupleDescription()).thenReturn(columns);
        when(localContext.getColumn(0)).thenReturn(columns.get(0));
        when(localContext.getColumn(1)).thenReturn(columns.get(1));

        DatabaseMetaData localDatabaseMetaData = mock(DatabaseMetaData.class);
        when(localDatabaseMetaData.supportsMixedCaseIdentifiers()).thenReturn(false);
        when(localDatabaseMetaData.getExtraNameCharacters()).thenReturn("");
        when(localDatabaseMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(localDatabaseMetaData.getIdentifierQuoteString()).thenReturn("\"");

        when(localContext.hasFilter()).thenReturn(true);
        // id > 5
        when(localContext.getFilterString()).thenReturn("a0c20s1d5o2");

        String localSQL = "SELECT \"id\", \"cDate\" FROM sales WHERE \"id\" > 5";

        SQLQueryBuilder builder = new SQLQueryBuilder(localContext, localDatabaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(localSQL, query);
    }

    @Test
    public void testIdMixedCaseWithFilterAndPartition() throws Exception {
        RequestContext localContext = mock(RequestContext.class);
        when(localContext.getDataSource()).thenReturn("sales");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cDate", DataType.DATE.getOID(), 1, "date", null));
        when(localContext.getTupleDescription()).thenReturn(columns);
        when(localContext.getColumn(0)).thenReturn(columns.get(0));
        when(localContext.getColumn(1)).thenReturn(columns.get(1));

        DatabaseMetaData localDatabaseMetaData = mock(DatabaseMetaData.class);
        when(localDatabaseMetaData.supportsMixedCaseIdentifiers()).thenReturn(false);
        when(localDatabaseMetaData.getExtraNameCharacters()).thenReturn("");
        when(localDatabaseMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(localDatabaseMetaData.getIdentifierQuoteString()).thenReturn("\"");

        when(localContext.hasFilter()).thenReturn(true);
        // id > 5
        when(localContext.getFilterString()).thenReturn("a0c20s1d5o2");

        when(localContext.getOption("PARTITION_BY")).thenReturn("cDate:date");
        when(localContext.getOption("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(localContext.getOption("INTERVAL")).thenReturn("2:month");
        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(localContext);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(6, fragments.size());
        // Partition: cdate >= 2008-01-01 and cdate < 2008-03-01
        when(localContext.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        String localSQL = "SELECT \"id\", \"cDate\" FROM sales WHERE \"id\" > 5 AND \"cDate\" >= DATE('2008-01-01') AND \"cDate\" < DATE('2008-03-01')";

        SQLQueryBuilder builder = new SQLQueryBuilder(localContext, localDatabaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(localSQL, query);
    }

    @Test
    public void testIdSpecialCharacters() throws Exception {
        RequestContext localContext = mock(RequestContext.class);
        when(localContext.getDataSource()).thenReturn("sales");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("c date", DataType.DATE.getOID(), 1, "date", null));
        when(localContext.getTupleDescription()).thenReturn(columns);
        when(localContext.getColumn(0)).thenReturn(columns.get(0));
        when(localContext.getColumn(1)).thenReturn(columns.get(1));

        DatabaseMetaData localDatabaseMetaData = mock(DatabaseMetaData.class);
        when(localDatabaseMetaData.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(localDatabaseMetaData.getExtraNameCharacters()).thenReturn("");
        when(localDatabaseMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(localDatabaseMetaData.getIdentifierQuoteString()).thenReturn("\"");

        when(localContext.hasFilter()).thenReturn(false);

        String localSQL = "SELECT \"id\", \"c date\" FROM sales";

        SQLQueryBuilder builder = new SQLQueryBuilder(localContext, localDatabaseMetaData);
        String query = builder.buildSelectQuery();
        assertEquals(localSQL, query);
    }

    private RequestContext context;
    private DatabaseMetaData databaseMetaData;

    static final String SQL = "SELECT id, cdate, amt, grade FROM sales";
}
