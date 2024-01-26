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

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SQLQueryBuilderTest {

    private static final String SQL = "SELECT id, cdate, amt, grade, b FROM sales";
    public static final String NAMED_QUERY = "SELECT a, b FROM c";
    public static final String NAMED_QUERY_WHERE = "SELECT a, b FROM c WHERE d = 'foo'";

    private RequestContext context;

    @Mock
    private DatabaseMetaData mockMetaData;

    @BeforeEach
    public void setup() {
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("sales");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 1, "date", null));
        columns.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 2, "float8", null));
        columns.add(new ColumnDescriptor("grade", DataType.TEXT.getOID(), 3, "text", null));
        columns.add(new ColumnDescriptor("b", DataType.BOOLEAN.getOID(), 4, "bool", null));

        context.setTupleDescription(columns);
        context.setUser("test-user");
    }

    @Test
    public void testIdFilter() throws Exception {
        // id = 1
        context.setFilterString("a0c20s1d1o5");

        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE id = 1", query);
    }

    @Test
    public void testDateAndAmtFilter() throws Exception {
        // cdate > '2008-02-01' and cdate < '2008-12-01' and amt > 1200
        context.setFilterString("a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0");
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE ((cdate > DATE('2008-02-01') AND cdate < DATE('2008-12-01')) AND amt > 1200)", query);
    }

    @Test
    public void testDateWithOrAndAmtFilter() throws Exception {
        // cdate > '2008-02-01' OR (cdate < '2008-12-01' AND amt > 1200)
        context.setFilterString("a1c1082s10d2008-02-01o2a1c1082s10d2008-12-01o1a0c23s4d1200o2l0l1");

        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE (cdate > DATE('2008-02-01') OR (cdate < DATE('2008-12-01') AND id > 1200))", query);
    }

    @Test
    public void testDateOrAmtFilter() throws Exception {
        // cdate > '2008-02-01' or amt > 1200
        context.setFilterString("a1c25s10d2008-02-01o2a2c20s4d1200o2l1");
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE (cdate > DATE('2008-02-01') OR amt > 1200)", query);
    }

    @Test
    public void testIsNotNullOperator() throws Exception {
        // a3 IS NOT NULL
        context.setFilterString("a3o9");
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");
        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE grade IS NOT NULL", query);
    }

    @Test
    public void testUnsupportedOperationFilter() throws Exception {
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        // IN 'bad'
        context.setFilterString("a3c25s3dbado10");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE grade IN 'bad'", query);
    }

    @Test
    public void testDatePartition() throws Exception {
        context.addOption("PARTITION_BY", "cdate:date");
        context.addOption("RANGE", "2008-01-01:2008-12-01");
        context.addOption("INTERVAL", "2:month");

        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        Fragmenter fragmenter = getFragmenter(context);
        List<Fragment> fragments = fragmenter.getFragments();
        assertEquals(9, fragments.size());

        SQLQueryBuilder builder;
        String query;

        context.setFragmentMetadata(fragments.get(0).getMetadata());
        builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE cdate < DATE('2008-01-01')", query);

        context.setFragmentMetadata(fragments.get(1).getMetadata());
        builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE cdate >= DATE('2008-12-01')", query);

        context.setFragmentMetadata(fragments.get(2).getMetadata());
        builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE cdate >= DATE('2008-01-01') AND cdate < DATE('2008-03-01')", query);

        context.setFragmentMetadata(fragments.get(3).getMetadata());
        builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE cdate >= DATE('2008-03-01') AND cdate < DATE('2008-05-01')", query);

        context.setFragmentMetadata(fragments.get(8).getMetadata());
        builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE cdate IS NULL", query);
    }

    @Test
    public void testFilterAndPartition() throws Exception {
        // id > 5
        context.setFilterString("a0c20s1d5o2");
        context.addOption("PARTITION_BY", "grade:enum");
        context.addOption("RANGE", "excellent:good:general:bad");

        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        Fragmenter fragmenter = getFragmenter(context);
        List<Fragment> fragments = fragmenter.getFragments();
        // Fragment 0: grade = 'excellent'
        context.setFragmentMetadata(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE id > 5 AND grade = 'excellent'", query);

        context.setFragmentMetadata(fragments.get(4).getMetadata());

        builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE id > 5 AND ( grade <> 'excellent' AND grade <> 'good' AND grade <> 'general' AND grade <> 'bad' )", query);

        context.setFragmentMetadata(fragments.get(5).getMetadata());

        builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE id > 5 AND grade IS NULL", query);
    }

    @Test
    public void testFilterAndPartitionWithOrPredicate() throws Exception {
        // cdate > '2008-02-01' or amt > 1200
        context.setFilterString("a1c25s10d2008-02-01o2a2c20s4d1200o2l1");
        context.addOption("PARTITION_BY", "grade:enum");
        context.addOption("RANGE", "excellent:good:general:bad");

        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        Fragmenter fragmenter = getFragmenter(context);
        List<Fragment> fragments = fragmenter.getFragments();
        // Fragment 0: grade = 'excellent'
        context.setFragmentMetadata(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE (cdate > DATE('2008-02-01') OR amt > 1200) AND grade = 'excellent'", query);

        context.setFragmentMetadata(fragments.get(4).getMetadata());

        builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE (cdate > DATE('2008-02-01') OR amt > 1200) AND ( grade <> 'excellent' AND grade <> 'good' AND grade <> 'general' AND grade <> 'bad' )", query);

        context.setFragmentMetadata(fragments.get(5).getMetadata());

        builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        query = builder.buildSelectQuery();
        assertEquals(SQL + " WHERE (cdate > DATE('2008-02-01') OR amt > 1200) AND grade IS NULL", query);
    }

    @Test
    public void testNoPartition() throws Exception {
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        Fragmenter fragmenter = getFragmenter(context);
        List<Fragment> fragments = fragmenter.getFragments();
        assertEquals(1, fragments.size());
        context.setFragmentMetadata(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(SQL, query);
    }

    @Test
    public void testIdMixedCase() throws Exception {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cDate", DataType.DATE.getOID(), 1, "date", null));
        context.setTupleDescription(columns);

        DatabaseMetaData localDatabaseMetaData = mock(DatabaseMetaData.class);
        when(localDatabaseMetaData.supportsMixedCaseIdentifiers()).thenReturn(false);
        when(localDatabaseMetaData.getExtraNameCharacters()).thenReturn("");
        when(localDatabaseMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(localDatabaseMetaData.getIdentifierQuoteString()).thenReturn("\"");

        String localSQL = "SELECT \"id\", \"cDate\" FROM sales";

        SQLQueryBuilder builder = new SQLQueryBuilder(context, localDatabaseMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(localSQL, query);
    }

    @Test
    public void testIdMixedCaseWithFilter() throws Exception {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cDate", DataType.DATE.getOID(), 1, "date", null));
        context.setTupleDescription(columns);

        DatabaseMetaData localDatabaseMetaData = mock(DatabaseMetaData.class);
        when(localDatabaseMetaData.supportsMixedCaseIdentifiers()).thenReturn(false);
        when(localDatabaseMetaData.getExtraNameCharacters()).thenReturn("");
        when(localDatabaseMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(localDatabaseMetaData.getIdentifierQuoteString()).thenReturn("\"");

        // id > 5
        context.setFilterString("a0c20s1d5o2");

        String localSQL = "SELECT \"id\", \"cDate\" FROM sales WHERE \"id\" > 5";

        SQLQueryBuilder builder = new SQLQueryBuilder(context, localDatabaseMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(localSQL, query);
    }

    @Test
    public void testIdMixedCaseWithFilterAndPartition() throws Exception {
        context.setDataSource("sales");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cDate", DataType.DATE.getOID(), 1, "date", null));
        context.setTupleDescription(columns);

        DatabaseMetaData localDatabaseMetaData = mock(DatabaseMetaData.class);
        when(localDatabaseMetaData.supportsMixedCaseIdentifiers()).thenReturn(false);
        when(localDatabaseMetaData.getExtraNameCharacters()).thenReturn("");
        when(localDatabaseMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(localDatabaseMetaData.getIdentifierQuoteString()).thenReturn("\"");

        // id > 5
        context.setFilterString("a0c20s1d5o2");

        context.addOption("PARTITION_BY", "cDate:date");
        context.addOption("RANGE", "2008-01-01:2009-01-01");
        context.addOption("INTERVAL", "2:month");
        Fragmenter fragmenter = getFragmenter(context);
        List<Fragment> fragments = fragmenter.getFragments();
        assertEquals(9, fragments.size());
        // Partition: cdate >= 2008-01-01 and cdate < 2008-03-01
        context.setFragmentMetadata(fragments.get(2).getMetadata());

        String expected = "SELECT \"id\", \"cDate\" FROM sales WHERE \"id\" > 5 AND \"cDate\" >= DATE('2008-01-01') AND \"cDate\" < DATE('2008-03-01')";

        SQLQueryBuilder builder = new SQLQueryBuilder(context, localDatabaseMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(expected, query);
    }

    @Test
    public void testIdSpecialCharacters() throws Exception {
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("c date", DataType.DATE.getOID(), 1, "date", null));
        context.setTupleDescription(columns);

        DatabaseMetaData localDatabaseMetaData = mock(DatabaseMetaData.class);
        when(localDatabaseMetaData.getExtraNameCharacters()).thenReturn("");
        when(localDatabaseMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(localDatabaseMetaData.getIdentifierQuoteString()).thenReturn("\"");

        String localSQL = "SELECT \"id\", \"c date\" FROM sales";

        SQLQueryBuilder builder = new SQLQueryBuilder(context, localDatabaseMetaData);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals(localSQL, query);
    }

    @Test
    public void testIdFilterForceQuote() throws Exception {
        // id = 1
        context.setFilterString("a0c20s1d1o5");
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getIdentifierQuoteString()).thenReturn("\"");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.forceSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals("SELECT \"id\", \"cdate\", \"amt\", \"grade\", \"b\" FROM sales WHERE \"id\" = 1", query);
    }

    @Test
    public void testColumnProjection() throws Exception {
        // id = 1
        context.setFilterString("a0c20s1d1o5");
        context.getTupleDescription().get(1).setProjected(false);
        context.getTupleDescription().get(3).setProjected(false);
        context.getTupleDescription().get(4).setProjected(false);
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        String query = builder.buildSelectQuery();
        assertEquals("SELECT id, amt FROM sales WHERE id = 1", query);
    }

    /* -------------- NAMED QUERY TESTS --------------- */
    @Test
    public void testSimpleNamedQuery() throws Exception {
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData, NAMED_QUERY_WHERE);
        String query = builder.buildSelectQuery();
        assertEquals("SELECT id, cdate, amt, grade, b FROM (SELECT a, b FROM c WHERE d = 'foo') pxfsubquery", query);
    }

    @Test
    public void testNamedQueryIdFilterForceQuote() throws Exception {
        // id = 1
        context.setFilterString("a0c20s1d1o5");
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getIdentifierQuoteString()).thenReturn("\"");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData, NAMED_QUERY_WHERE);
        builder.forceSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals("SELECT \"id\", \"cdate\", \"amt\", \"grade\", \"b\" FROM (SELECT a, b FROM c WHERE d = 'foo') pxfsubquery WHERE \"id\" = 1", query);
    }

    @Test
    public void testNamedQueryColumnProjection() throws Exception {
        // id = 1
        context.setFilterString("a0c20s1d1o5");
        context.getTupleDescription().get(1).setProjected(false);
        context.getTupleDescription().get(3).setProjected(false);
        context.getTupleDescription().get(4).setProjected(false);
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData, NAMED_QUERY_WHERE);
        String query = builder.buildSelectQuery();
        assertEquals("SELECT id, amt FROM (SELECT a, b FROM c WHERE d = 'foo') pxfsubquery WHERE id = 1", query);
    }

    @Test
    public void testNamedQueryWithWhereWithFilterWithPartition() throws Exception {
        // id > 5
        context.setFilterString("a0c20s1d5o2");
        context.addOption("PARTITION_BY", "grade:enum");
        context.addOption("RANGE", "excellent:good:general:bad");
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        Fragmenter fragmenter = getFragmenter(context);
        List<Fragment> fragments = fragmenter.getFragments();
        // Fragment 0: grade = 'excellent'
        context.setFragmentMetadata(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData, NAMED_QUERY_WHERE);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals("SELECT id, cdate, amt, grade, b FROM (SELECT a, b FROM c WHERE d = 'foo') pxfsubquery WHERE id > 5 AND grade = 'excellent'", query);
    }

    @Test
    public void testNamedQueryWithWhereWithoutFilterWithPartition() throws Exception {
        context.addOption("PARTITION_BY", "grade:enum");
        context.addOption("RANGE", "excellent:good:general:bad");
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        Fragmenter fragmenter = getFragmenter(context);
        List<Fragment> fragments = fragmenter.getFragments();
        // Fragment 0: grade = 'excellent'
        context.setFragmentMetadata(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData, NAMED_QUERY_WHERE);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals("SELECT id, cdate, amt, grade, b FROM (SELECT a, b FROM c WHERE d = 'foo') pxfsubquery WHERE grade = 'excellent'", query);
    }

    @Test
    public void testNamedQueryWithoutWhereWithFilterWithPartition() throws Exception {
        // id > 5
        context.setFilterString("a0c20s1d5o2");
        context.addOption("PARTITION_BY", "grade:enum");
        context.addOption("RANGE", "excellent:good:general:bad");
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        Fragmenter fragmenter = getFragmenter(context);
        List<Fragment> fragments = fragmenter.getFragments();
        // Fragment 0: grade = 'excellent'
        context.setFragmentMetadata(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData, NAMED_QUERY);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals("SELECT id, cdate, amt, grade, b FROM (SELECT a, b FROM c) pxfsubquery WHERE id > 5 AND grade = 'excellent'", query);
    }

    @Test
    public void testNamedQueryWithoutWhereWithoutFilterWithPartition() throws Exception {
        context.addOption("PARTITION_BY", "grade:enum");
        context.addOption("RANGE", "excellent:good:general:bad");
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        Fragmenter fragmenter = getFragmenter(context);
        List<Fragment> fragments = fragmenter.getFragments();
        // Fragment 0: grade = 'excellent'
        context.setFragmentMetadata(fragments.get(0).getMetadata());

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData, NAMED_QUERY);
        builder.autoSetQuoteString();
        String query = builder.buildSelectQuery();
        assertEquals("SELECT id, cdate, amt, grade, b FROM (SELECT a, b FROM c) pxfsubquery WHERE grade = 'excellent'", query);
    }

    @Test
    public void testNotBoolean() throws Exception {
        when(mockMetaData.getDatabaseProductName()).thenReturn("mysql");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        // NOT a4
        context.setFilterString("a4c16s4dtrueo0l2");

        SQLQueryBuilder builder = new SQLQueryBuilder(context, mockMetaData);
        builder.autoSetQuoteString();

        assertEquals("SELECT id, cdate, amt, grade, b FROM sales WHERE NOT (b)", builder.buildSelectQuery());
    }

    private Fragmenter getFragmenter(RequestContext context) {
        JdbcPartitionFragmenter fragmenter = new JdbcPartitionFragmenter();
        fragmenter.setRequestContext(context);
        fragmenter.afterPropertiesSet();
        return fragmenter;
    }

}
