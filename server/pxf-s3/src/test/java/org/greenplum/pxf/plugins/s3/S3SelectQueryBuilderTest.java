package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class S3SelectQueryBuilderTest {

    private static final String SQL_POSITION = "SELECT s._1, s._2, s._3, s._4, s._5, s._6 FROM S3Object s";
    private static final String SQL_NO_POSITION = "SELECT s.\"id\", s.\"cdate\", s.\"amt\", s.\"grade\", s.\"pass\", s.\"weight\" FROM S3Object s";

    private RequestContext context;
    private S3SelectQueryBuilder builderPosition, builderNoPosition;

    @Before
    public void setup() {
        context = new RequestContext();
        context.setDataSource("sales");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 1, "date", null));
        columns.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 2, "float8", null));
        columns.add(new ColumnDescriptor("grade", DataType.TEXT.getOID(), 3, "text", null));
        columns.add(new ColumnDescriptor("pass", DataType.BOOLEAN.getOID(), 4, "boolean", null));
        columns.add(new ColumnDescriptor("weight", DataType.REAL.getOID(), 5, "float4", null));
        context.setTupleDescription(columns);

        builderPosition = new S3SelectQueryBuilder(context, true);
        builderNoPosition = new S3SelectQueryBuilder(context, false);
    }

    @Test
    public void testNoFilterNoProjection() {
        assertEquals(SQL_POSITION, builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION, builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testIdFilter() {
        context.setFilterString("a0c20s1d1o5"); // id = 1
        assertEquals(SQL_POSITION + " WHERE CAST (s._1 AS int) = 1", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE CAST (s.\"id\" AS int) = 1", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testDateAndAmtFilter() {
        // cdate > '2008-02-01' and cdate < '2008-12-01' and amt > 1200
        context.setFilterString("a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0");
        assertEquals(SQL_POSITION + " WHERE ((TO_TIMESTAMP(s._2) > TO_TIMESTAMP('2008-02-01') AND TO_TIMESTAMP(s._2) < TO_TIMESTAMP('2008-12-01')) AND CAST (s._3 AS float) > 1200)", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE ((TO_TIMESTAMP(s.\"cdate\") > TO_TIMESTAMP('2008-02-01') AND TO_TIMESTAMP(s.\"cdate\") < TO_TIMESTAMP('2008-12-01')) AND CAST (s.\"amt\" AS float) > 1200)", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testInOperationFilter() {
        // a0 IN (194 , 82756)
        context.setFilterString("a0m1016s3d194s5d82756o10");
        assertEquals(SQL_POSITION + " WHERE CAST (s._1 AS int) IN (194,82756)", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE CAST (s.\"id\" AS int) IN (194,82756)", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testInOperationFilterWithText() {
        // a3 IN ('A' , 'B')
        context.setFilterString("a3m1009s1dAs1dBo10");
        assertEquals(SQL_POSITION + " WHERE s._4 IN ('A','B')", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE s.\"grade\" IN ('A','B')", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testInOperationFilterWithTextWithSingleQuote() {
        // a3 IN ('A' , 'B')
        context.setFilterString("a3m1009s5dA'B'Cs1dBo10");
        assertEquals(SQL_POSITION + " WHERE s._4 IN ('A''B''C','B')", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE s.\"grade\" IN ('A''B''C','B')", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testOrFilter() {
        // cdate > '2008-02-01' or amt > 1200
        context.setFilterString("a1c25s10d2008-02-01o2a2c20s4d1200o2l1");
        assertEquals(SQL_POSITION + " WHERE (TO_TIMESTAMP(s._2) > TO_TIMESTAMP('2008-02-01') OR CAST (s._3 AS float) > 1200)", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE (TO_TIMESTAMP(s.\"cdate\") > TO_TIMESTAMP('2008-02-01') OR CAST (s.\"amt\" AS float) > 1200)", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testIsNotNullOperator() {
        // a3 IS NOT NULL
        context.setFilterString("a3o9");
        assertEquals(SQL_POSITION + " WHERE s._4 IS NOT NULL", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE s.\"grade\" IS NOT NULL", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testIsNullOperator() {
        // a3 IS NULL
        context.setFilterString("a3o8");
        assertEquals(SQL_POSITION + " WHERE s._4 IS NULL", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE s.\"grade\" IS NULL", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testBoolean() {
        // a4
        context.setFilterString("a4c16s4dtrueo0");
        assertEquals(SQL_POSITION + " WHERE CAST (s._5 AS bool)", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE CAST (s.\"pass\" AS bool)", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testNotBoolean() {
        // NOT a4
        context.setFilterString("a4c16s4dtrueo0l2");
        assertEquals(SQL_POSITION + " WHERE NOT (CAST (s._5 AS bool))", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE NOT (CAST (s.\"pass\" AS bool))", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testReal() {
        // a5 = 2.0
        context.setFilterString("a5c701s1d2o5");
        assertEquals(SQL_POSITION + " WHERE CAST (s._6 AS decimal) = 2", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE CAST (s.\"weight\" AS decimal) = 2", builderNoPosition.buildSelectQuery());
    }

    @Test
    public void testNestedExpressionWithLogicalOperation() {
        // (a3 = 'foobar' AND a2 <> 999) AND (a1 = '2008-02-01' OR a5 <> 999)
        context.setFilterString("a3c25s6dfoobaro5a2c23s3d999o6l0a1c25s10d2008-02-01o5a5c20s3d999o6l1l0");
        assertEquals(SQL_POSITION + " WHERE ((s._4 = 'foobar' AND CAST (s._3 AS float) <> 999) AND (TO_TIMESTAMP(s._2) = TO_TIMESTAMP('2008-02-01') OR CAST (s._6 AS decimal) <> 999))", builderPosition.buildSelectQuery());
        assertEquals(SQL_NO_POSITION + " WHERE ((s.\"grade\" = 'foobar' AND CAST (s.\"amt\" AS float) <> 999) AND (TO_TIMESTAMP(s.\"cdate\") = TO_TIMESTAMP('2008-02-01') OR CAST (s.\"weight\" AS decimal) <> 999))", builderNoPosition.buildSelectQuery());
    }
}
