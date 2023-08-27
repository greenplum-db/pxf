package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ORCFilterPushDownTest extends ORCVectorizedBaseTest {

    private Accessor accessor;
    private ORCVectorizedResolver resolver;
    private RequestContext context;

    @BeforeEach
    public void setup() {
        super.setup();

        accessor = new ORCVectorizedAccessor();
        resolver = new ORCVectorizedResolver();
        context = new RequestContext();

        String path = Objects.requireNonNull(getClass().getClassLoader().getResource("orc/orc_types.orc")).getPath();

        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setUser("test-user");
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.setDataSource(path);
        context.setFragmentMetadata(new HcfsFragmentMetadata(0, 2257));
        context.setTupleDescription(columnDescriptors);
        context.setConfiguration(new Configuration());

        accessor.setRequestContext(context);
        resolver.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.afterPropertiesSet();
    }

    @Test
    public void testNoFilter() throws Exception {
        // all rows are expected
        assertRowsReturned(ALL_ROWS);
    }

    //Find out the filter string in FilterParser.java
    @Test
    public void testTextPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // t2 = 's_6'
        context.setFilterString("a1c25s3ds_6o5");
        assertRowsReturned(ALL_ROWS);

        // t2 = 'foo'
        context.setFilterString("a1c25s3dfooo5");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testIntegerPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // num1 = 1
        context.setFilterString("a2c23s1d1o5");
        assertRowsReturned(ALL_ROWS);

        // 25 is not a value for num1, so no rows should be returned
        // num1 = 25
        context.setFilterString("a2c23s2d25o5");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testFloat8PushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // dub1 = 11
        context.setFilterString("a3c701s2d11o5");
        assertRowsReturned(ALL_ROWS);

        // dub1 = -25
        context.setFilterString("a3c701s3d-25o5");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testTimestampPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // tm = '2013-07-15 21:00:05'
        context.setFilterString("a5c1114s19d2013-07-15 21:00:05o5");
        assertRowsReturned(ALL_ROWS);

        // tm = '2020-07-15 21:00:05'
        context.setFilterString("a5c1114s19d2020-07-15 21:00:05o5");
        assertRowsReturned(NO_ROWS);
    }

    //TODO: add testTimestampWithTimeZonePushDown()

    @Test
    public void testRealPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // r > 7 and r < 8
        context.setFilterString("a7c701s1d7o2a7c700s1d8o1l0");
        assertRowsReturned(ALL_ROWS);

        // r >= 7 and r <= 8
        context.setFilterString("a7c701s1d7o4a7c700s1d8o3l0");
        assertRowsReturned(ALL_ROWS);

        // r = 15
        context.setFilterString("a7c700s2d15o5");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testBigIntPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // bg = 23456789
        context.setFilterString("a8c23s8d23456789o5");
        assertRowsReturned(ALL_ROWS);

        // bg <> 23456789
        context.setFilterString("a8c23s8d23456789o6");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testBooleanPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // b
        context.setFilterString("a9c16s4dtrueo0");
        assertRowsReturned(ALL_ROWS);

        // not b
        context.setFilterString("a9c16s4dtrueo0l2");
        assertRowsReturned(ALL_ROWS);
    }

    @Test
    public void testSmallIntToTinyIntPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // tn = 5
        context.setFilterString("a10c23s1d5o5");
        assertRowsReturned(ALL_ROWS);

        // tn = 25
        context.setFilterString("a10c23s2d25o5");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testSmallIntPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // sml = 1100
        context.setFilterString("a11c23s4d1100o5");
        assertRowsReturned(ALL_ROWS);

        // tn = 25
        context.setFilterString("a11c23s1d0o5");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testDatePushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // dt < '2020-09-01'
        context.setFilterString("a12c1082s10d2020-09-01o1");
        assertRowsReturned(ALL_ROWS);

        // dt > '2020-09-01'
        context.setFilterString("a12c1082s10d2020-09-01o2");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testCharPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // c1 = 'abc'
        context.setFilterString("a14c1042s3dabco5");
        assertRowsReturned(ALL_ROWS);

        // c1 = 'abd'
        context.setFilterString("a14c1042s3dabdo5");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testVarcharPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // vc1 = 'abcde'
        context.setFilterString("a13c1043s5dabcdeo5");
        assertRowsReturned(ALL_ROWS);

        // vc1 = 'xyz'
        context.setFilterString("a13c1043s3dxyzo5");
        assertRowsReturned(NO_ROWS);
    }

    @Test
    public void testNullPushDown() throws Exception {
        // there are no nulls in column t1
        // t1 is not null
        context.setFilterString("a0o8l2");
        assertRowsReturned(ALL_ROWS);

        // t1 is null
        context.setFilterString("a0o8");
        assertRowsReturned(NO_ROWS);

        // there are nulls in column t2
        // t2 is not null
        context.setFilterString("a1o8l2");
        assertRowsReturned(ALL_ROWS);

        // t2 is null
        context.setFilterString("a1o8");
        assertRowsReturned(ALL_ROWS);
    }

    @Test
    public void testTextOrInt() throws Exception {
        // t1 is null or num1 = 11
        // t1 produces no results, but or produces 13 tuples, so we expect
        // all rows
        context.setFilterString("a0o8a2c23s2d11o5l1");
        assertRowsReturned(ALL_ROWS);
    }

    @Test
    public void testInOperator() throws Exception {
        // num1 in (11, 12, 15)
        context.setFilterString("a2m1007s2d11s2d12s2d15o10");
        assertRowsReturned(ALL_ROWS);
    }

    @Test
    public void testNumericPushDown() throws Exception {
        // ORC SearchArgument filters out row groups. Since we have a single
        // group, all rows will be returned
        // dec1 = 5
        context.setFilterString("a4c1700s1d5o5");
        assertRowsReturned(ALL_ROWS);

        // dec1 < -1.2345599999999999999999999999999999999
        context.setFilterString("a4c1700s40d-1.2345599999999999999999999999999999999o1");
        assertRowsReturned(ALL_ROWS);

        // dec1 < -1.23456
        context.setFilterString("a4c1700s8d-1.23456o1");
        assertRowsReturned(NO_ROWS);
    }

    private void assertRowsReturned(int[] expectedRows) throws Exception {
        assertTrue(accessor.openForRead());

        OneRow batchOfRows = accessor.readNextObject();
        if (expectedRows.length == 0) {
            assertNull(batchOfRows);
        } else {
            assertNotNull(batchOfRows);
            List<List<OneField>> fieldsForBatch = resolver.getFieldsForBatch(batchOfRows);
            assertNotNull(fieldsForBatch);

            assertEquals(expectedRows.length, fieldsForBatch.size());

            int currentRow = 0;
            for (int expectedRow : expectedRows) {
                List<OneField> fieldList = fieldsForBatch.get(currentRow);
                assertEquals(16, fieldList.size(), "Row " + expectedRow);
                assertTypes(fieldList);
                assertValues(fieldList, expectedRow - 1);

                currentRow++;
            }

            batchOfRows = accessor.readNextObject();
            assertNull(batchOfRows, "No more batches expected");
        }
        accessor.closeForRead();
    }

    private void assertTypes(List<OneField> fieldList) {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();

        if (columnDescriptors.get(0).isProjected()) {
            assertEquals(DataType.TEXT.getOID(), fieldList.get(0).type);
        }
        if (columnDescriptors.get(1).isProjected()) {
            assertEquals(DataType.TEXT.getOID(), fieldList.get(1).type);
        }
        if (columnDescriptors.get(2).isProjected()) {
            assertEquals(DataType.INTEGER.getOID(), fieldList.get(2).type);
        }
        if (columnDescriptors.get(3).isProjected()) {
            assertEquals(DataType.FLOAT8.getOID(), fieldList.get(3).type);
        }
        if (columnDescriptors.get(4).isProjected()) {
            assertEquals(DataType.NUMERIC.getOID(), fieldList.get(4).type);
        }
        if (columnDescriptors.get(5).isProjected()) {
            assertEquals(DataType.TIMESTAMP.getOID(), fieldList.get(5).type);
        }

        //TODO: assertType check for col6 TIMESTAMP_WITH_TIME_ZONE

        if (columnDescriptors.get(7).isProjected()) {
            assertEquals(DataType.REAL.getOID(), fieldList.get(7).type);
        }
        if (columnDescriptors.get(8).isProjected()) {
            assertEquals(DataType.BIGINT.getOID(), fieldList.get(8).type);
        }
        if (columnDescriptors.get(9).isProjected()) {
            assertEquals(DataType.BOOLEAN.getOID(), fieldList.get(9).type);
        }
        if (columnDescriptors.get(10).isProjected()) {
            assertEquals(DataType.SMALLINT.getOID(), fieldList.get(10).type);
        }
        if (columnDescriptors.get(11).isProjected()) {
            assertEquals(DataType.SMALLINT.getOID(), fieldList.get(11).type);
        }
        if (columnDescriptors.get(12).isProjected()) {
            assertEquals(DataType.DATE.getOID(), fieldList.get(12).type);
        }
        if (columnDescriptors.get(13).isProjected()) {
            assertEquals(DataType.VARCHAR.getOID(), fieldList.get(13).type);
        }
        if (columnDescriptors.get(14).isProjected()) {
            assertEquals(DataType.BPCHAR.getOID(), fieldList.get(14).type);
        }
        if (columnDescriptors.get(15).isProjected()) {
            assertEquals(DataType.BYTEA.getOID(), fieldList.get(15).type);
        }
    }

    private void assertValues(List<OneField> fieldList, final int row) {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();

        if (columnDescriptors.get(0).isProjected()) {
            assertEquals(COL1[row], fieldList.get(0).val, "Row " + row);
        } else {
            assertNull(fieldList.get(0).val, "Row " + row);
        }

        if (columnDescriptors.get(1).isProjected() && COL2[row] != null) {
            assertEquals(COL2[row], fieldList.get(1).val, "Row " + row);
        } else {
            assertNull(fieldList.get(1).val, "Row " + row);
        }

        if (columnDescriptors.get(2).isProjected() && COL3[row] != null) {
            assertEquals(COL3[row], fieldList.get(2).val, "Row " + row);
        } else {
            assertNull(fieldList.get(2).val, "Row " + row);
        }

        if (columnDescriptors.get(3).isProjected() && COL4[row] != null) {
            assertEquals(COL4[row], fieldList.get(3).val, "Row " + row);
        } else {
            assertNull(fieldList.get(3).val, "Row " + row);
        }

        if (columnDescriptors.get(4).isProjected() && COL5[row] != null) {
            assertEquals(new HiveDecimalWritable(COL5[row]), fieldList.get(4).val, "Row " + row);
        } else {
            assertNull(fieldList.get(4).val, "Row " + row);
        }

        if (columnDescriptors.get(5).isProjected() && COL6[row] != null) {
            assertEquals(COL6[row], fieldList.get(5).val, "Row " + row);
        } else {
            assertNull(fieldList.get(5).val, "Row " + row);
        }

        //TODO: assertValue check for col6 TIMESTAMP_WITH_TIME_ZONE

        if (columnDescriptors.get(7).isProjected() && COL8[row] != null) {
            assertEquals(COL8[row], fieldList.get(7).val, "Row " + row);
        } else {
            assertNull(fieldList.get(7).val, "Row " + row);
        }

        if (columnDescriptors.get(8).isProjected() && COL9[row] != null) {
            assertEquals(COL9[row], fieldList.get(8).val, "Row " + row);
        } else {
            assertNull(fieldList.get(8).val, "Row " + row);
        }

        if (columnDescriptors.get(9).isProjected() && COL10[row] != null) {
            assertEquals(COL10[row], fieldList.get(9).val, "Row " + row);
        } else {
            assertNull(fieldList.get(9).val, "Row " + row);
        }

        if (columnDescriptors.get(10).isProjected() && COL11[row] != null) {
            assertEquals(COL11[row], fieldList.get(10).val, "Row " + row);
        } else {
            assertNull(fieldList.get(10).val, "Row " + row);
        }

        if (columnDescriptors.get(11).isProjected() && COL12[row] != null) {
            assertEquals(COL12[row], fieldList.get(11).val, "Row " + row);
        } else {
            assertNull(fieldList.get(11).val);
        }

        if (columnDescriptors.get(12).isProjected() && COL13[row] != null) {
            assertEquals(Date.valueOf(COL13[row]), fieldList.get(12).val, "Row " + row);
        } else {
            assertNull(fieldList.get(12).val);
        }

        if (columnDescriptors.get(13).isProjected() && COL14[row] != null) {
            assertEquals(COL14[row], fieldList.get(13).val, "Row " + row);
        } else {
            assertNull(fieldList.get(13).val, "Row " + row);
        }

        if (columnDescriptors.get(14).isProjected() && COL15[row] != null) {
            assertEquals(COL15[row], fieldList.get(14).val, "Row " + row);
        } else {
            assertNull(fieldList.get(14).val, "Row " + row);
        }

        if (columnDescriptors.get(15).isProjected() && COL16[row] != null) {
            assertTrue(fieldList.get(15).val instanceof byte[], "Row " + row);
            byte[] bin = (byte[]) fieldList.get(15).val;
            assertEquals(1, bin.length, "Row " + row);
            assertEquals(COL16[row].byteValue(), bin[0],
                    "Row " + row + ", actual " + String.format("%8s", Integer.toBinaryString(bin[0] & 0xFF)).replace(' ', '0'));
        } else {
            assertNull(fieldList.get(15).val, "Row " + row);
        }
    }
}
