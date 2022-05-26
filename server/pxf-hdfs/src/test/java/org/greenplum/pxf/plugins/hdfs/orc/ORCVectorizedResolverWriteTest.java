package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ORCVectorizedResolverWriteTest extends ORCVectorizedBaseTest {
    private ORCVectorizedResolver resolver;
    private RequestContext context;
    private List<List<OneField>> records;

    @Mock
    private OrcFile.WriterOptions mockWriterOptions;

    @BeforeEach
    public void setup() {
        super.setup();

        resolver = new ORCVectorizedResolver();
        context = new RequestContext();
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setUser("test-user");
        context.setTupleDescription(columnDescriptors);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.setConfiguration(new Configuration());
    }

    @Test
    public void testInitialize() {
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
    }

    @Test
    public void testGetBatchSize() {
        assertEquals(1024, resolver.getBatchSize());
    }

    @Test
    public void testReturnsNullOnEmptyInput() {
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        assertNull(resolver.setFieldsForBatch(null));
        assertNull(resolver.setFieldsForBatch(Collections.emptyList()));
    }

    @Test
    public void testFailsOnBatchSizeMismatch() {
        fillEmptyRecords(1025);
        Exception e = assertThrows(PxfRuntimeException.class, () -> resolver.setFieldsForBatch(records));
        assertEquals("Provided set of 1025 records is greater than the batch size of 1024", e.getMessage());
    }

    @Test
    public void testFailsOnMissingSchema() {
        context.setMetadata(null);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        fillEmptyRecords(1);
        Exception e = assertThrows(RuntimeException.class, () -> resolver.setFieldsForBatch(records));
        assertEquals("No schema detected in request context", e.getMessage());
    }

    @Test
    public void testExceedingDefaultPrecisionWithRounding() {
        // simple test with hardcoded value assertions to make sure basic test logic itself is correct
        boolean[] IS_NULL = new boolean[16]; // no nulls in test records
        boolean[] NO_NULL = new boolean[16]; // no nulls in test records
        Arrays.fill(NO_NULL, true);

        columnDescriptors = getAllColumns();
        context.setTupleDescription(columnDescriptors);
        when(mockWriterOptions.getSchema()).thenReturn(getSchemaForAllColumns());
        when(mockWriterOptions.getUseUTCTimestamp()).thenReturn(true);
        context.setMetadata(mockWriterOptions);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        records = new ArrayList<>(1);
        List<OneField> record = getRecord(0, -1);
        // reset the decimal value to a higher unsupported (>38) precision
        record.set(14, new OneField(DataType.NUMERIC.getOID(),"12345678901234567890123456789.0123456789"));
        records.add(record);

        OneRow batchWrapper = resolver.setFieldsForBatch(records);
        VectorizedRowBatch batch = (VectorizedRowBatch) batchWrapper.getData();

        // this value we expect to be rounded
        assertDecimalColumnVectorCell(batch,0,14, IS_NULL, new HiveDecimalWritable("12345678901234567890123456789.012345679"));
    }

    @Test
    public void testExceedingDefaultPrecisionNoRounding() {
        // simple test with hardcoded value assertions to make sure basic test logic itself is correct
        boolean[] IS_NULL = new boolean[16]; // no nulls in test records
        boolean[] NO_NULL = new boolean[16]; // no nulls in test records
        Arrays.fill(NO_NULL, true);

        columnDescriptors = getAllColumns();
        context.setTupleDescription(columnDescriptors);
        when(mockWriterOptions.getSchema()).thenReturn(getSchemaForAllColumns());
        when(mockWriterOptions.getUseUTCTimestamp()).thenReturn(true);
        context.setMetadata(mockWriterOptions);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        records = new ArrayList<>(1);
        List<OneField> record = getRecord(0, -1);
        // reset the decimal value to a higher unsupported (>38) precision
        record.set(14, new OneField(DataType.NUMERIC.getOID(),"123456789012345678901234567890123456789"));
        records.add(record);

        OneRow batchWrapper = resolver.setFieldsForBatch(records);
        VectorizedRowBatch batch = (VectorizedRowBatch) batchWrapper.getData();

        // this value we expect to be not set and null flag turned on
        IS_NULL[0] = true;
        assertDecimalColumnVectorCell(batch,0,14, IS_NULL, null);
    }

    @Test
    public void testResolvesSingleRecord_NoNulls() {
        // simple test with hardcoded value assertions to make sure basic test logic itself is correct
        boolean[] IS_NULL = new boolean[16]; // no nulls in test records
        boolean[] NO_NULL = new boolean[16]; // no nulls in test records
        Arrays.fill(NO_NULL, true);

        columnDescriptors = getAllColumns();
        context.setTupleDescription(columnDescriptors);
        when(mockWriterOptions.getSchema()).thenReturn(getSchemaForAllColumns());
        when(mockWriterOptions.getUseUTCTimestamp()).thenReturn(true);
        context.setMetadata(mockWriterOptions);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        records = new ArrayList<>(1);
        records.add(getRecord(0, -1));
        records.add(getRecord(1, -1));

        OneRow batchWrapper = resolver.setFieldsForBatch(records);
        VectorizedRowBatch batch = (VectorizedRowBatch) batchWrapper.getData();

        assertBatch(batch, 2, 16, getAllColumnTypes(), NO_NULL, new boolean[16][2]);
        // spot check columns in row 1 (not in row 0 to be different from defaults) with hardcoded value assertions
        assertLongColumnVectorCell     (batch,1, 0, IS_NULL, 1L);
        assertBytesColumnVectorCell    (batch,1, 1, IS_NULL, new byte[]{(byte) 0x01, (byte) 0x02});
        assertLongColumnVectorCell     (batch,1, 2, IS_NULL, 123456789000000001L);
        assertLongColumnVectorCell     (batch,1, 3, IS_NULL, 11L);
        assertLongColumnVectorCell     (batch,1, 4, IS_NULL, 101L);
        assertBytesColumnVectorCell    (batch,1, 5, IS_NULL, "row-1".getBytes(StandardCharsets.UTF_8));
        assertDoubleColumnVectorCell   (batch,1, 6, IS_NULL, (double) 1.00001f);
        assertDoubleColumnVectorCell   (batch,1, 7, IS_NULL, 4.14159265358979323846d);
        assertBytesColumnVectorCell    (batch,1, 8, IS_NULL, "1".getBytes(StandardCharsets.UTF_8));
        assertBytesColumnVectorCell    (batch,1, 9, IS_NULL, "var1".getBytes(StandardCharsets.UTF_8));
        assertDateColumnVectorCell     (batch,1,10, IS_NULL, 14611L);
        assertBytesColumnVectorCell    (batch,1,11, IS_NULL, "10:11:01".getBytes(StandardCharsets.UTF_8));
        assertTimestampColumnVectorCell(batch,1,12, IS_NULL, (1373774405L-7*60*60)*1000+1, 1456000);
        assertTimestampColumnVectorCell(batch,1,13, IS_NULL, 1373774405987L,987001000);
        assertDecimalColumnVectorCell  (batch,1,14, IS_NULL, new HiveDecimalWritable("12345678900000.000001"));
        assertBytesColumnVectorCell    (batch,1,15, IS_NULL, "476f35e4-da1a-43cf-8f7c-950a00000001".getBytes(StandardCharsets.UTF_8));
    }
    @Test
    public void testResolvesBatch_WithNulls() {
        columnDescriptors = getAllColumns();
        context.setTupleDescription(columnDescriptors);
        when(mockWriterOptions.getSchema()).thenReturn(getSchemaForAllColumns());
        when(mockWriterOptions.getUseUTCTimestamp()).thenReturn(true);
        context.setMetadata(mockWriterOptions);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        int numColumns = 16;
        int numRows = 3;

        // iterate over columns
        for (int column = 0; column < numColumns; column++) {
            // iterate over null placement of value within the column
            for (NullPlacement placement : NullPlacement.values()) {
                // request the set or records
                records = getRecordsWithNulls(column, placement);
                // convert into batch
                VectorizedRowBatch batch = (VectorizedRowBatch) resolver.setFieldsForBatch(records).getData();
                // prepare expected null values
                boolean[] noNulls = new boolean[numColumns];
                Arrays.fill(noNulls, true); // all column are noNulls to start with
                boolean[][] isNull = new boolean[numColumns][numRows]; // no null indicators by default
                if (!placement.equals(NullPlacement.NONE)) {
                    noNulls[column] = false; // column under test when some rows will have null values
                    for (int row = 0; row < numRows; row++) {
                        if (row == placement.ordinal() || placement.equals(NullPlacement.ALL)) {
                            // according to our use case on null placement, we expect this row for the test column to be null
                            isNull[column][row] = true;
                        }
                    }
                }
                // assert batch values for null flags and correctness of non-null values
                assertBatch(batch, 3, numColumns, getAllColumnTypes(), noNulls, isNull);
            }
        }
    }

    private void assertBatch(VectorizedRowBatch batch, int numRows, int numColumns, ColumnVector.Type[] columnType, boolean[] noNulls, boolean[][] isNull) {
        assertNotNull(batch);
        assertEquals(numRows, batch.size);
        assertEquals(numColumns, batch.cols.length);

        // normalize array for ArrayEquals to be the same size as default batch size of 1024
        boolean[][] isNullNormalized = new boolean[numColumns][1024];
        for (int column = 0; column < numColumns; column++) {
            System.arraycopy(isNull[column], 0, isNullNormalized[column], 0, numRows);
        }

        // assert correctness of column metadata
        for (int column = 0; column < numColumns; column++) {
            ColumnVector columnVector = batch.cols[column];
            assertEquals(columnType[column], columnVector.type);
            assertEquals(noNulls[column], columnVector.noNulls);
            assertArrayEquals(isNullNormalized[column], columnVector.isNull);
            assertFalse(columnVector.isRepeating); // we are not setting this flag
        }
        // assert each row of the batch
        for (int row = 0; row < numRows; row++) {
            assertRecord(batch, row, isNullNormalized);
        }
    }

    private List<OneField> getRecord(int index, int nullColumn) {
        List<OneField> fields = new ArrayList<>(32);
        fields.add(new OneField(DataType.BOOLEAN.getOID(),   nullColumn ==  0 ? null : (index % 2 != 0)));
        fields.add(new OneField(DataType.BYTEA.getOID(),     nullColumn ==  1 ? null : new byte[]{(byte) index, (byte) (index + 1)}));
        fields.add(new OneField(DataType.BIGINT.getOID(),    nullColumn ==  2 ? null : 123456789000000000L + index));
        fields.add(new OneField(DataType.SMALLINT.getOID(),  nullColumn ==  3 ? null : 10 + index % 32000));
        fields.add(new OneField(DataType.INTEGER.getOID(),   nullColumn ==  4 ? null : 100 + index));
        fields.add(new OneField(DataType.TEXT.getOID(),      nullColumn ==  5 ? null : "row-" + index));
        fields.add(new OneField(DataType.REAL.getOID(),      nullColumn ==  6 ? null : index + 0.00001f * index));
        fields.add(new OneField(DataType.FLOAT8.getOID(),    nullColumn ==  7 ? null : index + Math.PI));
        fields.add(new OneField(DataType.BPCHAR.getOID(),    nullColumn ==  8 ? null : String.valueOf(index)));
        fields.add(new OneField(DataType.VARCHAR.getOID(),   nullColumn ==  9 ? null : "var" + index));
        fields.add(new OneField(DataType.DATE.getOID(),      nullColumn == 10 ? null : String.format("2010-01-%02d", (index % 30) + 1)));
        fields.add(new OneField(DataType.TIME.getOID(),      nullColumn == 11 ? null : String.format("10:11:%02d", index % 60)));
        fields.add(new OneField(DataType.TIMESTAMP.getOID(), nullColumn == 12 ? null : String.format("2013-07-13 21:00:05.%03d456", index % 1000)));
        fields.add(new OneField(DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), nullColumn == 13 ? null : String.format("2013-07-13 21:00:05.987%03d-07", index % 1000)));
        fields.add(new OneField(DataType.NUMERIC.getOID(),   nullColumn == 14 ? null : "12345678900000.00000" + index));
        fields.add(new OneField(DataType.UUID.getOID(),      nullColumn == 15 ? null : String.format("476f35e4-da1a-43cf-8f7c-950a%08d", index % 100000000)));
        // array types
        /*
        fields.add(new OneField(DataType.INT2ARRAY.getOID(),16,"", null));
        fields.add(new OneField(DataType.INT4ARRAY.getOID(),17,"", null));
        fields.add(new OneField(DataType.INT8ARRAY.getOID(),18,"", null));
        fields.add(new OneField(DataType.BOOLARRAY.getOID(),19,"", null));
        fields.add(new OneField(DataType.TEXTARRAY.getOID(),20,"", null));
        fields.add(new OneField(DataType.FLOAT4ARRAY.getOID(),21,"", null));
        fields.add(new OneField(DataType.FLOAT8ARRAY.getOID(),22,"", null));
        fields.add(new OneField(DataType.BYTEAARRAY.getOID(),23,"", null));
        fields.add(new OneField(DataType.BPCHARARRAY.getOID(),24,"", null));
        fields.add(new OneField(DataType.VARCHARARRAY.getOID(),25,"", null));
        fields.add(new OneField(DataType.DATEARRAY.getOID(),26,"", null));
        fields.add(new OneField(DataType.UUIDARRAY.getOID(),27,"", null));
        fields.add(new OneField(DataType.NUMERICARRAY.getOID(),28,"", null));
        fields.add(new OneField(DataType.TIMEARRAY.getOID(),29,"", null));
        fields.add(new OneField(DataType.TIMESTAMPARRAY.getOID(),30,"", null));
        fields.add(new OneField(DataType.TIMESTAMP_WITH_TIMEZONE_ARRAY.getOID(),31,"", null));
         */

        return fields;
    }

    private void assertRecord(VectorizedRowBatch batch, int row, boolean[][] isNull) {
        // check columns
        assertLongColumnVectorCell     (batch, row,  0, isNull[ 0], (long) row % 2);
        assertBytesColumnVectorCell    (batch, row,  1, isNull[ 1], new byte[]{(byte) row, (byte) (row + 1)});
        assertLongColumnVectorCell     (batch, row,  2, isNull[ 2], 123456789000000000L + row);
        assertLongColumnVectorCell     (batch, row,  3, isNull[ 3], 10L + row % 32000);
        assertLongColumnVectorCell     (batch, row,  4, isNull[ 4], 100L + row);
        assertBytesColumnVectorCell    (batch, row,  5, isNull[ 5], ("row-" + row).getBytes(StandardCharsets.UTF_8));
        assertDoubleColumnVectorCell   (batch, row,  6, isNull[ 6], Float.valueOf(row + 0.00001f * row).doubleValue());
        assertDoubleColumnVectorCell   (batch, row,  7, isNull[ 7], row + Math.PI);
        assertBytesColumnVectorCell    (batch, row,  8, isNull[ 8], String.valueOf(row).getBytes(StandardCharsets.UTF_8));
        assertBytesColumnVectorCell    (batch, row,  9, isNull[ 9], ("var" + row).getBytes(StandardCharsets.UTF_8));
        assertDateColumnVectorCell     (batch, row, 10, isNull[10], 14610L + row % 30); // 14610L is for "2010-01-01"
        assertBytesColumnVectorCell    (batch, row, 11, isNull[11], String.format("10:11:%02d", row % 60).getBytes(StandardCharsets.UTF_8));
        // 1373774405000 <-- epoch millis for instant "2013-07-13 21:00:05.123456" in PST shifted to UTC
        // assertTimestampColumnVector(batch, 12, false, true, new long[]{1373774405123L}, new int[]{123456000});
        assertTimestampColumnVectorCell(batch, row, 12, isNull[12], (1373774405L-7*60*60)*1000+row%1000, (row%1000)*1000000+456000);
        assertTimestampColumnVectorCell(batch, row, 13, isNull[13], 1373774405987L, 987 * 1000000 + (row % 1000) * 1000);
        assertDecimalColumnVectorCell  (batch, row, 14, isNull[14], new HiveDecimalWritable("12345678900000.00000" + row));
        assertBytesColumnVectorCell    (batch, row, 15, isNull[15], String.format("476f35e4-da1a-43cf-8f7c-950a%08d", row % 100000000).getBytes(StandardCharsets.UTF_8));
    }
    private void assertLongColumnVectorCell(VectorizedRowBatch batch, int row, int col, boolean[] isNull, Long value) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof LongColumnVector);
        LongColumnVector longColumnVector = (LongColumnVector) batch.cols[col];

        if (isNull[row]) {
            assertFalse(longColumnVector.noNulls);
            assertTrue(longColumnVector.isNull[row]);
        } else {
            assertEquals(value, longColumnVector.vector[row]); // check expected value in the cell
        }
    }

    private void assertBytesColumnVectorCell(VectorizedRowBatch batch, int row, int col, boolean[] isNull, byte[] value) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof BytesColumnVector);
        BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[col];

        if (isNull[row]) {
            assertFalse(bytesColumnVector.noNulls);
            assertTrue(bytesColumnVector.isNull[row]);
        } else {
            assertArrayEquals(value, bytesColumnVector.vector[row]); // check expected value in the cell
        }
    }

    private void assertDoubleColumnVectorCell(VectorizedRowBatch batch, int row, int col, boolean[] isNull, Double value) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof DoubleColumnVector);
        DoubleColumnVector doubleColumnVector = (DoubleColumnVector) batch.cols[col];

        if (isNull[row]) {
            assertFalse(doubleColumnVector.noNulls);
            assertTrue(doubleColumnVector.isNull[row]);
        } else {
            assertEquals(value, doubleColumnVector.vector[row]); // check expected value in the cell
        }
    }

    private void assertDateColumnVectorCell(VectorizedRowBatch batch, int row, int col, boolean[] isNull, Long value) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof LongColumnVector);
        assertLongColumnVectorCell(batch, row, col, isNull, value);
    }

    private void assertTimestampColumnVectorCell(VectorizedRowBatch batch, int row, int col, boolean[] isNull, Long time, Integer nanos) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof TimestampColumnVector);
        TimestampColumnVector timestampColumnVector = (TimestampColumnVector) batch.cols[col];

        if (isNull[row]) {
            assertFalse(timestampColumnVector.noNulls);
            assertTrue(timestampColumnVector.isNull[row]);
        } else {
            assertEquals(time, timestampColumnVector.time[row]); // check expected value in the cell
            assertEquals(nanos, timestampColumnVector.nanos[row]); // check expected value in the cell
        }
    }

    private void assertDecimalColumnVectorCell(VectorizedRowBatch batch, int row, int col, boolean[] isNull, HiveDecimalWritable value) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof DecimalColumnVector);
        DecimalColumnVector decimalColumnVector = (DecimalColumnVector) batch.cols[col];

        if (isNull[row]) {
            assertFalse(decimalColumnVector.noNulls);
            assertTrue(decimalColumnVector.isNull[row]);
        } else {
            assertEquals(value, decimalColumnVector.vector[row]); // check expected value in the cell
        }
    }

    private void fillEmptyRecords(int size) {
        records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            records.add(Collections.emptyList());
        }
    }

    private enum NullPlacement {
        // order matters as we use ordinal position here to place null values
        FIRST, MIDDLE, LAST, ALL, NONE
    }


    private List<List<OneField>> getRecordsWithNulls(int nullColumn, NullPlacement mode) {
        // we will return 3 records with null value for a given column placed in requested position
        // working with 1 column at a time to make sure there are no mistakes with indices
        List<List<OneField>> result = new ArrayList<>(3);
        for (int row = 0; row < 3; row++) {
            // add a record where given column will take a null value depending on the ordinal number of the record
            result.add(getRecord(row, mode.ordinal() == row || mode.equals(NullPlacement.ALL) ? nullColumn : -1));
        }
        return result;
    }


    private List<ColumnDescriptor> getAllColumns() {
        List<ColumnDescriptor> descriptors = new ArrayList<>();
        // scalar types
        descriptors.add(new ColumnDescriptor( "col0", DataType.BOOLEAN.getOID()  ,0,"", null));
        descriptors.add(new ColumnDescriptor( "col1", DataType.BYTEA.getOID()    ,1,"", null));
        descriptors.add(new ColumnDescriptor( "col2", DataType.BIGINT.getOID()   ,2,"", null));
        descriptors.add(new ColumnDescriptor( "col3", DataType.SMALLINT.getOID() ,3,"", null));
        descriptors.add(new ColumnDescriptor( "col4", DataType.INTEGER.getOID()  ,4,"", null));
        descriptors.add(new ColumnDescriptor( "col5", DataType.TEXT.getOID()     ,5,"", null));
        descriptors.add(new ColumnDescriptor( "col6", DataType.REAL.getOID()     ,6,"", null));
        descriptors.add(new ColumnDescriptor( "col7", DataType.FLOAT8.getOID()   ,7,"", null));
        descriptors.add(new ColumnDescriptor( "col8", DataType.BPCHAR.getOID()   ,8,"", null));
        descriptors.add(new ColumnDescriptor( "col9", DataType.VARCHAR.getOID()  ,9,"", null));
        descriptors.add(new ColumnDescriptor("col10", DataType.DATE.getOID()     ,10,"", null));
        descriptors.add(new ColumnDescriptor("col11", DataType.TIME.getOID()     ,11,"", null));
        descriptors.add(new ColumnDescriptor("col12", DataType.TIMESTAMP.getOID(),12,"", null));
        descriptors.add(new ColumnDescriptor("col13", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(),13,"", null));
        descriptors.add(new ColumnDescriptor("col14", DataType.NUMERIC.getOID()  ,14,"", null));
        descriptors.add(new ColumnDescriptor("col15", DataType.UUID.getOID()     ,15,"", null));
        // array types
        /*
        descriptors.add(new ColumnDescriptor("col16", DataType.INT2ARRAY.getOID(),16,"", null));
        descriptors.add(new ColumnDescriptor("col17", DataType.INT4ARRAY.getOID(),17,"", null));
        descriptors.add(new ColumnDescriptor("col18", DataType.INT8ARRAY.getOID(),18,"", null));
        descriptors.add(new ColumnDescriptor("col19", DataType.BOOLARRAY.getOID(),19,"", null));
        descriptors.add(new ColumnDescriptor("col20", DataType.TEXTARRAY.getOID(),20,"", null));
        descriptors.add(new ColumnDescriptor("col21", DataType.FLOAT4ARRAY.getOID(),21,"", null));
        descriptors.add(new ColumnDescriptor("col22", DataType.FLOAT8ARRAY.getOID(),22,"", null));
        descriptors.add(new ColumnDescriptor("col23", DataType.BYTEAARRAY.getOID(),23,"", null));
        descriptors.add(new ColumnDescriptor("col24", DataType.BPCHARARRAY.getOID(),24,"", null));
        descriptors.add(new ColumnDescriptor("col25", DataType.VARCHARARRAY.getOID(),25,"", null));
        descriptors.add(new ColumnDescriptor("col26", DataType.DATEARRAY.getOID(),26,"", null));
        descriptors.add(new ColumnDescriptor("col27", DataType.UUIDARRAY.getOID(),27,"", null));
        descriptors.add(new ColumnDescriptor("col28", DataType.NUMERICARRAY.getOID(),28,"", null));
        descriptors.add(new ColumnDescriptor("col29", DataType.TIMEARRAY.getOID(),29,"", null));
        descriptors.add(new ColumnDescriptor("col30", DataType.TIMESTAMPARRAY.getOID(),30,"", null));
        descriptors.add(new ColumnDescriptor("col31", DataType.TIMESTAMP_WITH_TIMEZONE_ARRAY.getOID(),31,"", null));

         */
        return descriptors;
    }

    private ColumnVector.Type[] getAllColumnTypes() {
        return new ColumnVector.Type[]{
                ColumnVector.Type.LONG,
                ColumnVector.Type.BYTES,
                ColumnVector.Type.LONG,
                ColumnVector.Type.LONG,
                ColumnVector.Type.LONG,
                ColumnVector.Type.BYTES,
                ColumnVector.Type.DOUBLE,
                ColumnVector.Type.DOUBLE,
                ColumnVector.Type.BYTES,
                ColumnVector.Type.BYTES,
                ColumnVector.Type.LONG,
                ColumnVector.Type.BYTES,
                ColumnVector.Type.TIMESTAMP,
                ColumnVector.Type.TIMESTAMP,
                ColumnVector.Type.DECIMAL,
                ColumnVector.Type.BYTES
        };
    }

    private TypeDescription getSchemaForAllColumns() {
        String schema = "struct<" +
                "col0:boolean," +
                "col1:binary," +
                "col2:bigint," +
                "col3:smallint," +
                "col4:int," +
                "col5:string," +
                "col6:float," +
                "col7:double," +
                "col8:char(256)," +
                "col9:varchar(256)," +
                "col10:date," +
                "col11:string," +
                "col12:timestamp," +
                "col13:timestamp with local time zone," +
                "col14:decimal(38,10)," +
                "col15:string" +
//                .append("col16:array<smallint>,")
//                .append("col17:array<int>,")
//                .append("col18:array<bigint>,")
//                .append("col19:array<boolean>,")
//                .append("col20:array<string>,")
//                .append("col21:array<float>,")
//                .append("col22:array<double>,")
//                .append("col23:array<binary>,")
//                .append("col24:array<char(256)>,")
//                .append("col25:array<varchar(256)>,")
//                .append("col26:array<date>,")
//                .append("col27:array<string>,")
//                .append("col28:array<decimal(38,10)>,")
//                .append("col29:array<string>,")
//                .append("col30:array<timestamp>,")
//                .append("col31:array<timestamp with local time zone>>")
                ">";
        return TypeDescription.fromString(schema);
    }
}
