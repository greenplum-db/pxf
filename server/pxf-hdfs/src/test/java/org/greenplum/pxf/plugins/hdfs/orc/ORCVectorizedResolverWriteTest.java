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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ORCVectorizedResolverWriteTest extends ORCVectorizedBaseTest {

    private static final String ORC_TYPES_SCHEMA = "struct<t1:string,t2:string,num1:int,dub1:double,dec1:decimal(38,18),tm:timestamp,r:float,bg:bigint,b:boolean,tn:tinyint,sml:smallint,dt:date,vc1:varchar(5),c1:char(3),bin:binary>";
    private static final String ORC_TYPES_SCHEMA_COMPOUND = "struct<id:int,bool_arr:array<boolean>,int2_arr:array<smallint>,int_arr:array<int>,int8_arr:array<bigint>,float_arr:array<float>,float8_arr:array<double>,text_arr:array<string>,bytea_arr:array<binary>,char_arr:array<char(15)>,varchar_arr:array<varchar(15)>>";
    private static final String ORC_TYPES_SCHEMA_COMPOUND_MULTI = "struct<id:int,bool_arr:array<array<boolean>>,int2_arr:array<array<smallint>>,int_arr:array<array<int>>,int8_arr:array<array<bigint>>,float_arr:array<array<float>>,float8_arr:array<array<double>>,text_arr:array<array<string>>,bytea_arr:array<array<binary>>,char_arr:array<array<char(15)>>,varchar_arr:array<array<varchar(15)>>>";
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
    public void testReturnsNullOnEmptyInput() throws Exception {
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
    public void testResolvesSingleRecord_NoRepeating_NoNulls() throws Exception {
        columnDescriptors = getAllColumns();
        context.setTupleDescription(columnDescriptors);
        when(mockWriterOptions.getSchema()).thenReturn(getSchemaForAllColumns());
        when(mockWriterOptions.getUseUTCTimestamp()).thenReturn(true);
        context.setMetadata(mockWriterOptions);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        records = new ArrayList<>(1);
        records.add(getRecord(1)); // use 1 to not get 0-based resulting values that would be similar to defaults

        OneRow batchWrapper = resolver.setFieldsForBatch(records);
        VectorizedRowBatch batch = (VectorizedRowBatch) batchWrapper.getData();
        assertNotNull(batch);
        assertEquals(1, batch.size);
        // check columns
        assertEquals(16, batch.cols.length);
//        assertEquals(32, batch.cols.length);
        assertLongColumnVector(batch, 0, false, true, 1L);
        assertBytesColumnVector(batch, 1, false, true, new byte[]{(byte) 0xA0, (byte) 0xA1});
        assertLongColumnVector(batch, 2, false, true, 123456789000000001L);
        assertLongColumnVector(batch, 3, false, true, 11L);
        assertLongColumnVector(batch, 4, false, true, 101L);
        assertBytesColumnVector(batch, 5, false, true, "row-1".getBytes(StandardCharsets.UTF_8));
        assertDoubleColumnVector(batch, 6, false, true, (double) 1.00001f);
        assertDoubleColumnVector(batch, 7, false, true, 4.14159265358979323846d);
        assertBytesColumnVector(batch, 8, false, true, "1".getBytes(StandardCharsets.UTF_8));
        assertBytesColumnVector(batch, 9, false, true, "var1".getBytes(StandardCharsets.UTF_8));
        assertDateColumnVector(batch, 10, false, true, 14610L);
        assertBytesColumnVector(batch, 11, false, true, "10:11:12".getBytes(StandardCharsets.UTF_8));
        // 1373774405000 <-- epoch millis for instant in local PST shifted to UTC - will work in PST only
        // assertTimestampColumnVector(batch, 12, false, true, new long[]{1373774405123L}, new int[]{123456000});
        assertTimestampColumnVector(batch, 12, false, true, new long[]{(1373774405L-7*60*60)*1000+123}, new int[]{123456000});
        assertTimestampColumnVector(batch, 13, false, true, new long[]{1373774405987L}, new int[]{987654000});
        assertDecimalColumnVector(batch, 14, false, true, new HiveDecimalWritable("12345678900000.000001"));
        assertBytesColumnVector(batch, 15, false, true, "476f35e4-da1a-43cf-8f7c-950ac71d4848".getBytes(StandardCharsets.UTF_8));
    }

    private void assertLongColumnVector(VectorizedRowBatch batch, int col, boolean isRepeating, boolean noNulls, Long... values) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof LongColumnVector);
        assertEquals(isRepeating, columnVector.isRepeating);
        assertEquals(noNulls, columnVector.noNulls);

        //TODO: handle nulls
        for (int row = 0; row < values.length; row++) {
            if (isRepeating) {
                assertEquals(values[row], ((LongColumnVector) columnVector).vector[0]);
                if (row != 0) {
                    assertEquals(0, ((LongColumnVector) columnVector).vector[row]);
                }
            } else {
                assertEquals(values[row], ((LongColumnVector) columnVector).vector[row]);
            }
        }
    }

    private void assertBytesColumnVector(VectorizedRowBatch batch, int col, boolean isRepeating, boolean noNulls, byte[]... values) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof BytesColumnVector);
        assertEquals(isRepeating, columnVector.isRepeating);
        assertEquals(noNulls, columnVector.noNulls);

        //TODO: handle nulls
        for (int row = 0; row < values.length; row++) {
            if (isRepeating) {
                assertTrue(Arrays.equals(values[row], ((BytesColumnVector) columnVector).vector[0]));
                if (row != 0) {
                    assertNull(((BytesColumnVector) columnVector).vector[row]);
                }
            } else {
                assertTrue(Arrays.equals(values[row], ((BytesColumnVector) columnVector).vector[row]));
            }
        }
    }

    private void assertDoubleColumnVector(VectorizedRowBatch batch, int col, boolean isRepeating, boolean noNulls, Double... values) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof DoubleColumnVector);
        assertEquals(isRepeating, columnVector.isRepeating);
        assertEquals(noNulls, columnVector.noNulls);

        //TODO: handle nulls
        for (int row = 0; row < values.length; row++) {
            if (isRepeating) {
                assertEquals(values[row], ((DoubleColumnVector) columnVector).vector[0]);
                if (row != 0) {
                    assertEquals(0, ((DoubleColumnVector) columnVector).vector[row]);
                }
            } else {
                assertEquals(values[row], ((DoubleColumnVector) columnVector).vector[row]);
            }
        }
    }

    private void assertDateColumnVector(VectorizedRowBatch batch, int col, boolean isRepeating, boolean noNulls, Long... values) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof LongColumnVector);
        assertLongColumnVector(batch, col, isRepeating, noNulls, values);
    }

    private void assertTimestampColumnVector(VectorizedRowBatch batch, int col, boolean isRepeating, boolean noNulls, long[] time, int[] nanos) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof TimestampColumnVector);
        assertEquals(isRepeating, columnVector.isRepeating);
        assertEquals(noNulls, columnVector.noNulls);

        //TODO: handle nulls
        for (int row = 0; row < time.length; row++) {
            if (isRepeating) {
                assertEquals(time[row], ((TimestampColumnVector) columnVector).time[0]);
                assertEquals(nanos[row], ((TimestampColumnVector) columnVector).nanos[0]);
                if (row != 0) {
                    assertEquals(0, ((TimestampColumnVector) columnVector).time[0]);
                    assertEquals(0, ((TimestampColumnVector) columnVector).nanos[0]);
                }
            } else {
                assertEquals(time[row], ((TimestampColumnVector) columnVector).time[row]);
                assertEquals(nanos[row], ((TimestampColumnVector) columnVector).nanos[row]);
            }
        }
    }

    private void assertDecimalColumnVector(VectorizedRowBatch batch, int col, boolean isRepeating, boolean noNulls, HiveDecimalWritable... values) {
        ColumnVector columnVector = batch.cols[col];
        assertTrue(columnVector instanceof DecimalColumnVector);
        assertEquals(isRepeating, columnVector.isRepeating);
        assertEquals(noNulls, columnVector.noNulls);

        //TODO: handle nulls
        for (int row = 0; row < values.length; row++) {
            if (isRepeating) {
                assertEquals(values[row], ((DecimalColumnVector) columnVector).vector[0]);
                if (row != 0) {
                    assertEquals(0, ((DecimalColumnVector) columnVector).vector[row]);
                }
            } else {
                assertEquals(values[row], ((DecimalColumnVector) columnVector).vector[row]);
            }
        }
    }

    private void fillEmptyRecords(int size) {
        records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            records.add(Collections.emptyList());
        }
    }

    private List<OneField> getRecord(int index) {
        List<OneField> fields = new ArrayList<>(32);
        fields.add(new OneField(DataType.BOOLEAN.getOID(), (index % 2 != 0)));
        fields.add(new OneField(DataType.BYTEA.getOID(), new byte[]{(byte) 0xA0, (byte) 0xA1}));

        fields.add(new OneField(DataType.BIGINT.getOID(), 123456789000000000L + index));
        fields.add(new OneField(DataType.SMALLINT.getOID(), 10 + index % 32000));
        fields.add(new OneField(DataType.INTEGER.getOID(), 100 + index));
        fields.add(new OneField(DataType.TEXT.getOID(), "row-" + index));
        fields.add(new OneField(DataType.REAL.getOID(), index + 0.00001f * index));
        fields.add(new OneField(DataType.FLOAT8.getOID(), index + Math.PI));
        fields.add(new OneField(DataType.BPCHAR.getOID(), String.valueOf(index)));
        fields.add(new OneField(DataType.VARCHAR.getOID(), "var" + index));
        fields.add(new OneField(DataType.DATE.getOID(), "2010-01-01"));
        fields.add(new OneField(DataType.TIME.getOID(), "10:11:12"));
        fields.add(new OneField(DataType.TIMESTAMP.getOID(), "2013-07-13 21:00:05.123456"));
        fields.add(new OneField(DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "2013-07-13 21:00:05.987654-07"));
        fields.add(new OneField(DataType.NUMERIC.getOID(), "12345678900000.00000" + index));
        fields.add(new OneField(DataType.UUID.getOID(),"476f35e4-da1a-43cf-8f7c-950ac71d4848"));
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

    private List<ColumnDescriptor> getAllColumns() {
        List<ColumnDescriptor> descriptors = new ArrayList<>();
        // scalar types
        descriptors.add(new ColumnDescriptor("col0", DataType.BOOLEAN.getOID(),0,"", null));
        descriptors.add(new ColumnDescriptor("col1", DataType.BYTEA.getOID(),1,"", null));
        descriptors.add(new ColumnDescriptor("col2", DataType.BIGINT.getOID(),2,"", null));
        descriptors.add(new ColumnDescriptor("col3", DataType.SMALLINT.getOID(),3,"", null));
        descriptors.add(new ColumnDescriptor("col4", DataType.INTEGER.getOID(),4,"", null));
        descriptors.add(new ColumnDescriptor("col5", DataType.TEXT.getOID(),5,"", null));
        descriptors.add(new ColumnDescriptor("col6", DataType.REAL.getOID(),6,"", null));
        descriptors.add(new ColumnDescriptor("col7", DataType.FLOAT8.getOID(),7,"", null));
        descriptors.add(new ColumnDescriptor("col8", DataType.BPCHAR.getOID(),8,"", null));
        descriptors.add(new ColumnDescriptor("col9", DataType.VARCHAR.getOID(),9,"", null));
        descriptors.add(new ColumnDescriptor("col10", DataType.DATE.getOID(),10,"", null));
        descriptors.add(new ColumnDescriptor("col11", DataType.TIME.getOID(),11,"", null));
        descriptors.add(new ColumnDescriptor("col12", DataType.TIMESTAMP.getOID(),12,"", null));
        descriptors.add(new ColumnDescriptor("col13", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(),13,"", null));
        descriptors.add(new ColumnDescriptor("col14", DataType.NUMERIC.getOID(),14,"", null));
        descriptors.add(new ColumnDescriptor("col15", DataType.UUID.getOID(),15,"", null));
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

    private TypeDescription getSchemaForAllColumns() {
        String schema = (new StringBuilder("struct<"))
                .append("col0:boolean,")
                .append("col1:binary,")
                .append("col2:bigint,")
                .append("col3:smallint,")
                .append("col4:int,")
                .append("col5:string,")
                .append("col6:float,")
                .append("col7:double,")
                .append("col8:char(256),")
                .append("col9:varchar(256),")
                .append("col10:date,")
                .append("col11:string,")
                .append("col12:timestamp,")
                .append("col13:timestamp with local time zone,")
                .append("col14:decimal(38,10),")
                .append("col15:string")
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
                .append(">")
                .toString();
        return TypeDescription.fromString(schema);
    }
}
