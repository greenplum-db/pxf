package org.greenplum.pxf.plugins.hdfs;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.avro.AvroTypeConverter;
import org.greenplum.pxf.plugins.hdfs.avro.AvroUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroResolverTest {

    List<DataType> primitiveDataTypes = Arrays.asList(DataType.BOOLEAN, DataType.BYTEA, DataType.BIGINT, DataType.INTEGER, DataType.REAL, DataType.FLOAT8, DataType.TEXT);
    // complex datatypes order is: union of nulls and bytea, record, string array, enum, fixed length bytes, map of string to long
    List<DataType> complexDataTypes = Arrays.asList(DataType.BYTEA, DataType.TEXT, DataType.TEXTARRAY, DataType.INTEGER, DataType.BYTEA, DataType.TEXT);
    // These are the underlying datatypes that AVRO uses for the logical types
    List<DataType> logicalDataTypes = Arrays.asList(DataType.TEXT, DataType.NUMERIC, DataType.DATE, DataType.TIME, DataType.TIME, DataType.TIMESTAMP_WITH_TIME_ZONE, DataType.TIMESTAMP_WITH_TIME_ZONE, DataType.TIMESTAMP, DataType.TIMESTAMP);
    List<DataType> logicalDecimalDataType = Arrays.asList(DataType.NUMERIC, DataType.NUMERIC, DataType.NUMERIC, DataType.NUMERIC, DataType.NUMERIC, DataType.NUMERIC);
    List<DataType> logicalTimeStampsDataType = Arrays.asList(DataType.TIMESTAMP_WITH_TIME_ZONE, DataType.TIMESTAMP_WITH_TIME_ZONE, DataType.TIMESTAMP, DataType.TIMESTAMP);
    private AvroResolver resolver;
    private RequestContext context;
    private Schema schema;
    private AvroTypeConverter avroTypeConverter;

    @BeforeEach
    public void setup() {
        PgUtilities pgUtilities = new PgUtilities();
        AvroUtilities avroUtilities = new AvroUtilities();
        avroUtilities.setPgUtilities(pgUtilities);
        resolver = new AvroResolver(avroUtilities, pgUtilities);
        Configuration configuration = new Configuration();
        configuration.set("pxf.fs.basePath", "/");
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        // initialize checks that accessor is some kind of avro accessor
        context.setAccessor("avro");
        context.setConfiguration(configuration);
        avroTypeConverter = AvroTypeConverter.getInstance();
    }

    @Test
    public void testSetFields_Primitive() {
        schema = getAvroSchemaForPrimitiveTypes();
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        List<OneField> fields = new ArrayList<>();
        fields.add(new OneField(DataType.BOOLEAN.getOID(), false));
        fields.add(new OneField(DataType.BYTEA.getOID(), new byte[]{(byte) 49}));
        fields.add(new OneField(DataType.BIGINT.getOID(), 23456789L));
        fields.add(new OneField(DataType.SMALLINT.getOID(), (short) 1));
        fields.add(new OneField(DataType.REAL.getOID(), 7.7f));
        fields.add(new OneField(DataType.FLOAT8.getOID(), 6.0d));
        fields.add(new OneField(DataType.TEXT.getOID(), "row1"));
        OneRow row = resolver.setFields(fields);

        assertNotNull(row);
        Object data = row.getData();
        assertNotNull(data);
        assertTrue(data instanceof GenericRecord);
        GenericRecord genericRecord = (GenericRecord) data;

        // assert column values
        assertEquals(false, genericRecord.get(0));
        assertEquals(ByteBuffer.wrap(new byte[]{(byte) 49}), genericRecord.get(1));
        assertEquals(23456789L, genericRecord.get(2));
        assertEquals(1, genericRecord.get(3));
        assertEquals((float) 7.7, genericRecord.get(4));
        assertEquals(6.0, genericRecord.get(5));
        assertEquals("row1", genericRecord.get(6));
    }

    @Test
    public void testSetFields_PrimitiveNulls() throws Exception {
        schema = getAvroSchemaForPrimitiveTypes();
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        List<OneField> fields = new ArrayList<>();
        fields.add(new OneField(DataType.BOOLEAN.getOID(), null));
        fields.add(new OneField(DataType.BYTEA.getOID(), null));
        fields.add(new OneField(DataType.BIGINT.getOID(), null));
        fields.add(new OneField(DataType.SMALLINT.getOID(), null));
        fields.add(new OneField(DataType.REAL.getOID(), null));
        fields.add(new OneField(DataType.FLOAT8.getOID(), null));
        fields.add(new OneField(DataType.TEXT.getOID(), null));
        OneRow row = resolver.setFields(fields);

        assertNotNull(row);
        Object data = row.getData();
        assertNotNull(data);
        assertTrue(data instanceof GenericRecord);
        GenericRecord genericRecord = (GenericRecord) data;

        // assert column values
        assertNull(genericRecord.get(0));
        assertNull(genericRecord.get(1));
        assertNull(genericRecord.get(2));
        assertNull(genericRecord.get(3));
        assertNull(genericRecord.get(4));
        assertNull(genericRecord.get(5));
        assertNull(genericRecord.get(6));
    }

    @Test
    public void testSetFields_Complex() {
        schema = getAvroSchemaForComplexTypes();
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        List<OneField> fields = new ArrayList<>();
        fields.add(new OneField(DataType.BYTEA.getOID(), new byte[]{65, 66, 67, 68}));               // union of null and bytes
        fields.add(new OneField(DataType.TEXT.getOID(), "{float:7.7,int:7,string:seven}"));      // record (composite type)
        fields.add(new OneField(DataType.TEXT.getOID(), "{one,two,three}"));                     // array
        fields.add(new OneField(DataType.TEXT.getOID(), "DIAMONDS"));                            // enum
        fields.add(new OneField(DataType.BYTEA.getOID(), new byte[]{'F', 'O', 'O', 'B', 'A', 'R'})); // fixed length bytes
        fields.add(new OneField(DataType.TEXT.getOID(), "{key1:123456789,key2:234567890}"));     // map of string to long
        OneRow row = resolver.setFields(fields);

        assertNotNull(row);
        Object data = row.getData();
        assertNotNull(data);
        assertTrue(data instanceof GenericRecord);
        GenericRecord genericRecord = (GenericRecord) data;

        // assert column values
        assertEquals(ByteBuffer.wrap(new byte[]{65, 66, 67, 68}), genericRecord.get(0));
        assertEquals("{float:7.7,int:7,string:seven}", genericRecord.get(1));
        assertEquals(Arrays.asList("one", "two", "three"), genericRecord.get(2));
        assertEquals("DIAMONDS", genericRecord.get(3));
        assertEquals(ByteBuffer.wrap(new byte[]{'F', 'O', 'O', 'B', 'A', 'R'}), genericRecord.get(4));
        assertEquals("{key1:123456789,key2:234567890}", genericRecord.get(5));
    }

    @Test
    public void testGetFields_Primitive() throws Exception {
        List<ColumnDescriptor> columnDescriptors = createColumnDescriptors(primitiveDataTypes);
        context.setTupleDescription(columnDescriptors);
        schema = getAvroSchemaForPrimitiveTypes();
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put(0, false);
        genericRecord.put(1, ByteBuffer.wrap(new byte[]{66, 89, 84, 69}));
        genericRecord.put(2, 23456789L);
        genericRecord.put(3, 1);
        genericRecord.put(4, 7.7f);
        genericRecord.put(5, 6.0d);
        genericRecord.put(6, "row1");
        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));

        assertField(fields, 0, false, DataType.BOOLEAN);
        assertField(fields, 1, new byte[]{'B', 'Y', 'T', 'E'}, DataType.BYTEA);
        assertField(fields, 2, 23456789L, DataType.BIGINT);
        assertField(fields, 3, 1, DataType.INTEGER); // shorts should become integers in Greenplum
        assertField(fields, 4, (float) 7.7, DataType.REAL);
        assertField(fields, 5, 6.0, DataType.FLOAT8);
        assertField(fields, 6, "row1", DataType.TEXT);
    }

    @Test
    public void testGetFields_PrimitiveNulls() throws Exception {
        List<ColumnDescriptor> columnDescriptors = createColumnDescriptors(primitiveDataTypes);
        context.setTupleDescription(columnDescriptors);
        schema = getAvroSchemaForPrimitiveTypes();
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put(0, null);
        genericRecord.put(1, null);
        genericRecord.put(2, null);
        genericRecord.put(3, null);
        genericRecord.put(4, null);
        genericRecord.put(5, null);
        genericRecord.put(6, null);
        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));

        assertField(fields, 0, null, DataType.BOOLEAN);
        assertField(fields, 1, null, DataType.BYTEA);
        assertField(fields, 2, null, DataType.BIGINT);
        assertField(fields, 3, null, DataType.INTEGER); // shorts should become integers in Greenplum
        assertField(fields, 4, null, DataType.REAL);
        assertField(fields, 5, null, DataType.FLOAT8);
        assertField(fields, 6, null, DataType.TEXT);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetFieldsEscapesTextArrayElements() throws Exception {
        List<ColumnDescriptor> columnDescriptors = Collections.singletonList(
                new ColumnDescriptor("testCol0", DataType.TEXTARRAY.getOID(), 0, "test", null)
        );
        context.setTupleDescription(columnDescriptors);
        schema = Schema.createRecord("tableName", "", "public.avro", false);
        Schema stringArraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        Schema.Field stringArrayField = new Schema.Field(Schema.Type.ARRAY.getName(), stringArraySchema, "", null);
        schema.setFields(Collections.singletonList(stringArrayField));
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put(0, new GenericData.Array(stringArraySchema, Arrays.asList("first string", "second-string", "string with a \"quote\" inside")));

        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));

        assertField(fields, 0, "{\"first string\",second-string,\"string with a \\\"quote\\\" inside\"}", DataType.TEXTARRAY);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetFieldsEscapesByteaArrayElements() throws Exception {
        List<ColumnDescriptor> columnDescriptors = Collections.singletonList(
                new ColumnDescriptor("testCol0", DataType.BYTEAARRAY.getOID(), 0, "test", null)
        );
        context.setTupleDescription(columnDescriptors);
        schema = Schema.createRecord("tableName", "", "public.avro", false);
        Schema bytesArraySchema = Schema.createArray(Schema.create(Schema.Type.BYTES));
        Schema.Field stringArrayField = new Schema.Field(Schema.Type.ARRAY.getName(), bytesArraySchema, "", null);
        schema.setFields(Collections.singletonList(stringArrayField));
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        GenericRecord genericRecord = new GenericData.Record(schema);

        byte[] data1 = new byte[]{0x01};
        byte[] data2 = new byte[]{0x23, 0x45};
        byte[] data3 = new byte[]{0x67, (byte) 0x89, (byte) 0xAB};
        ByteBuffer buffer1 = ByteBuffer.wrap(data1);
        ByteBuffer buffer2 = ByteBuffer.wrap(data2);
        ByteBuffer buffer3 = ByteBuffer.wrap(data3);
        genericRecord.put(0, new GenericData.Array(bytesArraySchema, Arrays.asList(buffer1, buffer2, buffer3)));

        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));

        assertField(fields, 0, "{\"\\\\x01\",\"\\\\x2345\",\"\\\\x6789ab\"}", DataType.BYTEAARRAY);

        GenericData.Fixed fixed1 = new GenericData.Fixed(Schema.createFixed("fixed1", "", "", 1), data1);
        GenericData.Fixed fixed2 = new GenericData.Fixed(Schema.createFixed("fixed2", "", "", 2), data2);
        GenericData.Fixed fixed3 = new GenericData.Fixed(Schema.createFixed("fixed3", "", "", 3), data3);

        // reverse the order of the fixed array elements
        genericRecord.put(0, new GenericData.Array(bytesArraySchema, Arrays.asList(fixed3, fixed2, fixed1)));
        fields = resolver.getFields(new OneRow(null, genericRecord));
        assertField(fields, 0, "{\"\\\\x6789ab\",\"\\\\x2345\",\"\\\\x01\"}", DataType.BYTEAARRAY);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetFieldsByteaArrayAsTextColumn() throws Exception {
        List<ColumnDescriptor> columnDescriptors = Collections.singletonList(
                new ColumnDescriptor("testCol0", DataType.TEXT.getOID(), 0, "test", null)
        );
        context.setTupleDescription(columnDescriptors);
        schema = Schema.createRecord("tableName", "", "public.avro", false);
        Schema bytesArraySchema = Schema.createArray(Schema.create(Schema.Type.BYTES));
        Schema.Field stringArrayField = new Schema.Field(Schema.Type.ARRAY.getName(), bytesArraySchema, "", null);
        schema.setFields(Collections.singletonList(stringArrayField));
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        GenericRecord genericRecord = new GenericData.Record(schema);

        byte[] data1 = new byte[]{0x01};
        byte[] data2 = new byte[]{0x23, 0x45};
        byte[] data3 = new byte[]{0x67, (byte) 0x89, (byte) 0xAB};
        ByteBuffer buffer1 = ByteBuffer.wrap(data1);
        ByteBuffer buffer2 = ByteBuffer.wrap(data2);
        ByteBuffer buffer3 = ByteBuffer.wrap(data3);
        genericRecord.put(0, new GenericData.Array(bytesArraySchema, Arrays.asList(buffer1, buffer2, buffer3)));

        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));

        assertField(fields, 0, "[\\\\001,\\\\043\\\\105,\\\\147\\\\211\\\\253]", DataType.TEXT);

        GenericData.Fixed fixed1 = new GenericData.Fixed(Schema.createFixed("fixed1", "", "", 1), data1);
        GenericData.Fixed fixed2 = new GenericData.Fixed(Schema.createFixed("fixed2", "", "", 2), data2);
        GenericData.Fixed fixed3 = new GenericData.Fixed(Schema.createFixed("fixed3", "", "", 3), data3);

        // reverse the order of the fixed array elements
        genericRecord.put(0, new GenericData.Array(bytesArraySchema, Arrays.asList(fixed3, fixed2, fixed1)));
        fields = resolver.getFields(new OneRow(null, genericRecord));
        assertField(fields, 0, "[\\\\147\\\\211\\\\253,\\\\043\\\\105,\\\\001]", DataType.TEXT);
    }

    @Test
    public void getFields_ComplexTypes() throws Exception {
        List<ColumnDescriptor> columnDescriptors = createColumnDescriptors(complexDataTypes);
        context.setTupleDescription(columnDescriptors);
        schema = getAvroSchemaForComplexTypes();
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        GenericRecord genericRecord = new GenericData.Record(schema);

        // UNION of NULL and BYTES
        genericRecord.put(0, ByteBuffer.wrap(new byte[]{65, 66, 67, 68}));

        // Record with a float, int and a string
        final Schema recordSchema = schema.getField("record").schema();
        final GenericData.Record nestedRecord = new GenericData.Record(recordSchema);
        nestedRecord.put("float", 7.7f);
        nestedRecord.put("int", 7);
        nestedRecord.put("string", "seven");
        genericRecord.put(1, nestedRecord);

        // ARRAY of strings
        final ArrayList<String> strings = new ArrayList<String>() {{
            add("one");
            add("two");
            add("three");
        }};
        final Schema arraySchema = schema.getField("array").schema();
        genericRecord.put(2, new GenericData.Array<>(arraySchema, strings));

        // ENUM of card suites
        final Schema enumSchema = schema.getField("enum").schema();
        genericRecord.put(3, new GenericData.EnumSymbol(enumSchema, "DIAMONDS"));

        // FIXED 4 byte string
        final Schema fixedSchema = schema.getField("fixed").schema();
        genericRecord.put(4, new GenericData.Fixed(fixedSchema, new byte[]{70, 79, 79, 66, 65, 82}));

        // MAP of string to long
        Map<String, Long> map = new HashMap<String, Long>() {{
            put("key1", 123456789L);
            put("key2", 234567890L);
        }};
        genericRecord.put(5, map);

        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));

        assertField(fields, 0, new byte[]{'A', 'B', 'C', 'D'}, DataType.BYTEA); // union of null and bytes
        assertField(fields, 1, "{float:7.7,int:7,string:seven}", DataType.TEXT); // record
        assertField(fields, 2, "{one,two,three}", DataType.TEXTARRAY); // array
        assertField(fields, 3, "DIAMONDS", DataType.TEXT); // enum
        assertField(fields, 4, new byte[]{'F', 'O', 'O', 'B', 'A', 'R'}, DataType.BYTEA); // fixed length bytes
        assertField(fields, 5, "{key1:123456789,key2:234567890}", DataType.TEXT); // map of string to long
    }

    @Test
    public void testLogicalTypes() throws Exception {
        List<ColumnDescriptor> columnDescriptors = createColumnDescriptors(logicalDataTypes);
        context.setTupleDescription(columnDescriptors);
        schema = getAvroSchemaForLogicalTypes(false);
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put(0, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        genericRecord.put(1, ByteBuffer.wrap(new byte[]{50, -6}));
        genericRecord.put(2, 12345);
        genericRecord.put(3, 14540334);
        genericRecord.put(4, 14540334343l);
        genericRecord.put(5, 1634068100402l);
        genericRecord.put(6, 1634242418614345l);
        genericRecord.put(7, 1634242418614345l);
        genericRecord.put(8, 1634068100402l);

        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));

        String date = avroTypeConverter.dateFromInt(12345, getFieldSchema(schema, 2), getFieldSchema(schema, 2).getLogicalType());

        List<Schema.Field> schemaFields = schema.getFields();
        String timeMillis = avroTypeConverter.timeMillis(14540334, getFieldSchema(schema, 3), getFieldSchema(schema, 3).getLogicalType());
        String timeMicros = avroTypeConverter.timeMicros(14540334343l, getFieldSchema(schema, 4), getFieldSchema(schema, 4).getLogicalType());
        String timeStampMillis = avroTypeConverter.timestampMillis(1634068100402l, getFieldSchema(schema, 5), getFieldSchema(schema, 5).getLogicalType());
        String timeStampMicros = avroTypeConverter.timestampMicros(1634242418614345l, getFieldSchema(schema, 6), getFieldSchema(schema, 6).getLogicalType());
        String localTimeStampMillis = avroTypeConverter.localTimestampMillis(1634068100402l, getFieldSchema(schema, 7), getFieldSchema(schema, 7).getLogicalType());
        String localTimeStampMicros = avroTypeConverter.localTimestampMicros(1634242418614345l, getFieldSchema(schema, 8), getFieldSchema(schema, 8).getLogicalType());

        assertField(fields, 0, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", DataType.UUID);
        assertField(fields, 1, new BigDecimal("130.50"), DataType.NUMERIC);
        assertField(fields, 2, date, DataType.DATE);
        assertField(fields, 3, timeMillis, DataType.TIME);
        assertField(fields, 4, timeMicros, DataType.TIME);
        assertField(fields, 5, timeStampMillis, DataType.TIMESTAMP_WITH_TIME_ZONE);
        assertField(fields, 6, timeStampMicros, DataType.TIMESTAMP_WITH_TIME_ZONE);
        assertField(fields, 7, localTimeStampMicros, DataType.TIMESTAMP);
        assertField(fields, 8, localTimeStampMillis, DataType.TIMESTAMP);
    }

    @Test
    public void testDecimalLogicalType() throws Exception {
        List<ColumnDescriptor> columnDescriptors = createColumnDescriptors(logicalDecimalDataType);
        context.setTupleDescription(columnDescriptors);
        schema = getAvroDecimalSchema();
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        GenericRecord genericRecord = new GenericData.Record(schema);

        genericRecord.put(0, new GenericData.Fixed(schema, new byte[]{70, 79, 79, 66, 65, 82}));
        genericRecord.put(1, new GenericData.Fixed(schema, new byte[]{70, 79, 79, 66, 65, 82}));
        genericRecord.put(2, new GenericData.Fixed(schema, new byte[]{70, 79, 79, 66, 65, 82}));
        genericRecord.put(3, ByteBuffer.wrap(new byte[]{102, -9}));
        genericRecord.put(4, ByteBuffer.wrap(new byte[]{50, 34, 67, -6}));
        genericRecord.put(5, ByteBuffer.wrap(new byte[]{1, -1}));

        List<OneField> oneFields = resolver.getFields(new OneRow(null, genericRecord));

        assertField(oneFields, 0, new BigDecimal("7730644610286.6"), DataType.NUMERIC);
        assertField(oneFields, 1, new BigDecimal("77306446102.866"), DataType.NUMERIC);
        assertField(oneFields, 2, new BigDecimal("7730644610.2866"), DataType.NUMERIC);
        assertField(oneFields, 3, new BigDecimal("0.26359"), DataType.NUMERIC);
        assertField(oneFields, 4, new BigDecimal("8411064.26"), DataType.NUMERIC);
        assertField(oneFields, 5, new BigDecimal("51.1"), DataType.NUMERIC);
    }

    @Test
    public void testTimeStamps() throws Exception {
        List<ColumnDescriptor> columnDescriptors = createColumnDescriptors(logicalTimeStampsDataType);
        context.setTupleDescription(columnDescriptors);
        schema = getAvroSchemaForLogicalTypes(true);
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        GenericRecord genericRecord = new GenericData.Record(schema);

        genericRecord.put(0, 1634845704629l);
        genericRecord.put(1, 1634845704629556l);
        genericRecord.put(2, 1634845704629556l);
        genericRecord.put(3, 1634845704629l);


        List<OneField> oneFields = resolver.getFields(new OneRow(null, genericRecord));
        List<Schema.Field> schemaFields = schema.getFields();
        // Default System TimeZone
        TimeZone defaultTimeZone = TimeZone.getDefault();
        String timeStampMillis = avroTypeConverter.timestampMillis(1634845704629l, getFieldSchema(schema, 0), getFieldSchema(schema, 0).getLogicalType());
        String timeStampMicros = avroTypeConverter.timestampMicros(1634845704629556l, getFieldSchema(schema, 1), getFieldSchema(schema, 1).getLogicalType());
        String localTimeStampMillis = avroTypeConverter.localTimestampMillis(1634845704629l, getFieldSchema(schema, 2), getFieldSchema(schema, 2).getLogicalType());
        String localTimeStampMicros = avroTypeConverter.localTimestampMicros(1634845704629556l, getFieldSchema(schema, 3), getFieldSchema(schema, 3).getLogicalType());

        assertField(oneFields, 0, timeStampMillis, DataType.TIMESTAMP_WITH_TIME_ZONE);
        assertField(oneFields, 1, timeStampMicros, DataType.TIMESTAMP_WITH_TIME_ZONE);
        assertField(oneFields, 2, localTimeStampMicros, DataType.TIMESTAMP);
        assertField(oneFields, 3, localTimeStampMillis, DataType.TIMESTAMP);

        TimeZone.setDefault(TimeZone.getTimeZone("est"));

        oneFields = resolver.getFields(new OneRow(null, genericRecord));

        timeStampMillis = avroTypeConverter.timestampMillis(1634845704629l, getFieldSchema(schema, 0), getFieldSchema(schema, 0).getLogicalType());
        timeStampMicros = avroTypeConverter.timestampMicros(1634845704629556l, getFieldSchema(schema, 1), getFieldSchema(schema, 1).getLogicalType());
        localTimeStampMillis = avroTypeConverter.localTimestampMillis(1634845704629l, getFieldSchema(schema, 2), getFieldSchema(schema, 2).getLogicalType());
        localTimeStampMicros = avroTypeConverter.localTimestampMicros(1634845704629556l, getFieldSchema(schema, 3), getFieldSchema(schema, 3).getLogicalType());

        assertField(oneFields, 0, timeStampMillis, DataType.TIMESTAMP_WITH_TIME_ZONE);
        assertField(oneFields, 1, timeStampMicros, DataType.TIMESTAMP_WITH_TIME_ZONE);
        assertField(oneFields, 2, localTimeStampMicros, DataType.TIMESTAMP);
        assertField(oneFields, 3, localTimeStampMillis, DataType.TIMESTAMP);

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Calcutta"));

        oneFields = resolver.getFields(new OneRow(null, genericRecord));

        timeStampMillis = avroTypeConverter.timestampMillis(1634845704629l, getFieldSchema(schema, 0), getFieldSchema(schema, 0).getLogicalType());
        timeStampMicros = avroTypeConverter.timestampMicros(1634845704629556l, getFieldSchema(schema, 1), getFieldSchema(schema, 1).getLogicalType());
        localTimeStampMillis = avroTypeConverter.localTimestampMillis(1634845704629l, getFieldSchema(schema, 2), getFieldSchema(schema, 2).getLogicalType());
        localTimeStampMicros = avroTypeConverter.localTimestampMicros(1634845704629556l, getFieldSchema(schema, 3), getFieldSchema(schema, 3).getLogicalType());

        assertField(oneFields, 0, timeStampMillis, DataType.TIMESTAMP_WITH_TIME_ZONE);
        assertField(oneFields, 1, timeStampMicros, DataType.TIMESTAMP_WITH_TIME_ZONE);
        assertField(oneFields, 2, localTimeStampMicros, DataType.TIMESTAMP);
        assertField(oneFields, 3, localTimeStampMillis, DataType.TIMESTAMP);

        TimeZone.setDefault(defaultTimeZone);
    }

    @Test
    public void wrongLogicalTypeforUUID() throws Exception {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> getWrongAvroSchemaForLogicalTypes(true));
        assertEquals("Time (micros) can only be used with an underlying long type", e.getMessage());
    }

    @Test
    public void getFields_ComplexTypesNulls() throws Exception {
        List<ColumnDescriptor> columnDescriptors = createColumnDescriptors(complexDataTypes);
        context.setTupleDescription(columnDescriptors);
        schema = getAvroSchemaForComplexTypes();
        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        GenericRecord genericRecord = new GenericData.Record(schema);
        // UNION of NULL and BYTES
        genericRecord.put(0, null);

        // Record with a float, int and a string
        genericRecord.put(1, null);

        // ARRAY of strings
        genericRecord.put(2, null);

        // ENUM of card suites
        genericRecord.put(3, null);

        // FIXED 4 byte string
        genericRecord.put(4, null);

        // MAP of string to long
        genericRecord.put(5, null);

        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));
        assertField(fields, 0, null, DataType.BYTEA);
        assertField(fields, 1, null, DataType.TEXT);
        assertField(fields, 2, null, DataType.TEXTARRAY);
        assertField(fields, 3, null, DataType.TEXT);
        assertField(fields, 4, null, DataType.BYTEA);
        assertField(fields, 5, null, DataType.TEXT);
    }

    private void assertField(List<OneField> fields, int index, Object value, DataType type) {
        assertEquals(type.getOID(), fields.get(index).type);
        if (type == DataType.BYTEA) {
            assertArrayEquals((byte[]) value, (byte[]) fields.get(index).val);
            return;
        }

        if (fields.get(index).val instanceof GenericData.EnumSymbol) {
            assertEquals(value, fields.get(index).val.toString());
            return;
        }
        assertEquals(value, fields.get(index).val);
    }

    private Schema getFieldSchema(Schema schema, int idx) {
        return schema.getFields().get(idx).schema();
    }

    private Schema getAvroSchemaForPrimitiveTypes() {
        Schema schema = Schema.createRecord("tableName", "", "public.avro", false);
        List<Schema.Field> fields = new ArrayList<>();
        Schema.Type[] types = new Schema.Type[]{
                Schema.Type.BOOLEAN,
                Schema.Type.BYTES,
                Schema.Type.LONG,
                Schema.Type.INT,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.STRING,
        };
        for (Schema.Type type : types) {
            fields.add(new Schema.Field(type.getName(), Schema.create(type), "", null));
        }
        schema.setFields(fields);

        return schema;
    }

    private Schema getAvroSchemaForComplexTypes() {
        Schema schema = Schema.createRecord("tableName", "", "public.avro", false);
        List<Schema.Field> fields = new ArrayList<>();

        // add a UNION of NULL with BYTES
        fields.add(new Schema.Field(
                Schema.Type.UNION.getName(),
                createUnion(),
                "",
                null)
        );
        // add a RECORD with a float, int, and string inside
        fields.add(new Schema.Field(
                Schema.Type.RECORD.getName(),
                createRecord(new Schema.Type[]{Schema.Type.FLOAT, Schema.Type.INT, Schema.Type.STRING}),
                "",
                null)
        );
        // add an ARRAY of strings
        fields.add(new Schema.Field(
                Schema.Type.ARRAY.getName(),
                Schema.createArray(Schema.create(Schema.Type.STRING)),
                "",
                null)
        );
        // add an ENUM of card suites
        fields.add(new Schema.Field(
                Schema.Type.ENUM.getName(),
                createEnum(new String[]{"SPADES", "HEARTS", "DIAMONDS", "CLUBS"}),
                "",
                null)
        );
        // add a FIXED with 6 byte length
        fields.add(new Schema.Field(
                Schema.Type.FIXED.getName(),
                Schema.createFixed("fixed", "", null, 6),
                "",
                null)
        );
        // add a MAP from string to long
        fields.add(new Schema.Field(
                Schema.Type.MAP.getName(),
                Schema.createMap(Schema.create(Schema.Type.LONG)),
                "",
                null)
        );
        schema.setFields(fields);

        return schema;
    }

    /**
     * Create schema for Avro Logical Types
     *
     * @return
     */
    private Schema getAvroSchemaForLogicalTypes(boolean timeStampsOnly) {
        Schema schema = Schema.createRecord("tableName", "", "public.avro", false);
        List<Schema.Field> fields = new ArrayList<>();

        if (!timeStampsOnly) {

            fields.add(new Schema.Field(
                    "uuid",
                    LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING))
            ));
            fields.add(new Schema.Field(
                    "decNum",
                    LogicalTypes.decimal(4, 2).addToSchema(Schema.create(Schema.Type.BYTES))
            ));

            fields.add(new Schema.Field(
                    "dob",
                    LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))
            ));

            fields.add(new Schema.Field(
                    "timeMillis",
                    LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))
            ));
            fields.add(new Schema.Field(
                    "timeMicros",
                    LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))
            ));
        }

        fields.add(new Schema.Field(
                "timeStampMillis",
                LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
        ));
        fields.add(new Schema.Field(
                "timeStampMicros",
                LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
        ));
        fields.add(new Schema.Field(
                "localTimestampMicros",
                LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
        ));

        fields.add(new Schema.Field(
                "localTimestampMillis",
                LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
        ));

        schema.setFields(fields);

        return schema;

    }


    /**
     * Create schema for Avro Logical Types
     *
     * @return
     */
    private Schema getWrongAvroSchemaForLogicalTypes(boolean incorrectLogicalType) {
        Schema schema = Schema.createRecord("tableName", "", "public.avro", false);
        List<Schema.Field> fields = new ArrayList<>();

        // set wrong logical type for uuid
        fields.add(new Schema.Field(
                "uuid",
                LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.STRING))
        ));

        schema.setFields(fields);

        return schema;
    }

    private Schema getAvroDecimalSchema() {

        Schema schema = Schema.createRecord("tableName", "", "public.avro", false);
        List<Schema.Field> fields = new ArrayList<>();

        /**
         * Precision must be a positive integer greater than zero. If the underlying type is a fixed, then the precision is limited by its size.
         * An array of length n can store at most floor(log_10(28 × n - 1 - 1)) base-10 digits of precision.
         * For eg: If the size for fixed is 5, Based on the above formula, the precision value cannot exceed 11  otherwise it will throw an error:
         * "fixed(5) cannot store 16 digits (max 11)"
         */
        fields.add(new Schema.Field("fixed1", LogicalTypes.decimal(5, 1).addToSchema(Schema.createFixed("fixed", "", null, 5))));
        fields.add(new Schema.Field("fixed2", LogicalTypes.decimal(4, 3).addToSchema(Schema.createFixed("fixed", "", null, 6))));
        fields.add(new Schema.Field("fixed3", LogicalTypes.decimal(5, 4).addToSchema(Schema.createFixed("fixed", "", null, 8))));
        fields.add(new Schema.Field("bytes1", LogicalTypes.decimal(12, 5).addToSchema(Schema.create(Schema.Type.BYTES))));
        fields.add(new Schema.Field("bytes2", LogicalTypes.decimal(4, 2).addToSchema(Schema.create(Schema.Type.BYTES))));
        fields.add(new Schema.Field("bytes3", LogicalTypes.decimal(2, 1).addToSchema(Schema.create(Schema.Type.BYTES))));

        schema.setFields(fields);
        return schema;
    }

    private Schema createEnum(String[] symbols) {
        List<String> values = new ArrayList<>();
        Collections.addAll(values, symbols);
        return Schema.createEnum("enum", "", null, values);
    }

    private Schema createRecord(Schema.Type[] types) {
        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Type type : types) {
            fields.add(new Schema.Field(type.getName(), Schema.create(type), "", null));
        }
        return Schema.createRecord("test", "", "", false, fields);
    }

    // we can only support Unions that have 2 elements, and one has to be NULL
    // otherwise we won't know which Greenplum type to use
    private Schema createUnion() {
        List<Schema> unionList = new ArrayList<>();
        unionList.add(Schema.create(Schema.Type.NULL));
        unionList.add(Schema.create(Schema.Type.BYTES));
        return Schema.createUnion(unionList);
    }

    private List<ColumnDescriptor> createColumnDescriptors(List<DataType> dataTypes) {
        List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
        for (int i = 0; i < dataTypes.size(); i++) {
            columnDescriptors.add(new ColumnDescriptor("testCol" + i, dataTypes.get(i).getOID(), i, "test", null));
        }
        return columnDescriptors;
    }

}
