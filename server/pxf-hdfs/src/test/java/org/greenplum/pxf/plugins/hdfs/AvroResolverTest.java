package org.greenplum.pxf.plugins.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.avro.AvroUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroResolverTest {

    private AvroResolver resolver;
    private RequestContext context;
    private Schema schema;

    @BeforeEach
    public void setup() {
        resolver = new AvroResolver(new AvroUtilities());
        Configuration configuration = new Configuration();
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        // initialize checks that accessor is some kind of avro accessor
        context.setAccessor("avro");
        context.setConfiguration(configuration);
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
        assertEquals((short) 1, genericRecord.get(3));
        assertEquals((float) 7.7, genericRecord.get(4));
        assertEquals(6.0, genericRecord.get(5));
        assertEquals("row1", genericRecord.get(6));
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
        fields.add(new OneField(DataType.TEXT.getOID(), "[one,two,three]"));                     // array
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
        assertEquals("[one,two,three]", genericRecord.get(2));
        assertEquals("DIAMONDS", genericRecord.get(3));
        assertEquals(ByteBuffer.wrap(new byte[]{'F', 'O', 'O', 'B', 'A', 'R'}), genericRecord.get(4));
        assertEquals("{key1:123456789,key2:234567890}", genericRecord.get(5));
    }

    @Test
    public void testGetFields_Primitive() throws Exception {
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

    @Test
    public void getFields_ComplexTypes() throws Exception {
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
        assertField(fields, 2, "[one,two,three]", DataType.TEXT); // array
        assertField(fields, 3, "DIAMONDS", DataType.TEXT); // enum
        assertField(fields, 4, new byte[]{'F', 'O', 'O', 'B', 'A', 'R'}, DataType.BYTEA); // fixed length bytes
        assertField(fields, 5, "{key1:123456789,key2:234567890}", DataType.TEXT); // map of string to long
    }

    @Test
    public void getFields_ComplexTypesNulls() throws Exception {
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
        assertField(fields, 2, null, DataType.TEXT);
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
        return Schema.createRecord(fields);
    }

    // we can only support Unions that have 2 elements, and one has to be NULL
    // otherwise we won't know which Greenplum type to use
    private Schema createUnion() {
        List<Schema> unionList = new ArrayList<>();
        unionList.add(Schema.create(Schema.Type.NULL));
        unionList.add(Schema.create(Schema.Type.BYTES));
        return Schema.createUnion(unionList);
    }

}
