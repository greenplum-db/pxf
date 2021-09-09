package org.greenplum.pxf.plugins.hdfs.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AvroUtilitiesTest {
    private AvroSchemaFileReaderFactory avroSchemaFileReaderFactory;
    private RequestContext context;
    private Schema schema;
    private Schema testSchema;
    private String avroDirectory;
    private AvroUtilities avroUtilities;
    private PgUtilities pgUtilities;
    private HcfsType hcfsType;


    @BeforeEach
    public void setup() {
        avroDirectory = this.getClass().getClassLoader().getResource("avro/").getPath();
        context = new RequestContext();
        Configuration configuration = new Configuration();
        configuration.set("pxf.fs.basePath", "/");
        context.setDataSource(avroDirectory + "test.avro");
        context.setConfiguration(configuration);
        testSchema = generateTestSchema();
        avroUtilities = new AvroUtilities();
        avroSchemaFileReaderFactory = new AvroSchemaFileReaderFactory();
        pgUtilities = new PgUtilities();
        avroUtilities.setSchemaFileReaderFactory(avroSchemaFileReaderFactory);
        avroUtilities.setPgUtilities(pgUtilities);
        hcfsType = HcfsType.getHcfsType(context);
    }

    /* READ PATH */

    @Test
    public void testObtainSchema_OnRead() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "example_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Binary_HCFS() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        avroUtilities.setSchemaFileReaderFactory(avroSchemaFileReaderFactory);
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Binary_HCFS_Spaces() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        avroUtilities.setSchemaFileReaderFactory(avroSchemaFileReaderFactory);
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_HCFS() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        avroUtilities.setSchemaFileReaderFactory(avroSchemaFileReaderFactory);
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_HCFS_Spaces() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        avroUtilities.setSchemaFileReaderFactory(avroSchemaFileReaderFactory);
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Binary_FullPathToLocalFile() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Binary_FullPathToLocalFile_Spaces() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_FullPathToLocalFile() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_FullPathToLocalFile_Spaces() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WhenUserProvidedSchema_Binary_OnClasspath() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "avro/user-provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WhenUserProvidedSchema_Binary_OnClasspath_Spaces() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "avro/user provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_OnClasspath() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "avro/user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_OnClasspath_Spaces() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "avro/user provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WhenUserProvidedSchema_Binary_NotFound() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avro");

        Exception e = assertThrows(RuntimeException.class,
                () -> schema = avroUtilities.obtainSchema(context, hcfsType));
        assertEquals("Failed to obtain Avro schema from 'user-provided.avro'", e.getMessage());
    }

    @Test
    public void testObtainSchema_OnRead_WhenUserProvidedSchema_Binary_NotFound_Spaces() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "user provided.avro");

        Exception e = assertThrows(RuntimeException.class,
                () -> schema = avroUtilities.obtainSchema(context, hcfsType));
        assertEquals("Failed to obtain Avro schema from 'user provided.avro'", e.getMessage());
    }

    @Test
    public void testObtainSchema_OnRead_WhenUserProvidedSchema_Json_NotFound() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avsc");

        Exception e = assertThrows(RuntimeException.class,
                () -> schema = avroUtilities.obtainSchema(context, hcfsType));
        assertEquals("Failed to obtain Avro schema from 'user-provided.avsc'", e.getMessage());
    }

    @Test
    public void testObtainSchema_OnRead_WhenUserProvidedSchema_Json_NotFound_Spaces() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "user provided.avsc");

        Exception e = assertThrows(RuntimeException.class,
                () -> schema = avroUtilities.obtainSchema(context, hcfsType));
        assertEquals("Failed to obtain Avro schema from 'user provided.avsc'", e.getMessage());
    }

    /* WRITE PATH */

    @Test
    public void testObtainSchema_OnWrite() {
        context.setTupleDescription(EnumAvroTypeConverter.getColumnDescriptorsFromSchema(testSchema));
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifyGeneratedSchema(schema);
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_HCFS() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        avroUtilities.setSchemaFileReaderFactory(avroSchemaFileReaderFactory);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");
        context.setDataSource(avroDirectory);

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_HCFS_Spaces() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        avroUtilities.setSchemaFileReaderFactory(avroSchemaFileReaderFactory);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avro");
        context.setDataSource(avroDirectory);

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_HCFS() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        avroUtilities.setSchemaFileReaderFactory(avroSchemaFileReaderFactory);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");
        context.setDataSource(avroDirectory);

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_HCFS_Spaces() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        avroUtilities.setSchemaFileReaderFactory(avroSchemaFileReaderFactory);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avsc");
        context.setDataSource(avroDirectory);

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_FullPathToLocalFile() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_FullPathToLocalFile_Spaces() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_FullPathToLocalFile() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_FullPathToLocalFile_Spaces() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_OnClasspath() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "avro/user-provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_OnClasspath_Spaces() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "avro/user provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_OnClasspath() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "avro/user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_OnClasspath_Spaces() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "avro/user provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_NotFound() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avro");

        Exception e = assertThrows(RuntimeException.class,
                () -> schema = avroUtilities.obtainSchema(context, hcfsType));
        assertEquals("Failed to obtain Avro schema from 'user-provided.avro'", e.getMessage());
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_NotFound_Spaces() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "user provided.avro");

        Exception e = assertThrows(RuntimeException.class,
                () -> schema = avroUtilities.obtainSchema(context, hcfsType));
        assertEquals("Failed to obtain Avro schema from 'user provided.avro'", e.getMessage());
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_NotFound() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avsc");

        Exception e = assertThrows(RuntimeException.class,
                () -> schema = avroUtilities.obtainSchema(context, hcfsType));
        assertEquals("Failed to obtain Avro schema from 'user-provided.avsc'", e.getMessage());
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_NotFound_Spaces() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "user provided.avsc");

        Exception e = assertThrows(RuntimeException.class,
                () -> schema = avroUtilities.obtainSchema(context, hcfsType));
        assertEquals("Failed to obtain Avro schema from 'user provided.avsc'", e.getMessage());
    }

    @Test
    public void testDecodeIntegerArray() {
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
        Object result = avroUtilities.decodeString(arraySchema, "{1,2,3}", true, false);
        assertEquals(Arrays.asList(1, 2, 3), result);
    }

    @Test
    public void testDecodeIntegerArrayWithNulls() {
        Schema arraySchema = Schema.createArray(
                Schema.createUnion(
                        Arrays.asList(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.INT))));

        Object result = avroUtilities.decodeString(arraySchema, "{1,NULL,3}", true, false);
        assertEquals(Arrays.asList(1, null, 3), result);
    }

    @Test
    public void testDecodeStringNullableArray() {
        Schema nullableArray = Schema.createUnion(
                Arrays.asList(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(Schema.create(Schema.Type.INT))));

        Object result = avroUtilities.decodeString(nullableArray, "{1,2,3}", true, false);
        assertEquals(Arrays.asList(1, 2, 3), result);

        result = avroUtilities.decodeString(nullableArray, null, true, false);
        assertNull(result);
    }

    @Test
    public void testDecodeStringUnionOnlyNull() {
        Schema schema = Schema.createUnion(
                Collections.singletonList(
                        Schema.create(Schema.Type.NULL)
                ));
        Exception exception = assertThrows(PxfRuntimeException.class,
                () -> avroUtilities.decodeString(schema, "", true, false));
        assertEquals("Avro union schema only contains null types", exception.getMessage());
    }

    @Test
    public void testDecodeStringDoubleArray() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.DOUBLE));
        String value = "{-1.79769E+308,-2.225E-307,0,2.225E-307,1.79769E+308}";

        Object result = avroUtilities.decodeString(schema, value, true, false);
        assertEquals(Arrays.asList(-1.79769E308, -2.225E-307, 0.0, 2.225E-307, 1.79769E308), result);
    }

    @Test
    public void testDecodeStringStringArray() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.STRING));
        String value = "{fizz,buzz,fizzbuzz}";

        Object result = avroUtilities.decodeString(schema, value, true, false);
        assertEquals(Arrays.asList("fizz", "buzz", "fizzbuzz"), result);
    }

    @Test
    public void testDecodeStringByteaArrayEscapeOutput() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.BYTES));
        String value = "{\"\\\\001\",\"\\\\001#\"}";

        @SuppressWarnings("unchecked")
        List<ByteBuffer> result = (List<ByteBuffer>) avroUtilities.decodeString(schema, value, true, false);
        assertEquals(2, result.size());

        ByteBuffer buffer1 = result.get(0);
        assertEquals(ByteBuffer.wrap(new byte[] {0x01}), buffer1);
        ByteBuffer buffer2 = result.get(1);
        assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x23}), buffer2);
    }

    @Test
    public void testDecodeStringByteaArrayEscapeOutputContainsQuote() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.BYTES));
        String value = "{\"\\\"#$\"}";

        @SuppressWarnings("unchecked")
        List<ByteBuffer> result = (List<ByteBuffer>) avroUtilities.decodeString(schema, value, true, false);
        assertEquals(1, result.size());

        ByteBuffer buffer1 = result.get(0);
        assertEquals(ByteBuffer.wrap(new byte[]{0x22, 0x23, 0x24}), buffer1);
    }

    @Test
    public void testDecodeStringByteaArrayHexOutput() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.BYTES));
        String value = "{\"\\\\x01\",\"\\\\x0123\"}";

        @SuppressWarnings("unchecked")
        List<ByteBuffer> result = (List<ByteBuffer>) avroUtilities.decodeString(schema, value, true, false);
        assertEquals(2, result.size());

        ByteBuffer buffer1 = result.get(0);
        assertArrayEquals(new byte[]{0x01}, buffer1.array());
        ByteBuffer buffer2 = result.get(1);
        assertArrayEquals(new byte[]{0x01, 0x23}, buffer2.array());
    }

    @Test
    public void testDecodeStringByteaInvalidHexFormat() {
        Schema schema = Schema.create(Schema.Type.BYTES);
        String value = "\\xGG";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> avroUtilities.decodeString(schema, value, true, false));
        assertEquals("malformed bytea literal \"\\xGG\"", exception.getMessage());
    }

    @Test
    public void testDecodeStringValidBooleanArray() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.BOOLEAN));
        String value = "{t,f,t}";

        Object result = avroUtilities.decodeString(schema, value, true, false);
        assertEquals(Arrays.asList(true, false, true), result);
    }

    @Test
    public void testDecodeStringInValidBooleanArrayPXFGeneratedSchema() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.BOOLEAN));
        // this situation should never happen as the toString method of booleans (boolout) should not return a string in this format
        String value = "{true,false,true}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> avroUtilities.decodeString(schema, value, true, false));
        assertEquals("Error parsing array element: true was not of expected type \"boolean\"", exception.getMessage());
        assertEquals("Unexpected state since PXF generated the AVRO schema.", ((PxfRuntimeException) exception).getHint());
    }

    @Test
    public void testDecodeStringInValidBooleanArrayUserProvidedSchema() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.BOOLEAN));
        String value = "{t,f,5}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> avroUtilities.decodeString(schema, value, true, true));
        assertEquals("Error parsing array element: 5 was not of expected type \"boolean\"", exception.getMessage());
        assertEquals("Check that the AVRO and GPDB schemas are correct.", ((PxfRuntimeException) exception).getHint());
    }

    @Test
    public void testDecodeStringMultiDimensionalArrayWithPXFGeneratedSchema() {
        // pxf generated schemas only produce one-dimensional arrays
        Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
        String value = "{{1,2},{3,4}}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> avroUtilities.decodeString(schema, value, true, false));
        assertEquals("Error parsing array element: {1,2} was not of expected type \"int\"", exception.getMessage());
        assertEquals("Value is a multi-dimensional array, user is required to provide an AVRO schema with matching dimensions.", ((PxfRuntimeException) exception).getHint());

    }

    @Test
    public void testDecodeStringMismatchedMultiDimensionalArrayWithUserProvidedSchema() {
        Schema schema = Schema.createArray(Schema.createArray(Schema.create(Schema.Type.INT)));
        String value = "{{{1,2},{3,4}},{{5,6},{7,8}}}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> avroUtilities.decodeString(schema, value, true, true));
        assertEquals("Error parsing array element: {{1,2},{3,4}} was not of expected type {\"type\":\"array\",\"items\":\"int\"}", exception.getMessage());
        assertEquals("Value is a multi-dimensional array, please check that the provided AVRO schema has the correct dimensions.", ((PxfRuntimeException) exception).getHint());

    }

    @Test
    public void testDecodeStringMultiDimensionalArrayWithUserProvidedSchema() {
        Schema schema = Schema.createArray(Schema.createArray(Schema.create(Schema.Type.INT)));
        String value = "{{1,2},{3,4}}";

        Object result = avroUtilities.decodeString(schema, value, true, false);

        assertEquals(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)), result);
    }

    @Test
    public void testFirstNotNullSchema() {
        Schema nullSchema = Schema.create(Schema.Type.NULL);
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema intSchema = Schema.create(Schema.Type.INT);

        assertEquals(stringSchema, avroUtilities.firstNotNullSchema(Arrays.asList(nullSchema, stringSchema)));

        assertEquals(Schema.create(Schema.Type.INT), avroUtilities.firstNotNullSchema(Arrays.asList(intSchema, stringSchema, nullSchema)));

        Exception e = assertThrows(PxfRuntimeException.class, () -> avroUtilities.firstNotNullSchema(Arrays.asList(nullSchema, nullSchema, nullSchema)));
        assertEquals("Avro union schema only contains null types", e.getMessage());
    }

    /**
     * Helper method for testing schema
     *
     * @param schema the schema
     * @param name   the name
     */
    private static void verifySchema(Schema schema, String name) {
        assertNotNull(schema);
        assertEquals(Schema.Type.RECORD, schema.getType());
        assertEquals(name, schema.getName());
        Map<String, String> fieldToType = new HashMap<String, String>() {{
            put("id", "long");
            put("username", "string");
            put("followers", "array");
        }};
        for (String key : fieldToType.keySet()) {
            assertEquals(
                    fieldToType.get(key),
                    schema.getField(key).schema().getType().getName()
            );
        }
    }

    /**
     * Helper method for testing generated schema
     *
     * @param schema the schema
     */
    private static void verifyGeneratedSchema(Schema schema) {
        assertNotNull(schema);
        assertEquals(schema.getType(), Schema.Type.RECORD);
        Map<String, String> fieldToType = new HashMap<String, String>() {{
            put("id", "union");
            put("username", "union");
            put("followers", "union");
        }};
        Map<String, String> unionToInnerType = new HashMap<String, String>() {{
            put("id", "long");
            put("username", "string");
            put("followers", "string"); // arrays become strings
        }};
        for (String key : fieldToType.keySet()) {
            assertEquals(
                    fieldToType.get(key),
                    schema.getField(key).schema().getType().getName()
            );
            // check the union's inner types
            assertEquals(
                    "null",
                    schema.getField(key).schema().getTypes().get(0).getName()
            );
            assertEquals(
                    unionToInnerType.get(key),
                    schema.getField(key).schema().getTypes().get(1).getName()
            );
        }
    }

    /**
     * Generate a schema that matches the avro file
     * server/pxf-hdfs/src/test/resources/avro/test.avro
     *
     * @return the schema
     */
    private Schema generateTestSchema() {
        Schema schema = Schema.createRecord("example_schema", "A basic schema for storing messages", "com.example", false);
        List<Schema.Field> fields = new ArrayList<>();

        Schema.Type type = Schema.Type.LONG;
        fields.add(new Schema.Field("id", Schema.create(type), "Id of the user account", null));

        type = Schema.Type.STRING;
        fields.add(new Schema.Field("username", Schema.create(type), "Name of the user account", null));

        // add an ARRAY of strings
        fields.add(new Schema.Field(
                "followers",
                Schema.createArray(Schema.create(Schema.Type.STRING)),
                "Users followers",
                null)
        );
        schema.setFields(fields);

        return schema;
    }

    private File dontFindLocalFile() {
        return null;
    }
}
