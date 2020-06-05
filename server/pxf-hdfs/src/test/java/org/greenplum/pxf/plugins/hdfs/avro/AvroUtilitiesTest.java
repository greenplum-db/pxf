package org.greenplum.pxf.plugins.hdfs.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AvroUtilitiesTest {
    private RequestContext context;
    private Schema schema;
    private Schema testSchema;
    private String avroDirectory;
    private AvroUtilities avroUtilities;
    private HcfsType hcfsType;

    @BeforeEach
    public void setup() {
        avroDirectory = this.getClass().getClassLoader().getResource("avro/").getPath();
        context = new RequestContext();
        Configuration configuration = new Configuration();
        context.setDataSource(avroDirectory + "test.avro");
        context.setConfiguration(configuration);
        testSchema = generateTestSchema();
        avroUtilities = new AvroUtilities();
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
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Binary_HCFS_Spaces() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avro");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_HCFS() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_HCFS_Spaces() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
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
        context.setTupleDescription(AvroTypeConverter.getColumnDescriptorsFromSchema(testSchema));
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifyGeneratedSchema(schema);
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_HCFS() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");
        context.setDataSource(avroDirectory);

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Binary_HCFS_Spaces() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user provided.avro");
        context.setDataSource(avroDirectory);

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_HCFS() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");
        context.setDataSource(avroDirectory);

        schema = avroUtilities.obtainSchema(context, hcfsType);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnWrite_WithUserProvidedSchema_Json_HCFS_Spaces() {
        avroUtilities = new AvroUtilities((file) -> dontFindLocalFile());
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
