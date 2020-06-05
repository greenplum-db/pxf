package org.greenplum.pxf.plugins.hdfs;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.avro.AvroUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AvroFileAccessorTest {
    AvroFileAccessor accessor;
    RequestContext context;
    String avroDirectory;

    @BeforeEach
    public void setup() {
        accessor = new AvroFileAccessor(new AvroUtilities());
        context = new RequestContext();
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setDataSource(avroDirectory + "test.avro");
        context.setSegmentId(0);
        context.setTransactionId("testID");
        context.setProfileScheme("localfile");
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.setDataSource(this.getClass().getClassLoader().getResource("avro/").getPath() + "test.avro");
        context.setConfiguration(new Configuration());
    }

    @Test
    public void testInitialize() {
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        Schema schema = (Schema) context.getMetadata();
        assertNotNull(schema);
        verifySchema(schema, "example_schema");
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
}
