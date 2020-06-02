package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.schema.MessageType;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;

public class ParquetFileAccessorTest {
    ParquetFileAccessor accessor;
    RequestContext context;
    MessageType schema;

    @BeforeEach
    public void setup() {
        accessor = new ParquetFileAccessor();
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        schema = new MessageType("hive_schema");
    }

    @Test
    public void testInitialize() {
        accessor.setRequestContext(context);
        assertNull(context.getMetadata());
    }
}
