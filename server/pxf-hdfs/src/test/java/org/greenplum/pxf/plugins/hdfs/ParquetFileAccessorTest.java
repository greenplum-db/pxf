package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.schema.MessageType;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class ParquetFileAccessorTest {
    ParquetFileAccessor accessor;
    RequestContext context;
    MessageType schema;

    @Before
    public void setup() {
        accessor = new ParquetFileAccessor();
        context = new RequestContext();
        context.setConfig("default");
        schema = new MessageType("hive_schema");
    }

    @Test
    public void testInitialize() {
        accessor.initialize(context);
        assertNull(context.getMetadata());
    }

}
