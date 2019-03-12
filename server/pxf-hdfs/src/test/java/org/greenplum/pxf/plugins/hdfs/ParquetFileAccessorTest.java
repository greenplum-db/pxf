package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.schema.MessageType;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class ParquetFileAccessorTest {
    ParquetFileAccessor accessor;
    RequestContext context;
    MessageType schema;

    @Before
    public void setup() {
        accessor = new ParquetFileAccessor();
        context = new RequestContext();
        schema = new MessageType("hive_schema");
    }

    @Test
    public void testInitialize() {
        accessor.initialize(context);
        assertNull(context.getMetadata());
    }

}
