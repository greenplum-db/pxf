package org.greenplum.pxf.service.serde;

import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordReaderFactoryTest {

    private RecordReaderFactory factory;
    private RequestContext context;

    @BeforeEach
    public void before() {
        context = new RequestContext();
        factory = new RecordReaderFactory();
    }

    @Test
    public void testGetGPDBWritableReader() {
        context.setOutputFormat(OutputFormat.GPDBWritable);
        assertTrue(factory.getRecordReader(context, false) instanceof GPDBWritableRecordReader);
        assertTrue(factory.getRecordReader(context, true) instanceof GPDBWritableRecordReader);
    }

    @Test
    public void testGetStreamRecordReader() {
        context.setOutputFormat(OutputFormat.TEXT);
        assertTrue(factory.getRecordReader(context, true) instanceof StreamRecordReader);
    }

    @Test
    public void testGetTestRecordReader() {
        context.setOutputFormat(OutputFormat.TEXT);
        assertTrue(factory.getRecordReader(context, false) instanceof TextRecordReader);
    }
}
