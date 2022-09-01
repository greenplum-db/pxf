package org.greenplum.pxf.plugins.json;

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonProtocolHandlerTest {

    private static final String FILE_FRAGMENTER = "org.greenplum.pxf.plugins.hdfs.HdfsFileFragmenter";
    private static final String DEFAULT_ACCESSOR = "default-accessor";
    private static final String DEFAULT_RESOLVER = "default-resolver";
    private static final String DEFAULT_FRAGMENTER = "default-fragmenter";
    private JsonProtocolHandler handler;
    private RequestContext context;

    @BeforeEach
    public void before() {
        handler = new JsonProtocolHandler();
        context = new RequestContext();
        context.setFragmenter("default-fragmenter");
        context.setAccessor("default-accessor");
        context.setResolver("default-resolver");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("c1", 1, 0, "INT", null, true)); // actual args do not matter
        columns.add(new ColumnDescriptor("c2", 2, 0, "INT", null, true)); // actual args do not matter
        context.setTupleDescription(columns);
    }

    @Test
    public void testTextIdentifierOptionMissing() {
        assertEquals(DEFAULT_FRAGMENTER, handler.getFragmenterClassName(context));
        assertEquals(DEFAULT_ACCESSOR, handler.getAccessorClassName(context));
        assertEquals(DEFAULT_RESOLVER, handler.getResolverClassName(context));
    }

    @Test
    public void testWithEmptyIdentifier() {
        context.addOption("IDENTIFIER", "");
        assertEquals(DEFAULT_FRAGMENTER, handler.getFragmenterClassName(context));
        assertEquals(DEFAULT_ACCESSOR, handler.getAccessorClassName(context));
        assertEquals(DEFAULT_RESOLVER, handler.getResolverClassName(context));
    }

    @Test
    public void testWithIdentifier() {
        context.addOption("IDENTIFIER", "c1");
        assertEquals(FILE_FRAGMENTER, handler.getFragmenterClassName(context));
        assertEquals(DEFAULT_ACCESSOR, handler.getAccessorClassName(context));
        assertEquals(DEFAULT_RESOLVER, handler.getResolverClassName(context));
    }
}
