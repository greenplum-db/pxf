package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the readLine functionality in QuotedLineBreakAccessor
 * where we read one line ahead to be able to determine when
 * the last line occurs
 */
public class QuotedLineBreakAccessorReadLineTest {

    private QuotedLineBreakAccessor accessor;

    /*
     * setup function called before each test.
     */
    @BeforeEach
    public void setup() {
        RequestContext context = new RequestContext();
        context.setDataSource("/foo/bar");
        context.setConfig("default");
        context.setUser("test-user");
        context.addOption("FILE_AS_ROW", "true");
        context.getTupleDescription().add(new ColumnDescriptor(
                "file_as_row", 1, 1, "TEXT", null
        ));

        accessor = new QuotedLineBreakAccessor();
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
    }

    @Test
    public void testReadLineReturnsNullWhenReaderReturnsNull()
            throws IOException {
        accessor.reader = mock(BufferedReader.class);
        when(accessor.reader.readLine()).thenReturn(null);
        assertNull(accessor.readLine());

        // Make sure the queue was never created
        assertNull(accessor.lineQueue);
    }

    @Test
    public void testReadLineReturnsSingleLine() throws IOException {
        accessor.reader = mock(BufferedReader.class);

        when(accessor.reader.readLine())
                .thenReturn("first line")
                .thenReturn(null);

        assertEquals("first line", accessor.readLine());
        assertNull(accessor.readLine());

        // Make sure the queue was consumed
        assertEquals(0, accessor.lineQueue.size());
    }

    @Test
    public void testReadLineReturnsMultipleLines() throws IOException {
        accessor.reader = mock(BufferedReader.class);

        when(accessor.reader.readLine())
                .thenReturn("first line")
                .thenReturn("second line")
                .thenReturn("third line")
                .thenReturn("fourth line")
                .thenReturn("fifth line")
                .thenReturn(null);

        assertEquals("first line", accessor.readLine());
        assertEquals("second line", accessor.readLine());
        assertEquals("third line", accessor.readLine());
        assertEquals("fourth line", accessor.readLine());
        assertEquals("fifth line", accessor.readLine());
        assertNull(accessor.readLine());

        // Make sure the queue was consumed
        assertEquals(0, accessor.lineQueue.size());
    }

}
