package org.greenplum.pxf.plugins.hdfs;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

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
    @Before
    public void setup() {
        accessor = new QuotedLineBreakAccessor();
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