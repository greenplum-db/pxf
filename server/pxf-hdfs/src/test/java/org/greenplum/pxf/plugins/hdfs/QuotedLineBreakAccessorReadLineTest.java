package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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
@RunWith(PowerMockRunner.class)
@PrepareForTest({HdfsUtilities.class})
public class QuotedLineBreakAccessorReadLineTest {

    private QuotedLineBreakAccessor accessor;

    /*
     * setup function called before each test.
     */
    @Before
    public void setup() {
        FileSplit fileSplitMock = mock(FileSplit.class);
        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.addOption("FILE_AS_ROW", "true");
        context.getTupleDescription().add(new ColumnDescriptor(
                "file_as_row", 1, 1, "TEXT", null
        ));

        PowerMockito.mockStatic(HdfsUtilities.class);
        when(HdfsUtilities.parseFileSplit(context)).thenReturn(fileSplitMock);

        accessor = new QuotedLineBreakAccessor();
        accessor.initialize(context);
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