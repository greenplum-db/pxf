package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LineBreakAccessorTest {

    private Accessor accessor;
    private RequestContext context;

    @BeforeEach
    public void setup() {
        accessor = new LineBreakAccessor();

        context = new RequestContext();
        context.setConfig("default");
        context.setProfileScheme("localfile");
        context.setUser("test-user");
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.setConfiguration(new Configuration());
    }

    @Test
    public void testLineFeedForNewLineCharacter() throws Exception {
        prepareTest("csv/csv_with_line_feed.csv");
        context.getGreenplumCSV().withNewline("\n");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("this,file", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("has,line feeds", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("as,new line delimiter", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("LF,0x0A", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testCarriageReturnLineFeedForNewLineCharacter() throws Exception {
        prepareTest("csv/csv_with_carriage_return_line_feed.csv");
        context.getGreenplumCSV().withNewline("\r\n");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("this,file", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("has,line feeds", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("as,new line delimiter", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("CRLF,0x0D 0x0A", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testCarriageReturnForNewLineCharacter() throws Exception {
        prepareTest("csv/csv_with_carriage_return.csv");
        context.getGreenplumCSV().withNewline("\r");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("this,file", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("has,line feeds", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("as,new line delimiter", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("CR,0x0D", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testMixedNewLineCharacters() throws Exception {
        prepareTest("csv/csv_with_mixed_new_lines.csv");
        context.getGreenplumCSV().withNewline("\r");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("this,\nfile", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("has,line feeds\n", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testSkipHeaderCountIsNotANumber() {
        context.setDataSource("/foo");
        context.addOption("SKIP_HEADER_COUNT", "foo");
        accessor.setRequestContext(context);
        Exception e = assertThrows(IllegalArgumentException.class, accessor::afterPropertiesSet);
        assertEquals("Property SKIP_HEADER_COUNT has incorrect value foo : must be a non-negative integer", e.getMessage());
    }

    @Test
    public void testSkipHeaderCountIsNotANaturalNumber() {
        context.setDataSource("/foo");
        context.addOption("SKIP_HEADER_COUNT", "-5");
        accessor.setRequestContext(context);
        Exception e = assertThrows(IllegalArgumentException.class, accessor::afterPropertiesSet);
        assertEquals("Property SKIP_HEADER_COUNT has incorrect value -5 : must be a non-negative integer", e.getMessage());
    }

    @Test
    public void testDontSkipHeaders() throws Exception {
        prepareTest("csv/csv_with_header.csv");
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("line1,header1,header2,header3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("line2,header1,header2,header3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("line3,value1,value2,value3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testSkipHeaderCountOne() throws Exception {
        prepareTest("csv/csv_with_header.csv");
        context.addOption("SKIP_HEADER_COUNT", "1");
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("line2,header1,header2,header3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("line3,value1,value2,value3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testSkipHeaderCountWhenNonZeroFragmentIndex() throws Exception {
        prepareTest("csv/csv_with_header.csv");
        context.addOption("SKIP_HEADER_COUNT", "1");
        context.setFragmentIndex(2);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("line1,header1,header2,header3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("line2,header1,header2,header3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("line3,value1,value2,value3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testSkipHeaderCountTwo() throws Exception {
        prepareTest("csv/csv_with_header.csv");
        context.addOption("SKIP_HEADER_COUNT", "2");
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertEquals("line3,value1,value2,value3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testSkipHeaderCountTen() throws Exception {
        prepareTest("csv/csv_with_header.csv");
        context.addOption("SKIP_HEADER_COUNT", "10");
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    private void prepareTest(String resourceName) throws IOException, URISyntaxException {
        String filepath = this.getClass().getClassLoader()
                .getResource(resourceName).toURI().toString();
        Path path = new Path(filepath);
        long length = path.getFileSystem(new Configuration()).getContentSummary(path).getLength();

        context.setDataSource(filepath);
        context.setFragmentMetadata(new HcfsFragmentMetadata(0, length));
    }

}
