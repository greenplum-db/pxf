package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class LineBreakAccessorTest {

    private Accessor accessor;
    private RequestContext context;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        accessor = new LineBreakAccessor();

        context = new RequestContext();
        context.setConfig("default");
        context.setProfileScheme("localfile");
        context.setUser("test-user");
    }

    @Test
    public void testLineFeedForNewLineCharacter() throws Exception {
        prepareTest("csv/csv_with_line_feed.csv");
        context.getGreenplumCSV().withNewline("\n");

        accessor.initialize(context);
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

        accessor.initialize(context);
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

        accessor.initialize(context);
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
    public void testSkipHeaderCountIsNotANumber() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property SKIP_HEADER_COUNT has incorrect value foo : must be a non-negative integer");

        context.setDataSource("/foo");
        context.addOption("SKIP_HEADER_COUNT", "foo");
        accessor.initialize(context);
    }

    @Test
    public void testSkipHeaderCountIsNotANaturalNumber() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property SKIP_HEADER_COUNT has incorrect value -5 : must be a non-negative integer");

        context.setDataSource("/foo");
        context.addOption("SKIP_HEADER_COUNT", "-5");
        accessor.initialize(context);
    }

    @Test
    public void testDontSkipHeaders() throws Exception {
        prepareTest("csv/csv_with_header.csv");
        accessor.initialize(context);
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
        accessor.initialize(context);
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
        accessor.initialize(context);
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
        accessor.initialize(context);
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
        accessor.initialize(context);
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
        FileSplit split = new FileSplit(path, 0, length, (String[]) null);

        context.setDataSource(filepath);
        context.setFragmentMetadata(HdfsUtilities.prepareFragmentMetadata(split));
    }

}