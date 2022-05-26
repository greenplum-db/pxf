package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QuotedLineBreakAccessorTest {

    private QuotedLineBreakAccessor accessor;
    private RequestContext context;

    /*
     * setup function called before each test.
     */
    @BeforeEach
    public void setup() {
        context = new RequestContext();
        accessor = new QuotedLineBreakAccessor();

        context.setConfig("default");
        context.setProfileScheme("localfile");
        context.setUser("test-user");
        context.setFragmentMetadata(new HcfsFragmentMetadata(0, 10));
        context.setConfiguration(new Configuration());
    }

    @AfterEach
    public void tearDown() throws Exception {
        accessor.closeForRead();
    }

    @Test
    public void testFileAsRowFailsWithMoreThanOneColumn() {
        context.setDataSource("/foo/bar");
        context.addOption("FILE_AS_ROW", "true");
        // Add two columns
        context.getTupleDescription().add(new ColumnDescriptor("col1", 1, 1, "TEXT", null));
        context.getTupleDescription().add(new ColumnDescriptor("col2", 1, 2, "TEXT", null));
        accessor.setRequestContext(context);

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> accessor.afterPropertiesSet());
        assertEquals("the FILE_AS_ROW property " +
                "only supports tables with a single column in the table " +
                "definition. 2 columns were provided", e.getMessage());
    }

    @Test
    public void testReadFromEmptyFile() throws Exception {
        prepareTest("csv/empty.csv", false);

        OneRow oneRow = accessor.readNextObject();
        assertNull(oneRow);
    }

    @Test
    public void testReadFromEmptyFileFileAsRow() throws Exception {
        prepareTest("csv/empty.csv", true);

        OneRow oneRow = accessor.readNextObject();
        assertNull(oneRow);
    }

    @Test
    public void testReadFromSingleLineCsvFile() throws Exception {
        prepareTest("csv/singleline.csv", false);

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("a,b,c", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);
    }

    @Test
    public void testReadFromSingleLineCsvFileFileAsRow() throws Exception {
        prepareTest("csv/singleline.csv", true);

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("\"a,b,c\"", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);
    }

    @Test
    public void testReadFromSimpleCsvFile() throws Exception {
        prepareTest("csv/simple.csv", false);

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("1,a,b", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("2,c,d", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("3,e,f", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);
    }

    @Test
    public void testReadFromSimpleCsvFileFileAsRow() throws Exception {
        prepareTest("csv/simple.csv", true);

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("\"1,a,b", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("2,c,d", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("3,e,f\"", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);
    }

    @Test
    public void testReadFromQuotedCsvFile() throws Exception {
        prepareTest("csv/quoted.csv", false);

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("\"1\",\"2", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("and 3\",\"4\"", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);
    }

    @Test
    public void testReadFromQuotedCsvFileFileAsRow() throws Exception {
        prepareTest("csv/quoted.csv", true);

        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("\"\"\"1\"\",\"\"2", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("and 3\"\",\"\"4\"\"\"", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);
    }

    @Test
    public void testReadFromSimpleCsvFileWithSkipHeaderOption() throws Exception {
        context.addOption("SKIP_HEADER_COUNT", "1");
        prepareTest("csv/quoted.csv", false);

        OneRow oneRow = accessor.readNextObject();

        assertNotNull(oneRow);
        assertEquals("and 3\",\"4\"", oneRow.getData());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);
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
        prepareTest("csv/csv_with_header.csv",false);
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
    public void testCarriageReturnForNewLineCharacter() throws Exception {
        prepareTest("csv/csv_with_carriage_return.csv",false);
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
    public void testCarriageReturnLineFeedForNewLineCharacter() throws Exception {
        prepareTest("csv/csv_with_carriage_return_line_feed.csv",false);
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
    public void testLineFeedForNewLineCharacter() throws Exception {
        prepareTest("csv/csv_with_line_feed.csv", false);
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
    public void testSkipHeaderCountOne() throws Exception {
        prepareTest("csv/csv_with_header.csv", false);
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
        prepareTest("csv/csv_with_header.csv", false);
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
        prepareTest("csv/csv_with_header.csv", false);
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
        prepareTest("csv/csv_with_header.csv", false);
        context.addOption("SKIP_HEADER_COUNT", "10");
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        OneRow oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testSkipHeaderWithFileAsRow() throws Exception {

        prepareTest("csv/csv_with_header.csv", true);
        context.addOption("SKIP_HEADER_COUNT", "1");
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("\"line2,header1,header2,header3", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertEquals("line3,value1,value2,value3\"", oneRow.getData().toString());

        oneRow = accessor.readNextObject();
        assertNull(oneRow);

        accessor.closeForRead();
    }

    @Test
    public void testWriteIsNotSupported() throws Exception {
        context.setProfile("text:multi");
        accessor.setRequestContext(context);

        prepareTest("csv/simple.csv", false);

        Exception e = assertThrows(UnsupportedOperationException.class, () -> accessor.openForWrite());
        assertEquals("Profile 'text:multi' does not support write operation.", e.getMessage());

        e = assertThrows(UnsupportedOperationException.class, () -> accessor.writeNextObject(new OneRow()));
        assertEquals("Profile 'text:multi' does not support write operation.", e.getMessage());

        e = assertThrows(UnsupportedOperationException.class, () -> accessor.closeForWrite());
        assertEquals("Profile 'text:multi' does not support write operation.", e.getMessage());
    }

    private void prepareTest(String resourceName, boolean fileAsRow) throws Exception {
        if (fileAsRow) {
            context.addOption("FILE_AS_ROW", "true");
            context.getTupleDescription().add(new ColumnDescriptor(
                    "col1", 1, 1, "TEXT", null
            ));
        }

        context.setDataSource(this.getClass().getClassLoader()
                .getResource(resourceName).toURI().toString());

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();
    }
}
