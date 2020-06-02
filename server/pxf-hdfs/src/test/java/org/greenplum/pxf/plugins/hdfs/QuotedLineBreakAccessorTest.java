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
                () -> accessor.initialize());
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
        accessor.initialize();
        accessor.openForRead();
    }
}
