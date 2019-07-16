package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HdfsUtilities.class, UserGroupInformation.class})
public class QuotedLineBreakAccessorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private QuotedLineBreakAccessor accessor;
    private RequestContext context;

    /*
     * setup function called before each test.
     */
    @Before
    public void setup() throws IOException {
        context = new RequestContext();
        accessor = new QuotedLineBreakAccessor();

        context.setConfig("default");
        context.setProfileScheme("localfile");

        FileSplit fileSplitMock = mock(FileSplit.class);
        UserGroupInformation ugiMock = mock(UserGroupInformation.class);

        PowerMockito.mockStatic(HdfsUtilities.class);
        PowerMockito.mockStatic(UserGroupInformation.class);

        when(UserGroupInformation.getCurrentUser()).thenReturn(ugiMock);
        when(HdfsUtilities.parseFileSplit(context)).thenReturn(fileSplitMock);
        when(fileSplitMock.getStart()).thenReturn(0L);
    }

    @After
    public void tearDown() throws Exception {
        accessor.closeForRead();
    }

    @Test
    public void testFileAsRowFailsWithMoreThanOneColumn() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("the FILE_AS_ROW property " +
                "only supports tables with a single column in the table " +
                "definition. 2 columns were provided");

        context.addOption("FILE_AS_ROW", "true");
        // Add two columns
        context.getTupleDescription().add(new ColumnDescriptor("col1", 1, 1, "TEXT", null));
        context.getTupleDescription().add(new ColumnDescriptor("col2", 1, 2, "TEXT", null));
        accessor.initialize(context);
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

        accessor.initialize(context);
        accessor.openForRead();
    }
}
