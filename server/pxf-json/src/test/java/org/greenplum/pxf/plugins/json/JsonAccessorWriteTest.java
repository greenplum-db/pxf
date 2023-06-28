package org.greenplum.pxf.plugins.json;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class JsonAccessorWriteTest {
    @TempDir
    static File temp; // must be non-private, one temp dir per class execution

    private JsonAccessor accessor;
    private RequestContext context;
    private Configuration configuration;

    private List<ColumnDescriptor> columnDescriptors;

    @BeforeEach
    public void setUp() {
        columnDescriptors = new ArrayList<>();

        context = new RequestContext();
        configuration = new Configuration();
        configuration.set("pxf.fs.basePath", "/");
        context.setConfiguration(configuration);

        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setSegmentId(4);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.setTupleDescription(columnDescriptors);

        accessor = new JsonAccessor(new JsonUtilities());
        accessor.setRequestContext(context);
    }

    @Test
    public void errorOnMissingEncoding() {
        PxfRuntimeException e = assertThrows(PxfRuntimeException.class,
                () -> accessor.afterPropertiesSet());
        assertEquals("Effective data encoding null is not UTF8 and is not supported.", e.getMessage());
        assertEquals("Make sure either the database is in UTF8 or the table is defined with ENCODING 'UTF8' option.", e.getHint());
    }

    @Test
    public void errorOnMissingTableEncodingDBEncodingNonUTF8() {
        context.setDatabaseEncoding(StandardCharsets.ISO_8859_1);
        PxfRuntimeException e = assertThrows(PxfRuntimeException.class,
                () -> accessor.afterPropertiesSet());
        assertEquals("Effective data encoding ISO-8859-1 is not UTF8 and is not supported.", e.getMessage());
        assertEquals("Make sure either the database is in UTF8 or the table is defined with ENCODING 'UTF8' option.", e.getHint());
    }

    @Test
    public void errorOnEmptyRoot() {
        context.addOption("ROOT", " ");
        PxfRuntimeException e = assertThrows(PxfRuntimeException.class,
                () -> accessor.afterPropertiesSet());
        assertEquals("Option ROOT can not have an empty value", e.getMessage());
    }

    @Test
    public void testWriteRowsOneRecord() throws IOException {
        runScenario("test-write-rows-one", new String[]{"blue"}, null);
    }

    @Test
    public void testWriteObjectOneRecord() throws IOException {
        runScenario("test-write-object-one", new String[]{"blue"}, "records");
    }

    @Test
    public void testWriteRowsThreeRecords() throws IOException {
        runScenario("test-write-rows-three", new String[]{"red","yellow","green"}, null);
    }

    @Test
    public void testWriteObjectThreeRecords() throws IOException {
        runScenario("test-write-object-three", new String[]{"red","yellow","green"}, "records");
    }

    /**
     * Runs test scenario, simulating a write bridge initiating the accessor, opening iterations, writing records
     * and closing the iterations. Compares contents of the written files with the expected values.
     * @param fileName name of the file / scenario
     * @param values values for the string field, one value per row
     * @param root value for the root element, null if no object layout is needed
     * @throws IOException
     */
    private void runScenario(String fileName, String[] values, String root) throws IOException {
        String path = temp.getAbsolutePath() + "/json/" + fileName;

        // -- prepare table schema and sample data, will use a simple schema as we are testing layout, not data types
        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, null, null));
        columnDescriptors.add(new ColumnDescriptor("color", DataType.TEXT.getOID(), 1, null, null));
        List<OneRow> rows = new ArrayList<>(values.length);
        for (int index = 0; index < values.length; index++) {
            List<OneField> record = new ArrayList<>(2);
            record.add(new OneField(DataType.INTEGER.getOID(), index + 1));
            record.add(new OneField(DataType.TEXT.getOID(), values[index]));
            rows.add(new OneRow(record)); // Json resolver just wraps the list of fields into a OneRow
        }

        // -- prepare context and initialize the accessor
        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123456");
        context.setDatabaseEncoding(StandardCharsets.UTF_8);
        if (root != null) {
            context.addOption("ROOT", root);
        }
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();

        // -- simulate bridge writing data
        assertTrue(accessor.openForWrite());
        for (OneRow row : rows) {
            assertTrue(accessor.writeNextObject(row));
        }
        accessor.closeForWrite();

        // -- validate a file has been written and is the same as expected one
        String extension = (root == null) ? ".jsonl" : ".json";
        File writtenFile = new File(path + "/XID-XYZ-123456_4" + extension);
        assertTrue(writtenFile.exists());
        File expectedFile = new File(getClass().getClassLoader().getResource(fileName + extension).getPath());
        assertTrue(FileUtils.contentEqualsIgnoreEOL(expectedFile, writtenFile, "UTF-8"));
    }

}
