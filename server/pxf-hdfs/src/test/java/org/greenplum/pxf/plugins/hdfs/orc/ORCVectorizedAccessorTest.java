package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.LongWritable;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ORCVectorizedAccessorTest extends ORCVectorizedBaseTest {
    private ORCVectorizedAccessor accessor;
    private RequestContext context;

    @BeforeEach
    public void setup() {
        super.setup();

        accessor = new ORCVectorizedAccessor();
        context = new RequestContext();
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setUser("test-user");
        context.setConfiguration(new Configuration());
    }

    @Test
    public void testInitialize() {
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        assertNull(context.getMetadata());
    }

    @Test
    public void testReadOrcTypesFile() throws IOException {
        runTestScenarioReadOrcTypesFile(15);
    }

    @Test
    public void testReadOrcTypesFileByPosition() throws IOException {
        context.addOption("MAP_BY_POSITION", "true");
        runTestScenarioReadOrcTypesFile(15);
    }

    @Test
    public void testReadOrcTypesFileWithColumnProjection() throws IOException {
        // Only project indexes 2, 5 and 12
        IntStream.range(0, columnDescriptors.size()).forEach(idx ->
                columnDescriptors
                        .get(idx)
                        .setProjected(idx == 2 || idx == 5 || idx == 12));

        runTestScenarioReadOrcTypesFile(3);
    }

    @Test
    public void testReadOrcTypesFileByPositionWithColumnProjection() throws IOException {
        // Only project indexes 2, 5 and 12
        IntStream.range(0, columnDescriptors.size()).forEach(idx ->
                columnDescriptors
                        .get(idx)
                        .setProjected(idx == 2 || idx == 5 || idx == 12));

        context.addOption("MAP_BY_POSITION", "true");
        runTestScenarioReadOrcTypesFile(3);
    }

    /**
     * This test case is effectively as if testing column projection. Your
     * Greenplum table definition can be a subset of the number of columns of
     * your ORC file.
     *
     * @throws IOException when a read error occurs
     */
    @Test
    public void testReadSubsetOfOrcTypesFile() throws IOException {
        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("t1", DataType.TEXT.getOID(), 0, "text", null));
        columnDescriptors.add(new ColumnDescriptor("num1", DataType.INTEGER.getOID(), 2, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("tm", DataType.TIMESTAMP.getOID(), 5, "timestamp", null));
        columnDescriptors.add(new ColumnDescriptor("r", DataType.REAL.getOID(), 6, "real", null));
        columnDescriptors.add(new ColumnDescriptor("tn", DataType.SMALLINT.getOID(), 9, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("dt", DataType.DATE.getOID(), 11, "date", null));
        columnDescriptors.add(new ColumnDescriptor("bin", DataType.BYTEA.getOID(), 14, "bin", null));

        runTestScenarioReadOrcTypesFile(7);
    }

    /**
     * This test case is effectively as if testing column projection. Your
     * Greenplum table definition can be a subset of the columns in the ORC
     * file, specified in different order.
     *
     * @throws IOException when a read error occurs
     */
    @Test
    public void testReadUnorderedSubsetOfOrcTypesFile() throws IOException {
        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("r", DataType.REAL.getOID(), 6, "real", null));
        columnDescriptors.add(new ColumnDescriptor("t2", DataType.TEXT.getOID(), 1, "text", null));
        columnDescriptors.add(new ColumnDescriptor("dt", DataType.DATE.getOID(), 11, "date", null));
        columnDescriptors.add(new ColumnDescriptor("tn", DataType.SMALLINT.getOID(), 9, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("dec1", DataType.NUMERIC.getOID(), 4, "numeric", new Integer[]{38, 18}));
        columnDescriptors.add(new ColumnDescriptor("bg", DataType.BIGINT.getOID(), 7, "int8", null));

        runTestScenarioReadOrcTypesFile(6);
    }

    @Test
    public void testOrcWrite() {
        // TODO: implement tests
    }

    private void runTestScenarioReadOrcTypesFile(int expectedNumCols) throws IOException {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        String path = Objects.requireNonNull(getClass().getClassLoader().getResource("orc/orc_types.orc")).getPath();
        context.setDataSource(path);
        context.setFragmentMetadata(new HcfsFragmentMetadata(0, 2257));
        context.setTupleDescription(columnDescriptors);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();

        assertTrue(accessor.openForRead());
        OneRow oneRow = accessor.readNextObject();
        assertNotNull(oneRow);
        assertNotNull(oneRow.getKey());
        assertNotNull(oneRow.getData());
        assertTrue(oneRow.getKey() instanceof LongWritable);
        assertTrue(oneRow.getData() instanceof VectorizedRowBatch);

        VectorizedRowBatch batch = (VectorizedRowBatch) oneRow.getData();
        assertEquals(expectedNumCols, batch.numCols);
        assertEquals(25, batch.count());
        assertEquals(25, batch.getSelectedSize());

        assertNull(accessor.readNextObject());

        accessor.closeForRead();
    }

}