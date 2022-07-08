package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.orc.CompressionKind;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.greenplum.pxf.plugins.hdfs.HcfsType.CONFIG_KEY_BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        runTestScenario_ReadOrcTypesFile(15);
    }

    @Test
    public void testReadOrcTypesFileByPosition() throws IOException {
        context.addOption("MAP_BY_POSITION", "true");
        runTestScenario_ReadOrcTypesFile(15);
    }

    @Test
    public void testReadOrcTypesFileWithColumnProjection() throws IOException {
        // Only project indexes 2, 5 and 12
        IntStream.range(0, columnDescriptors.size()).forEach(idx ->
                columnDescriptors
                        .get(idx)
                        .setProjected(idx == 2 || idx == 5 || idx == 12));

        runTestScenario_ReadOrcTypesFile(3);
    }

    @Test
    public void testReadOrcTypesFileByPositionWithColumnProjection() throws IOException {
        // Only project indexes 2, 5 and 12
        IntStream.range(0, columnDescriptors.size()).forEach(idx ->
                columnDescriptors
                        .get(idx)
                        .setProjected(idx == 2 || idx == 5 || idx == 12));

        context.addOption("MAP_BY_POSITION", "true");
        runTestScenario_ReadOrcTypesFile(3);
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

        runTestScenario_ReadOrcTypesFile(7);
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

        runTestScenario_ReadOrcTypesFile(6);
    }

    @Test
    public void testOpenForWrite_DefaultCompression_DefaultWriterTimezone() throws IOException {
        runTestScenario_OpenForWrite(CompressionKind.ZLIB, true);
    }
    @Test
    public void testOpenForWrite_NoCompression() throws IOException {
        context.addOption("COMPRESSION_CODEC", "NoNe");
        runTestScenario_OpenForWrite(CompressionKind.NONE, true);
    }

    @Test
    public void testOpenForWrite_SnappyCompression() throws IOException {
        context.addOption("COMPRESSION_CODEC", "snappy");
        runTestScenario_OpenForWrite(CompressionKind.SNAPPY, true);
    }

    @Test
    public void testOpenForWrite_WriteTimeZoneLocal() throws IOException {
        context.getConfiguration().set("pxf.write.timezone", "loCAl");
        runTestScenario_OpenForWrite(CompressionKind.ZLIB, false);
    }

    @Test
    public void testOpenForWrite_WriteTimeZoneUTC() throws IOException {
        context.getConfiguration().set("pxf.write.timezone", "uTc");
        runTestScenario_OpenForWrite(CompressionKind.ZLIB, true);
    }

    @Test
    public void testOpenForWrite_WriteTimeZoneInvalid() throws IOException {
        context.getConfiguration().set("pxf.write.timezone", "America/Los_Angeles");
        runErrorScenario_OpenForWrite("Invalid value 'America/Los_Angeles' for pxf.write.timezone property, only 'local' or 'UTC' values are allowed.");
    }

    @Test
    public void testOpenForWrite_WriteTimeZoneOrcLocal() throws IOException {
        context.getConfiguration().set("pxf.write.timezone.orc", "loCAl");
        runTestScenario_OpenForWrite(CompressionKind.ZLIB, false);
    }

    @Test
    public void testOpenForWrite_WriteTimeZoneOrcUTC() throws IOException {
        context.getConfiguration().set("pxf.write.timezone.orc", "uTc");
        runTestScenario_OpenForWrite(CompressionKind.ZLIB, true);
    }

    @Test
    public void testOpenForWrite_WriteTimeZoneOrcInvalid() throws IOException {
        context.getConfiguration().set("pxf.write.timezone.orc", "America/Los_Angeles");
        runErrorScenario_OpenForWrite("Invalid value 'America/Los_Angeles' for pxf.write.timezone.orc property, only 'local' or 'UTC' values are allowed.");
    }

    @Test
    public void testOpenForWrite_WriteTimeZoneTakesOrcFirst() throws IOException {
        context.getConfiguration().set("pxf.write.timezone", "uTc");
        context.getConfiguration().set("pxf.write.timezone.orc", "loCAl");
        runTestScenario_OpenForWrite(CompressionKind.ZLIB, false);
    }

    @Test
    public void testWriteNextObject() throws IOException {
        OneRow mockRow = mock(OneRow.class);
        VectorizedRowBatch mockBatch = mock(VectorizedRowBatch.class);
        when(mockRow.getData()).thenReturn(mockBatch);
        Writer mockWriter = mock(Writer.class);
        accessor.getWriterState().setFileWriter(mockWriter);

        boolean result = accessor.writeNextObject(mockRow);

        assertTrue(result);
        verify(mockWriter).addRowBatch(mockBatch);
        // verify we do not reset the batch in accessor, it should be done in resolver, where it is created
        verify(mockBatch, never()).reset();
    }

    @Test
    public void testCloseForWrite() throws IOException {
        accessor.getWriterState().setFileWriter(null);
        accessor.closeForWrite();
        // no specific assertions -- closeForWrite is a noop when writer is null

        Writer mockWriter = mock(Writer.class);
        accessor.getWriterState().setFileWriter(mockWriter);
        accessor.closeForWrite();
        verify(mockWriter).close();
    }

    private void runTestScenario_OpenForWrite(CompressionKind expectedCompression, boolean utcTimezone) throws IOException {
        File tempDirBase = FileUtils.getTempDirectory();
        File writeDir = new File(tempDirBase, "pxf_orc_write");
        if (writeDir.exists()) {
            FileUtils.deleteDirectory(writeDir);
        }

        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.setTransactionId("123");
        context.setSegmentId(5);
        context.getConfiguration().set(CONFIG_KEY_BASE_PATH, tempDirBase.getAbsolutePath());
        context.setDataSource("pxf_orc_write");
        context.setTupleDescription(twoColumnDescriptors);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();

        assertTrue(accessor.openForWrite());
        ORCVectorizedAccessor.WriterState writerState = accessor.getWriterState();
        assertEquals("file://" + tempDirBase.getAbsolutePath() + "/pxf_orc_write/123_5.orc", writerState.getFileName());
        // check writer options were properly constructed
        assertEquals(expectedCompression, writerState.getWriterOptions().getCompress());
        assertSame(context.getConfiguration(), writerState.getWriterOptions().getConfiguration());
        assertEquals(utcTimezone, writerState.getWriterOptions().getUseUTCTimestamp());
        // check write schema is correct and is set on the writer and in the context
        TypeDescription writeSchema = writerState.getWriterOptions().getSchema();
        assertEquals("struct<col0:string,col1:int>", writeSchema.toString());
        assertEquals(writeSchema, writerState.getFileWriter().getSchema());
        assertSame(writerState.getWriterOptions(), context.getMetadata());
    }

    private void runErrorScenario_OpenForWrite(String exceptionMessage) throws IOException {
        File tempDirBase = FileUtils.getTempDirectory();
        File writeDir = new File(tempDirBase, "pxf_orc_write");
        if (writeDir.exists()) {
            FileUtils.deleteDirectory(writeDir);
        }
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.getConfiguration().set(CONFIG_KEY_BASE_PATH, tempDirBase.getAbsolutePath());
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        Exception e = assertThrows(PxfRuntimeException.class, () -> accessor.openForWrite());
        assertEquals(exceptionMessage, e.getMessage());
    }

    private void runTestScenario_ReadOrcTypesFile(int expectedNumCols) throws IOException {
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