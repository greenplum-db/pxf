package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SerializationService;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HiveAccessorTest {

    private static final String COLUMN_NAMES = "id,name,dec1";
    private static final String COLUMN_TYPES = "int:string:decimal(38,18)";

    @Mock
    HiveUtilities mockHiveUtilities;

    @Mock
    @SuppressWarnings("raw")
    InputFormat mockInputFormat;

    @Mock
    RecordReader<Object, Object> mockReader;

    Configuration configuration;
    RequestContext context;
    HiveAccessor accessor;
    Properties properties;
    SerializationService serializationService;
    List<ColumnDescriptor> columnDescriptors;

    @BeforeEach
    public void setup() {

        properties = new Properties();
        properties.put("columns", COLUMN_NAMES);
        properties.put("columns.types", COLUMN_TYPES);
        properties.put("file.inputformat", "org.apache.hadoop.mapred.TextInputFormat");

        configuration = new Configuration();
        configuration.set("pxf.fs.basePath", "/");

        String path = Objects.requireNonNull(getClass().getClassLoader().getResource("parquet_types.parquet")).getPath();

        context = new RequestContext();
        context.setAccessor(HiveORCAccessor.class.getName());
        context.setConfig("default");
        context.setUser("test-user");
        context.setDataSource(path);
        context.setConfiguration(configuration);

        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("name", DataType.TEXT.getOID(), 1, "", null));
        columnDescriptors.add(new ColumnDescriptor("dec1", DataType.NUMERIC.getOID(), 2, "", null, false));
        context.setTupleDescription(columnDescriptors);

        serializationService = new SerializationService();
    }

    // ---------- skip header tests ----------
    @Test
    public void testSkipHeaderCountGreaterThanZero() throws Exception {
        prepareReaderMocks();

        properties.put("skip.header.line.count", "2");
        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor(null, mockHiveUtilities, serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroFirstFragment() throws Exception {
        prepareReaderMocks();

        properties.put("skip.header.line.count", "2");
        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentIndex(0);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor(null, mockHiveUtilities, serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroNotFirstFragment() throws Exception {
        prepareReaderMocks();

        properties.put("skip.header.line.count", "2");
        context.setFragmentIndex(2);
        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentIndex(2);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor(null, mockHiveUtilities, serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountZeroFirstFragment() throws Exception {
        prepareReaderMocks();

        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentIndex(0);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor(null, mockHiveUtilities, serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountNegativeFirstFragment() throws Exception {
        prepareReaderMocks();

        properties.put("skip.header.line.count", "-1");
        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentIndex(0);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor(null, mockHiveUtilities, serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(1)).next(any(), any());
    }

    // ---------- Column Projection Setup tests ----------
    @Test
    public void testColumnProjection() throws Exception {
        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor(null, new HiveUtilities(), serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();

        JobConf jobConf = accessor.getJobConf();
        assertNull(jobConf.get("columns"));
        assertNull(jobConf.get("columns.types"));

        accessor.openForRead();
        assertEquals("false", jobConf.get("hive.io.file.read.all.columns"));
        assertEquals("0,1", jobConf.get("hive.io.file.readcolumn.ids"));
        assertEquals("id,name", jobConf.get("hive.io.file.readcolumn.names"));
        assertEquals(COLUMN_NAMES, jobConf.get("columns"));
        assertEquals(COLUMN_TYPES, jobConf.get("columns.types"));
    }

    @Test
    public void testColumnProjectionWithPartitionColumns() throws Exception {
        properties.put("columns", "name,dec1");
        properties.put("columns.types", "string:decimal(38,18)");
        properties.put(META_TABLE_PARTITION_COLUMNS, "id");
        properties.put(META_TABLE_PARTITION_COLUMN_TYPES, "int");
        properties.put("pxf.pcv", "1");
        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor(null, new HiveUtilities(), serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();

        JobConf jobConf = accessor.getJobConf();
        assertNull(jobConf.get("columns"));
        assertNull(jobConf.get("columns.types"));

        accessor.openForRead();
        assertEquals("false", jobConf.get("hive.io.file.read.all.columns"));
        assertEquals("0", jobConf.get("hive.io.file.readcolumn.ids"));
        assertEquals("name", jobConf.get("hive.io.file.readcolumn.names"));
        assertEquals("name,dec1", jobConf.get("columns"));
        assertEquals("string:decimal(38,18)", jobConf.get("columns.types"));
    }

    // ---------- Predicate Pushdown Setup tests ----------
    @Test
    public void testPPDEnabledNoFilter() throws Exception {
        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor(null, new HiveUtilities(), serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();

        JobConf jobConf = accessor.getJobConf();
        assertEquals("false", jobConf.get("hive.parquet.timestamp.skip.conversion"));
        assertNull(jobConf.get("columns"));
        assertNull(jobConf.get("columns.types"));

        accessor.openForRead();
        assertEquals(COLUMN_NAMES, jobConf.get("columns"));
        assertEquals(COLUMN_TYPES, jobConf.get("columns.types"));
        assertNull(jobConf.get("sarg.pushdown"));
    }

    @Test
    public void testPPDEnabledWithFilter() throws Exception {
        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentMetadata(metadata);
        context.setFilterString("a0c20s1d1o5");

        accessor = new HiveAccessor(null, new HiveUtilities(), serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();

        JobConf jobConf = accessor.getJobConf();
        assertEquals("false", jobConf.get("hive.parquet.timestamp.skip.conversion"));
        assertNull(jobConf.get("columns"));
        assertNull(jobConf.get("columns.types"));

        accessor.openForRead();
        assertEquals(COLUMN_NAMES, jobConf.get("columns"));
        assertEquals(COLUMN_TYPES, jobConf.get("columns.types"));
        assertNotNull(jobConf.get("sarg.pushdown"));
    }

    @Test
    public void testPPDDisabledWithFilter() throws Exception {
        configuration.set("pxf.ppd.hive", "false");
        HiveFragmentMetadata metadata = new HiveFragmentMetadata(0, 0, properties);
        context.setFragmentMetadata(metadata);
        context.setFilterString("a0c20s1d1o5");

        accessor = new HiveAccessor(null, new HiveUtilities(), serializationService);
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();

        JobConf jobConf = accessor.getJobConf();
        assertNull(jobConf.get("hive.parquet.timestamp.skip.conversion"));
        assertNull(jobConf.get("columns"));
        assertNull(jobConf.get("columns.types"));

        accessor.openForRead();
        assertNull(jobConf.get("hive.parquet.timestamp.skip.conversion"));
        assertEquals(COLUMN_NAMES, jobConf.get("columns"));
        assertEquals(COLUMN_TYPES, jobConf.get("columns.types"));
        assertNull(jobConf.get("sarg.pushdown"));
    }

    @Test
    public void testWriteIsNotSupported() {
        accessor = new HiveAccessor(null, new HiveUtilities(), serializationService);

        Exception e = assertThrows(UnsupportedOperationException.class, () -> accessor.openForWrite());
        assertEquals("Hive accessor does not support write operation.", e.getMessage());

        e = assertThrows(UnsupportedOperationException.class, () -> accessor.writeNextObject(null));
        assertEquals("Hive accessor does not support write operation.", e.getMessage());

        e = assertThrows(UnsupportedOperationException.class, () -> accessor.closeForWrite());
        assertEquals("Hive accessor does not support write operation.", e.getMessage());
    }

    @Test
    public void testWriteWithRequestContextAsWrite(){
        accessor = new HiveAccessor(null, new HiveUtilities(), serializationService);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        accessor.setRequestContext(context);
        Exception e = assertThrows(UnsupportedOperationException.class, () -> accessor.afterPropertiesSet());
        assertEquals("Hive accessor does not support write operation.", e.getMessage());
    }

    @SuppressWarnings("unchecked")
    private void prepareReaderMocks() throws Exception {
        when(mockHiveUtilities.makeInputFormat(any(), any())).thenReturn(mockInputFormat);
        when(mockInputFormat.getRecordReader(any(InputSplit.class), any(JobConf.class), any(Reporter.class))).thenReturn(mockReader);
    }

}
