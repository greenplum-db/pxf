package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES;
import static org.greenplum.pxf.plugins.hive.utilities.HiveUtilities.toKryo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveAccessor.class, HiveDataFragmenter.class})
public class HiveAccessorTest {

    private static final String COLUMN_NAMES = "id,name,dec1";
    private static final String COLUMN_TYPES = "int:string:decimal(38,18)";

    @Mock InputFormat inputFormat;
    @Mock RecordReader<Object, Object> reader;

    RequestContext context;
    HiveAccessor accessor;
    Properties properties;
    List<ColumnDescriptor> columnDescriptors;

    @Before
    public void setup() throws Exception {
        properties = new Properties();
        properties.put("columns", COLUMN_NAMES);
        properties.put("columns.types", COLUMN_TYPES);

        PowerMockito.mockStatic(HiveDataFragmenter.class);
        when(HiveDataFragmenter.makeInputFormat(any(String.class), any(JobConf.class))).thenReturn(inputFormat);
        when(inputFormat.getRecordReader(any(InputSplit.class), any(JobConf.class), any(Reporter.class))).thenReturn(reader);

        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource("/foo/bar");

        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("name", DataType.TEXT.getOID(),  1, "", null));
        columnDescriptors.add(new ColumnDescriptor("dec1", DataType.NUMERIC.getOID(), 2, "", null, false));
        context.setTupleDescription(columnDescriptors);

        accessor = new HiveAccessor();
    }

    // ---------- skip header tests ----------
    @Test
    public void testSkipHeaderCountGreaterThanZero() throws Exception {
        properties.put("skip.header.line.count", "2");
        context.setFragmentUserData(toKryo(properties));

        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroFirstFragment() throws Exception {
        properties.put("skip.header.line.count", "2");
        context.setFragmentUserData(toKryo(properties));

        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroNotFirstFragment() throws Exception {
        properties.put("skip.header.line.count", "2");
        context.setFragmentIndex(2);
        context.setFragmentUserData(toKryo(properties));

        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountZeroFirstFragment() throws Exception {
        context.setFragmentUserData(toKryo(properties));

        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountNegativeFirstFragment() throws Exception {
        properties.put("skip.header.line.count", "-1");
        context.setFragmentUserData(toKryo(properties));

        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }

    // ---------- Column Projection Setup tests ----------
    @Test
    public void testColumnProjection() throws Exception {
        context.setFragmentUserData(toKryo(properties));

        accessor = new HiveAccessor();
        accessor.initialize(context);

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
        properties = new Properties();
        properties.put("columns", "name,dec1");
        properties.put("columns.types", "string:decimal(38,18)");
        properties.put(META_TABLE_PARTITION_COLUMNS, "id");
        properties.put(META_TABLE_PARTITION_COLUMN_TYPES, "int");
        properties.put("pxf.pcv", "1");
        context.setFragmentUserData(toKryo(properties));

        accessor = new HiveAccessor();
        accessor.initialize(context);

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
        context.setFragmentUserData(toKryo(properties));

        accessor = new HiveAccessor();
        accessor.initialize(context);

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
        context.setFragmentUserData(toKryo(properties));
        context.setFilterString("a0c20s1d1o5");

        accessor = new HiveAccessor();
        accessor.initialize(context);

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
        Map<String,String> additionalProps = new HashMap<>();
        additionalProps.put("pxf.ppd.hive", "false");
        context.setAdditionalConfigProps(additionalProps);
        context.setFragmentUserData(toKryo(properties));
        context.setFilterString("a0c20s1d1o5");

        accessor = new HiveAccessor();
        accessor.initialize(context);

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

}
