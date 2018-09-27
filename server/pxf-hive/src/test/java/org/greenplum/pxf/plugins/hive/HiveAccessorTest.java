package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveAccessor.class, HiveUtilities.class, HdfsUtilities.class, HiveDataFragmenter.class})
public class HiveAccessorTest {

    @Mock
    InputData inputData;
    @Mock
    InputFormat inputFormat;
    @Mock
    RecordReader<Object, Object> reader;

    HiveAccessor accessor;
    HiveUserData.Builder userDataBuilder;

    @Before
    public void setup() throws Exception {
        userDataBuilder = new HiveUserData.Builder()
                .withSerdeClassName("org.apache.hadoop.mapred.TextInputFormat")
                .withPartitionKeys(HiveDataFragmenter.HIVE_NO_PART_TBL);

        PowerMockito.mockStatic(HiveUtilities.class);
        PowerMockito.mockStatic(HdfsUtilities.class);

        PowerMockito.mockStatic(HiveDataFragmenter.class);
        PowerMockito.when(HiveDataFragmenter.makeInputFormat(any(String.class), any(JobConf.class))).thenReturn(inputFormat);

        when(inputFormat.getRecordReader(any(InputSplit.class), any(JobConf.class), any(Reporter.class))).thenReturn(reader);
        PowerMockito.when(inputData.getAccessor()).thenReturn(HiveORCAccessor.class.getName());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZero() throws Exception {
        HiveUserData userData = userDataBuilder.withSkipHeader(2).build();

        PowerMockito.when(HiveUtilities.parseHiveUserData(any(InputData.class))).thenReturn(userData);
        when(inputData.hasFilter()).thenReturn(false);

        accessor = new HiveAccessor(inputData);

        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroFirstFragment() throws Exception {
        HiveUserData userData = userDataBuilder.withSkipHeader(2).build();
        PowerMockito.when(HiveUtilities.parseHiveUserData(any(InputData.class))).thenReturn(userData);
        when(inputData.hasFilter()).thenReturn(false);
        when(inputData.getFragmentIndex()).thenReturn(0);

        accessor = new HiveAccessor(inputData);

        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroNotFirstFragment() throws Exception {
        HiveUserData userData = userDataBuilder.withSkipHeader(2).build();
        PowerMockito.when(HiveUtilities.parseHiveUserData(any(InputData.class))).thenReturn(userData);
        when(inputData.hasFilter()).thenReturn(false);
        when(inputData.getFragmentIndex()).thenReturn(2);

        accessor = new HiveAccessor(inputData);

        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountZeroFirstFragment() throws Exception {
        HiveUserData userData = userDataBuilder.withSkipHeader(0).build();
        PowerMockito.when(HiveUtilities.parseHiveUserData(any(InputData.class))).thenReturn(userData);
        when(inputData.hasFilter()).thenReturn(false);
        when(inputData.getFragmentIndex()).thenReturn(0);

        accessor = new HiveAccessor(inputData);

        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }
}