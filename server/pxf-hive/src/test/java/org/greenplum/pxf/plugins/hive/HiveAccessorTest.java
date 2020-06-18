package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HiveAccessorTest {

    @Mock
    private HiveUtilities mockHiveUtilities;

    @Mock
    @SuppressWarnings("raw")
    private InputFormat mockInputFormat;

    @Mock
    private RecordReader<Object, Object> mockReader;

    private RequestContext context;
    private HiveAccessor accessor;
    private HiveFragmentMetadata.Builder userDataBuilder;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        userDataBuilder = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withSerdeClassName("org.apache.hadoop.mapred.TextInputFormat")
                .withPartitionKeys(HiveDataFragmenter.HIVE_NO_PART_TBL);

        when(mockInputFormat.getRecordReader(any(InputSplit.class), any(JobConf.class), any(Reporter.class))).thenReturn(mockReader);
        when(mockHiveUtilities.makeInputFormat(any(), any())).thenReturn(mockInputFormat);

        context = new RequestContext();
        context.setAccessor(HiveORCAccessor.class.getName());
        context.setConfig("default");
        context.setUser("test-user");
        context.setDataSource("/foo/bar");
        context.setConfiguration(new Configuration());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZero() throws Exception {
        HiveFragmentMetadata metadata = userDataBuilder.withSkipHeader(2).build();
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor();
        accessor.setRequestContext(context);
        accessor.setHiveUtilities(mockHiveUtilities);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroFirstFragment() throws Exception {
        HiveFragmentMetadata metadata = userDataBuilder.withSkipHeader(2).build();
        context.setFragmentIndex(0);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor();
        accessor.setRequestContext(context);
        accessor.setHiveUtilities(mockHiveUtilities);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroNotFirstFragment() throws Exception {
        HiveFragmentMetadata metadata = userDataBuilder.withSkipHeader(2).build();
        context.setFragmentIndex(2);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor();
        accessor.setRequestContext(context);
        accessor.setHiveUtilities(mockHiveUtilities);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountZeroFirstFragment() throws Exception {
        HiveFragmentMetadata metadata = userDataBuilder.withSkipHeader(0).build();
        context.setFragmentIndex(0);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor();
        accessor.setRequestContext(context);
        accessor.setHiveUtilities(mockHiveUtilities);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountNegativeFirstFragment() throws Exception {
        HiveFragmentMetadata metadata = userDataBuilder.withSkipHeader(-1).build();
        context.setFragmentIndex(0);
        context.setFragmentMetadata(metadata);

        accessor = new HiveAccessor();
        accessor.setRequestContext(context);
        accessor.setHiveUtilities(mockHiveUtilities);
        accessor.afterPropertiesSet();
        accessor.openForRead();
        accessor.readNextObject();

        verify(mockReader, times(1)).next(any(), any());
    }
}
