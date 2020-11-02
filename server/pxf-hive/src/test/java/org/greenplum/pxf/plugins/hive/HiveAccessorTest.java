package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

import static org.greenplum.pxf.plugins.hive.utilities.HiveUtilities.serializeProperties;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveAccessor.class, HiveDataFragmenter.class})
public class HiveAccessorTest {

    @Mock InputFormat inputFormat;
    @Mock RecordReader<Object, Object> reader;

    RequestContext context;
    HiveAccessor accessor;
    Properties properties;

    @Before
    public void setup() throws Exception {
        properties = new Properties();
        properties.put("columns", "");

        PowerMockito.mockStatic(HiveDataFragmenter.class);

        when(inputFormat.getRecordReader(any(InputSplit.class), any(JobConf.class), any(Reporter.class))).thenReturn(reader);

        context = new RequestContext();
        context.setAccessor(HiveORCAccessor.class.getName());
        context.setConfig("default");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setDataSource("/foo/bar");

        @SuppressWarnings("unchecked")
        OngoingStubbing ongoingStubbing = when(HiveDataFragmenter.makeInputFormat(any(String.class), any(JobConf.class))).thenReturn(inputFormat);
    }

    @Test
    public void testSkipHeaderCountGreaterThanZero() throws Exception {
        properties.put("skip.header.line.count", "2");
        context.setFragmentUserData(serializeProperties(properties));

        accessor = new HiveAccessor();
        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroFirstFragment() throws Exception {
        properties.put("skip.header.line.count", "2");
        context.setFragmentUserData(serializeProperties(properties));

        accessor = new HiveAccessor();
        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroNotFirstFragment() throws Exception {
        properties.put("skip.header.line.count", "2");
        context.setFragmentIndex(2);
        context.setFragmentUserData(serializeProperties(properties));

        accessor = new HiveAccessor();
        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountZeroFirstFragment() throws Exception {
        context.setFragmentUserData(serializeProperties(properties));

        accessor = new HiveAccessor();
        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountNegativeFirstFragment() throws Exception {
        properties.put("skip.header.line.count", "-1");
        context.setFragmentUserData(serializeProperties(properties));

        accessor = new HiveAccessor();
        accessor.initialize(context);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }
}
