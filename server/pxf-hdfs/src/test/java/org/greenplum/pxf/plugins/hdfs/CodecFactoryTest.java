package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CodecFactoryTest {

    private CodecFactory factory;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        factory = new CodecFactory();
    }

    @Test
    public void getCodecNoName() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Compression codec some.bad.codec was not found.");

        Configuration conf = new Configuration();
        String name = "some.bad.codec";
        factory.getCodec(name, conf);
    }

    @Test
    public void getCodecNoConf() {
        thrown.expect(NullPointerException.class);

        Configuration configuration = null;

        String name = "org.apache.hadoop.io.compress.GzipCodec";
        factory.getCodec(name, configuration);
    }

    @Test
    public void getCodecGzip() {
        Configuration conf = new Configuration();
        String name = "org.apache.hadoop.io.compress.GzipCodec";

        CompressionCodec codec = factory.getCodec(name, conf);
        assertNotNull(codec);
        assertEquals(".gz", codec.getDefaultExtension());
    }
}