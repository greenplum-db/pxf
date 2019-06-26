package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CodecFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void getCodecNoName() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Compression codec some.bad.codec was not found.");

        Configuration conf = new Configuration();
        String name = "some.bad.codec";
        new CodecFactory().getCodec(conf, name);
    }

    @Test
    public void getCodecNoConf() {
        thrown.expect(NullPointerException.class);

        String name = "org.apache.hadoop.io.compress.GzipCodec";
        new CodecFactory().getCodec(null, name);
    }

    @Test
    public void getCodecGzip() {
        Configuration conf = new Configuration();
        String name = "org.apache.hadoop.io.compress.GzipCodec";

        CompressionCodec codec = new CodecFactory().getCodec(conf, name);
        assertNotNull(codec);
        assertEquals(".gz", codec.getDefaultExtension());
    }
}