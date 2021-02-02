package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CodecFactoryTest {

    private CodecFactory factory;

    @BeforeEach
    public void setup() {
        factory = new CodecFactory();
    }

    @Test
    public void getCodecNoName() {

        Configuration conf = new Configuration();
        String name = "some.bad.codec";
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> factory.getCodec(name, conf));
        assertEquals("Compression codec some.bad.codec was not found.", e.getMessage());
    }

    @Test
    public void getCodecNoConf() {
        Configuration configuration = null;

        String name = "org.apache.hadoop.io.compress.GzipCodec";
        assertThrows(NullPointerException.class,
                () -> factory.getCodec(name, configuration));
    }

    @Test
    public void getCodecGzip() {
        Configuration conf = new Configuration();
        String name = "org.apache.hadoop.io.compress.GzipCodec";

        CompressionCodec codec = factory.getCodec(name, conf);
        assertNotNull(codec);
        assertEquals(".gz", codec.getDefaultExtension());
    }

    @Test
    public void getCodecGzipShortName() {
        Configuration conf = new Configuration();
        String name = "gzip";

        CompressionCodec codec = factory.getCodec(name, conf);
        assertNotNull(codec);
        assertEquals(".gz", codec.getDefaultExtension());
    }

    @Test
    public void getCodeDefaultShortName() {
        Configuration conf = new Configuration();
        String name = "default";

        CompressionCodec codec = factory.getCodec(name, conf);
        assertNotNull(codec);
        assertEquals(".deflate", codec.getDefaultExtension());
    }

    @Test
    public void getCodeSnappyShortName() {
        Configuration conf = new Configuration();
        String name = "snappy";

        CompressionCodec codec = factory.getCodec(name, conf);
        assertNotNull(codec);
        assertEquals(".snappy", codec.getDefaultExtension());
    }
}
