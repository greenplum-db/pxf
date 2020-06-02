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
    public void isThreadSafe() {

        testIsThreadSafe(
                "Parquet readable compression, no compression - thread safe",
                "/some/path/without.compression",
                null,
                true);

        testIsThreadSafe(
                "Parquet readable compression, gzip compression - thread safe",
                "/some/compressed/path.gz",
                null,
                true);

        testIsThreadSafe(
                "Parquet readable compression, bzip2 compression - not thread safe",
                "/some/path/with/bzip2.bz2",
                null,
                false);

        testIsThreadSafe(
                "Parquet writable compression, no compression codec - thread safe",
                "/some/path",
                null,
                true);

        testIsThreadSafe(
                "Parquet writable compression, compression codec bzip2 - not thread safe",
                "/some/path",
                "org.apache.hadoop.io.compress.BZip2Codec",
                false);

        testIsThreadSafe(
                "Avro writable compression, compression codec bzip2 - not thread safe",
                "/some/path",
                "bzip2",
                false);

        testIsThreadSafe(
                "Avro writable compression, compression codec deflate - thread safe",
                "/some/path",
                "deflate",
                true);

        testIsThreadSafe(
                "Avro writable compression, compression codec null - thread safe",
                "/some/path",
                "uncompressed",
                true);

        testIsThreadSafe(
                "Avro writable compression, compression codec snappy - thread safe",
                "/some/path",
                "snappy",
                true);

        testIsThreadSafe(
                "Avro writable compression, compression codec xz - thread safe",
                "/some/path",
                "xz",
                true);
    }

    private void testIsThreadSafe(String testDescription, String path, String codecStr, boolean expectedResult) {
        assertEquals(testDescription, expectedResult, factory.isCodecThreadSafe(codecStr, path, new Configuration()));
    }
}
