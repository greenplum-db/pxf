package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HcfsTypeTest {

    private final static String S3_PROTOCOL = "s3";

    private RequestContext context;
    private Configuration configuration;

    @BeforeEach
    public void setUp() {
        configuration = new Configuration();
        context = new RequestContext();
        context.setDataSource("/foo/bar.txt");
        context.setConfiguration(configuration);
    }

    @Test
    public void testProtocolTakesPrecedenceOverFileDefaultFs() {
        // Test that we can specify protocol when configuration defaults are loaded
        context.setProfileScheme(S3_PROTOCOL);

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals(HcfsType.S3, type);
        assertEquals("s3://foo/bar.txt", type.getDataUri(context));
    }

    @Test
    public void testNonFileDefaultFsWhenProtocolIsNotSet() {
        configuration.set("fs.defaultFS", "adl://foo.azuredatalakestore.net");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals(HcfsType.ADL, type);
        assertEquals("adl://foo.azuredatalakestore.net/foo/bar.txt", type.getDataUri(context));
    }

    @Test
    public void testFileFormatFails() {
        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals(HcfsType.FILE, type);

        Exception e = assertThrows(IllegalStateException.class,
                () -> type.getDataUri(context));
        assertEquals("core-site.xml is missing or using unsupported file:// as default filesystem", e.getMessage());
    }

    @Test
    public void testCustomProtocolWithFileDefaultFs() {
        context.setProfileScheme("xyz");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals(HcfsType.CUSTOM, type);
        assertEquals("xyz://foo/bar.txt", type.getDataUri(context));
    }

    @Test
    public void testCustomDefaultFs() {
        configuration.set("fs.defaultFS", "xyz://0.0.0.0:80");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals(HcfsType.CUSTOM, type);
        assertEquals("xyz://0.0.0.0:80/foo/bar.txt", type.getDataUri(context));
    }

    @Test
    public void testFailsToGetTypeWhenDefaultFSIsSetWithoutColon() {
        configuration.set("fs.defaultFS", "/");
        Exception e = assertThrows(IllegalStateException.class,
                () -> HcfsType.getHcfsType(context));
        assertEquals("No scheme for property fs.defaultFS=/", e.getMessage());
    }

    @Test
    public void testDefaultFsWithTrailingSlashAndPathWithLeadingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("/foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar.txt", type.getDataUri(context));
    }

    @Test
    public void testDefaultFsWithTrailingSlashAndPathWithoutLeadingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar.txt", type.getDataUri(context));
    }

    @Test
    public void testDefaultFsWithoutTrailingSlashAndPathWithLeadingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("/foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar.txt", type.getDataUri(context));
    }

    @Test
    public void testDefaultFsWithoutTrailingSlashAndPathWithoutLeadingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar.txt", type.getDataUri(context));
    }

    @Test
    public void testAllowWritingToLocalFileSystemWithLOCALFILE() {
        context.setProfileScheme("localfile");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals(HcfsType.LOCALFILE, type);
        assertEquals("file:///foo/bar.txt", type.getDataUri(context));
        assertEquals("same", type.normalizeDataSource("same"));
    }

    @Test
    public void testErrorsWhenProfileAndDefaultFSDoNotMatch() {
        context.setProfileScheme("s3a");
        configuration.set("fs.defaultFS", "hdfs://0.0.0.0:8020");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> HcfsType.getHcfsType(context));
        assertEquals("profile protocol (s3a) is not compatible with server filesystem (hdfs)", e.getMessage());
    }

    @Test
    public void testUriForWrite() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(context));
    }

    @Test
    public void testUriForWriteWithUncompressedCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "uncompressed");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(context));
    }

    @Test
    public void testUriForWriteWithSnappyCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "snappy");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.snappy", type.getUriForWrite(context));
    }

    @Test
    public void testUriForWriteWithSnappyCodecSkip() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "snappy");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(context, true));
    }

    @Test
    public void testUriForWriteWithGZipCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "gzip");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.gz", type.getUriForWrite(context));
    }

    @Test
    public void testUriForWriteWithGZipCodecSkip() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "gzip");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(context, true));
    }

    @Test
    public void testUriForWriteWithLzoCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "lzo");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.lzo", type.getUriForWrite(context));
    }

    @Test
    public void testUriForWriteWithLzoCodecSkip() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "lzo");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(context, true));
    }

    @Test
    public void testUriForWriteWithTrailingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar/");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(context));
    }

    @Test
    public void testUriForWriteWithCodec() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar/");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(2);
        context.addOption("COMPRESSION_CODEC", "org.apache.hadoop.io.compress.GzipCodec");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_2.gz",
                type.getUriForWrite(context));
    }

    @Test
    public void testNonSecureNoConfigChangeOnNonHdfs() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getDataUri(context);
        assertEquals("xyz://abc/foo/bar.txt", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testNonSecureNoConfigChangeOnHdfs() {
        configuration.set("fs.defaultFS", "hdfs://abc:8020/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getDataUri(context);
        assertEquals("hdfs://abc:8020/foo/bar.txt", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testSecureNoConfigChangeOnHdfs() {
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("fs.defaultFS", "hdfs://abc:8020/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getDataUri(context);
        assertEquals("hdfs://abc:8020/foo/bar.txt", dataUri);
        assertNull(configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testSecureNoConfigChangeOnHdfsForWrite() {
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("fs.defaultFS", "hdfs://abc:8020");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getUriForWrite(context);
        assertEquals("hdfs://abc:8020/foo/bar/XID-XYZ-123456_3", dataUri);
        assertNull(configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testSecureConfigChangeOnNonHdfs() {
        configuration.set("fs.defaultFS", "s3a://abc/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getDataUri(context);
        assertEquals("s3a://abc/foo/bar.txt", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testSecureConfigChangeOnNonHdfsForWrite() {
        configuration.set("fs.defaultFS", "s3a://abc/");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getUriForWrite(context);
        assertEquals("s3a://abc/foo/bar/XID-XYZ-123456_3", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testFailureOnNonHdfsOnShortPath() {
        configuration.set("fs.defaultFS", "s3a://"); //bad URL without a scheme

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> HcfsType.getHcfsType(context));
        assertEquals("Expected authority at index 6: s3a://", e.getMessage());
    }

    @Test
    public void testSecureConfigChangeOnInvalidFilesystem() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getDataUri(context);
        assertEquals("xyz://abc/foo/bar.txt", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

}
