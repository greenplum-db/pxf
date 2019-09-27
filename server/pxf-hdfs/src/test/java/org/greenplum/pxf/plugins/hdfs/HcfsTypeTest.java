package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HcfsTypeTest {

    private final static String S3_PROTOCOL = "s3";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private RequestContext context;
    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        context = new RequestContext();
        context.setDataSource("/foo/bar.txt");
        configuration = new Configuration();
    }

    @Test
    public void testProtocolTakesPrecedenceOverFileDefaultFs() {
        // Test that we can specify protocol when configuration defaults are loaded
        context.setProfileScheme(S3_PROTOCOL);

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals(HcfsType.S3, type);
        assertEquals("s3://foo/bar.txt", type.getDataUri(configuration, context));
    }

    @Test
    public void testNonFileDefaultFsWhenProtocolIsNotSet() {
        configuration.set("fs.defaultFS", "adl://foo.azuredatalakestore.net");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals(HcfsType.ADL, type);
        assertEquals("adl://foo.azuredatalakestore.net/foo/bar.txt", type.getDataUri(configuration, context));
    }

    @Test
    public void testFileFormatFails() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("core-site.xml is missing or using unsupported file:// as default filesystem");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals(HcfsType.FILE, type);
        type.getDataUri(configuration, context);
    }

    @Test
    public void testCustomProtocolWithFileDefaultFs() {
        context.setProfileScheme("xyz");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals(HcfsType.CUSTOM, type);
        assertEquals("xyz://foo/bar.txt", type.getDataUri(configuration, context));
    }

    @Test
    public void testCustomDefaultFs() {
        configuration.set("fs.defaultFS", "xyz://0.0.0.0:80");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals(HcfsType.CUSTOM, type);
        assertEquals("xyz://0.0.0.0:80/foo/bar.txt", type.getDataUri(configuration, context));
    }

    @Test
    public void testFailsToGetTypeWhenDefaultFSIsSetWithoutColon() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("No scheme for property fs.defaultFS=/");

        configuration.set("fs.defaultFS", "/");
        HcfsType.getHcfsType(configuration, context);
    }

    @Test
    public void testDefaultFsWithTrailingSlashAndPathWithLeadingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("/foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar.txt", type.getDataUri(configuration, context));
    }

    @Test
    public void testDefaultFsWithTrailingSlashAndPathWithoutLeadingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar.txt", type.getDataUri(configuration, context));
    }

    @Test
    public void testDefaultFsWithoutTrailingSlashAndPathWithLeadingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("/foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar.txt", type.getDataUri(configuration, context));
    }

    @Test
    public void testDefaultFsWithoutTrailingSlashAndPathWithoutLeadingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar.txt", type.getDataUri(configuration, context));
    }

    @Test
    public void testAllowWritingToLocalFileSystemWithLOCALFILE() {
        context.setProfileScheme("localfile");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals(HcfsType.LOCALFILE, type);
        assertEquals("file:///foo/bar.txt", type.getDataUri(configuration, context));
        assertEquals("same", type.normalizeDataSource("same"));
    }

    @Test
    public void testErrorsWhenProfileAndDefaultFSDoNotMatch() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("profile protocol (s3a) is not compatible with server filesystem (hdfs)");

        context.setProfileScheme("s3a");
        configuration.set("fs.defaultFS", "hdfs://0.0.0.0:8020");
        HcfsType.getHcfsType(configuration, context);
    }

    @Test
    public void testUriForWrite() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(configuration, context));
    }

    @Test
    public void testUriForWriteWithUncompressedCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "uncompressed");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(configuration, context));
    }

    @Test
    public void testUriForWriteWithSnappyCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "snappy");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.snappy", type.getUriForWrite(configuration, context));
    }

    @Test
    public void testUriForWriteWithSnappyCodecSkip() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "snappy");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(configuration, context, true));
    }

    @Test
    public void testUriForWriteWithGZipCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "gzip");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.gz", type.getUriForWrite(configuration, context));
    }

    @Test
    public void testUriForWriteWithGZipCodecSkip() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "gzip");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(configuration, context, true));
    }

    @Test
    public void testUriForWriteWithLzoCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "lzo");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.lzo", type.getUriForWrite(configuration, context));
    }

    @Test
    public void testUriForWriteWithLzoCodecSkip() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "lzo");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(configuration, context, true));
    }

    @Test
    public void testUriForWriteWithTrailingSlash() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar/");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(configuration, context));
    }

    @Test
    public void testUriForWriteWithCodec() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar/");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(2);
        context.addOption("COMPRESSION_CODEC", "org.apache.hadoop.io.compress.GzipCodec");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_2.gz",
                type.getUriForWrite(configuration, context));
    }

    @Test
    public void testNonSecureNoConfigChangeOnNonHdfs() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        String dataUri = type.getDataUri(configuration, context);
        assertEquals("xyz://abc/foo/bar.txt", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testNonSecureNoConfigChangeOnHdfs() {
        configuration.set("fs.defaultFS", "hdfs://abc:8020/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        String dataUri = type.getDataUri(configuration, context);
        assertEquals("hdfs://abc:8020/foo/bar.txt", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testSecureNoConfigChangeOnHdfs() {
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("fs.defaultFS", "hdfs://abc:8020/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        String dataUri = type.getDataUri(configuration, context);
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

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        String dataUri = type.getUriForWrite(configuration, context);
        assertEquals("hdfs://abc:8020/foo/bar/XID-XYZ-123456_3", dataUri);
        assertNull(configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testSecureConfigChangeOnNonHdfs() {
        configuration.set("fs.defaultFS", "s3a://abc/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        String dataUri = type.getDataUri(configuration, context);
        assertEquals("s3a://abc/foo/bar.txt", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testSecureConfigChangeOnNonHdfsForWrite() {
        configuration.set("fs.defaultFS", "s3a://abc/");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        String dataUri = type.getUriForWrite(configuration, context);
        assertEquals("s3a://abc/foo/bar/XID-XYZ-123456_3", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testFailureOnNonHdfsOnShortPath() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Expected authority at index 6: s3a://");

        configuration.set("fs.defaultFS", "s3a://"); //bad URL without a scheme
        HcfsType.getHcfsType(configuration, context);
    }

    @Test
    public void testSecureConfigChangeOnInvalidFilesystem() {
        configuration.set("fs.defaultFS", "xyz://abc/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        String dataUri = type.getDataUri(configuration, context);
        assertEquals("xyz://abc/foo/bar.txt", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

}
