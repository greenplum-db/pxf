package org.greenplum.pxf.plugins.hdfs;

import io.airlift.compress.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.greenplum.pxf.api.error.PxfRuntimeException;
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
        configuration.set("pxf.config.server.name", "awesome_server");
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
        configuration.set("fs.defaultFS", "abfss://foo.azuredatalakestore.net");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals(HcfsType.ABFSS, type);
        assertEquals("abfss://foo.azuredatalakestore.net/foo/bar.txt", type.getDataUri(context));
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
    public void testAllowWritingToLocalFileSystemWithFile() {
        configuration.set("pxf.fs.basePath", "/");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals(HcfsType.FILE, type);
        assertEquals("file:///foo/bar.txt", type.getDataUri(context));
        assertEquals("same", type.validateAndNormalizeDataSource("same"));
    }

    @Test
    public void testErrorsWhenProfileAndDefaultFSDoNotMatch() {
        context.setProfileScheme("s3a");
        configuration.set("fs.defaultFS", "hdfs://0.0.0.0:8020");
        configuration.set("pxf.config.server.directory", "/my/config/directory");
        PxfRuntimeException e = assertThrows(PxfRuntimeException.class,
                () -> HcfsType.getHcfsType(context));
        assertEquals("profile 's3a' is not compatible with server's 'default' configuration ('hdfs')", e.getMessage());
        assertEquals("Ensure that '/my/config/directory' includes only the configuration files for profile 's3a'.", e.getHint());
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

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.snappy", type.getUriForWrite(context, null, new SnappyCodec()));
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.foo.snappy", type.getUriForWrite(context, "foo", new SnappyCodec()));
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.foo.snappy", type.getUriForWrite(context, ".foo", new SnappyCodec()));
    }

    @Test
    public void testUriForWriteWithSnappyCodecSkip() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "snappy");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(context));
    }

    @Test
    public void testUriForWriteWithGZipCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.gz", type.getUriForWrite(context, null, new GzipCodec()));
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.foo.gz", type.getUriForWrite(context, "foo", new GzipCodec()));
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.foo.gz", type.getUriForWrite(context, ".foo", new GzipCodec()));
    }

    @Test
    public void testUriForWriteWithLzoCodec() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.lzo", type.getUriForWrite(context, null, new LzopCodec()));
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.foo.lzo", type.getUriForWrite(context, "foo", new LzopCodec()));
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3.foo.lzo", type.getUriForWrite(context, ".foo", new LzopCodec()));
    }

    @Test
    public void testUriForWriteWithLzoCodecSkip() {
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);
        context.addOption("COMPRESSION_CODEC", "lzo");

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(context, null, null));
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

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_2.gz", type.getUriForWrite(context, null, new GzipCodec()));
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_2.foo.gz", type.getUriForWrite(context, "foo", new GzipCodec()));
        assertEquals("xyz://abc/foo/bar/XID-XYZ-123456_2.foo.gz", type.getUriForWrite(context, ".foo", new GzipCodec()));
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

    @Test
    public void testWhitespaceInDataSource() {
        configuration.set("fs.defaultFS", "s3a://abc/");
        context.setDataSource("foo/bar 1.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getDataUri(context);
        assertEquals("s3a://abc/foo/bar 1.txt", dataUri);
        assertEquals("abc", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testHcfsGlobPattern() {
        configuration.set("fs.defaultFS", "hdfs://0.0.0.0:8020");
        context.setDataSource("/tmp/issues/172848577/[a-b].csv");

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getDataUri(context);
        assertEquals("hdfs://0.0.0.0:8020/tmp/issues/172848577/[a-b].csv", dataUri);
        assertEquals("0.0.0.0", configuration.get(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE));
    }

    @Test
    public void testFailureOnFileWhenBasePathIsNotConfigured() {
        PxfRuntimeException e = assertThrows(PxfRuntimeException.class,
                () -> HcfsType.getHcfsType(context));
        assertEquals("invalid configuration for server 'awesome_server'", e.getMessage());
        assertEquals("Configure a valid value for 'pxf.fs.basePath' property for server 'awesome_server' to access the filesystem.", e.getHint());
    }

    @Test
    public void testFailureOnFileWhenInvalidDefaultFSIsProvided() {

        configuration.set("fs.defaultFS", "s3a://abc/");
        context.setProfileScheme("file");
        Exception e = assertThrows(PxfRuntimeException.class,
                () -> HcfsType.getHcfsType(context));
        assertEquals("profile 'file' is not compatible with server's 'default' configuration ('s3a')", e.getMessage());
    }

    @Test
    public void testBasePathIsConfiguredToRootDirectory() {
        configuration.set("pxf.fs.basePath", "/");
        HcfsType file = HcfsType.getHcfsType(context);
        String uri = file.getDataUri(context);
        assertEquals("file:///foo/bar.txt", uri);
    }

    @Test
    public void testBasePathIsConfiguredToAFixedBucket() {
        configuration.set("pxf.fs.basePath", "some-bucket");
        context.setProfileScheme("s3a");
        HcfsType file = HcfsType.getHcfsType(context);
        String uri = file.getDataUri(context);
        assertEquals("s3a://some-bucket/foo/bar.txt", uri);
    }

    @Test
    public void testBasePathIsConfiguredToSomeValueForCustomFS() {
        configuration.set("pxf.fs.basePath", "private");
        configuration.set("fs.defaultFS", "xyz://abc");
        context.setDataSource("foo/bar");
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(3);

        HcfsType type = HcfsType.getHcfsType(context);
        assertEquals("xyz://abc/private/foo/bar/XID-XYZ-123456_3", type.getUriForWrite(context));
    }

    @Test
    public void testBasePathIsConfiguredToASingleCharPath() {
        configuration.set("pxf.fs.basePath", "p");
        HcfsType file = HcfsType.getHcfsType(context);
        String uri = file.getDataUri(context);
        assertEquals("file:///p/foo/bar.txt", uri);

        // trailing / in basePath
        configuration.set("pxf.fs.basePath", "p/");
        file = HcfsType.getHcfsType(context);
        uri = file.getDataUri(context);
        assertEquals("file:///p/foo/bar.txt", uri);

        // preceding / in basePath
        configuration.set("pxf.fs.basePath", "/p");
        file = HcfsType.getHcfsType(context);
        uri = file.getDataUri(context);
        assertEquals("file:///p/foo/bar.txt", uri);

        // trailing and preceding / in basePath
        configuration.set("pxf.fs.basePath", "/p/");
        file = HcfsType.getHcfsType(context);
        uri = file.getDataUri(context);
        assertEquals("file:///p/foo/bar.txt", uri);
    }

    @Test
    public void testBasePathIsConfiguredToSomeValue() {
        configuration.set("pxf.fs.basePath", "my/base/path");
        HcfsType file = HcfsType.getHcfsType(context);
        String uri = file.getDataUri(context);
        assertEquals("file:///my/base/path/foo/bar.txt", uri);

        // trailing / in basePath
        configuration.set("pxf.fs.basePath", "my/base/path/");
        uri = file.getDataUri(context);
        assertEquals("file:///my/base/path/foo/bar.txt", uri);

        // preceding / in basePath
        configuration.set("pxf.fs.basePath", "/my/base/path");
        uri = file.getDataUri(context);
        assertEquals("file:///my/base/path/foo/bar.txt", uri);

        // trailing and preceding / in basePath
        configuration.set("pxf.fs.basePath", "/my/base/path/");
        uri = file.getDataUri(context);
        assertEquals("file:///my/base/path/foo/bar.txt", uri);
    }

    @Test
    public void testFailsWhenARelativeDataSourceIsProvided1() {

        configuration.set("pxf.fs.basePath", "/some/base/path");
        context.setDataSource("../../../etc/passwd");
        HcfsType file = HcfsType.getHcfsType(context);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> file.getDataUri(context));
        assertEquals("the provided path '../../../etc/passwd' is invalid. Relative paths are not allowed by PXF", e.getMessage());
    }

    @Test
    public void testFailsWhenARelativeDataSourceIsProvided2() {

        configuration.set("pxf.fs.basePath", "/some/base/path");
        context.setDataSource("..");
        HcfsType file = HcfsType.getHcfsType(context);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> file.getDataUri(context));
        assertEquals("the provided path '..' is invalid. Relative paths are not allowed by PXF", e.getMessage());
    }

    @Test
    public void testFailsWhenARelativeDataSourceIsProvided3() {

        configuration.set("pxf.fs.basePath", "/some/base/path");
        context.setDataSource("dir1/../dir2");
        HcfsType file = HcfsType.getHcfsType(context);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> file.getDataUri(context));
        assertEquals("the provided path 'dir1/../dir2' is invalid. Relative paths are not allowed by PXF", e.getMessage());
    }

    @Test
    public void testFailsWhenARelativeDataSourceIsProvided4() {

        configuration.set("pxf.fs.basePath", "/some/base/path");
        context.setDataSource("../");
        HcfsType file = HcfsType.getHcfsType(context);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> file.getDataUri(context));
        assertEquals("the provided path '../' is invalid. Relative paths are not allowed by PXF", e.getMessage());
    }

    @Test
    public void testFailsWhenARelativeDataSourceIsProvided5() {

        configuration.set("pxf.fs.basePath", "/some/base/path");
        context.setDataSource("a/..");
        HcfsType file = HcfsType.getHcfsType(context);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> file.getDataUri(context));
        assertEquals("the provided path 'a/..' is invalid. Relative paths are not allowed by PXF", e.getMessage());
    }

    @Test
    public void testFailsWhenARelativeDataSourceIsProvidedForWrite() {

        configuration.set("pxf.fs.basePath", "/some/base/path");
        context.setDataSource("../../../etc/passwd");
        HcfsType file = HcfsType.getHcfsType(context);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> file.getUriForWrite(context));
        assertEquals("the provided path '../../../etc/passwd' is invalid. Relative paths are not allowed by PXF", e.getMessage());
    }

    @Test
    public void testDataSourceWithTwoDotsInName() {
        configuration.set("pxf.fs.basePath", "/some/base/path");
        context.setDataSource("a..txt");
        HcfsType file = HcfsType.getHcfsType(context);
        String uri = file.getDataUri(context);
        assertEquals("file:///some/base/path/a..txt", uri);
    }

    @Test
    public void testFailsWhenADollarSignInDataSourceIsProvided() {

        configuration.set("pxf.fs.basePath", "/");
        context.setDataSource("$HOME/secret-files-in-gpadmin-home");
        HcfsType file = HcfsType.getHcfsType(context);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> file.getDataUri(context));
        assertEquals("the provided path '$HOME/secret-files-in-gpadmin-home' is invalid. The dollar sign character ($) is not allowed by PXF", e.getMessage());
    }

    @Test
    public void testDfsUserHomeBaseDirPropertyHasNoSideEffectInURI() {
        configuration.set("fs.defaultFS", "hdfs://abc:8020/");
        configuration.set("dfs.user.home.base.dir", "/home/francisco/");
        context.setDataSource("foo/bar.txt");

        HcfsType type = HcfsType.getHcfsType(context);
        String dataUri = type.getDataUri(context);
        assertEquals("hdfs://abc:8020/foo/bar.txt", dataUri);
    }

}
