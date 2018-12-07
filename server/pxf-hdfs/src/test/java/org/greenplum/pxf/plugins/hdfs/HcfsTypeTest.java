package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class HcfsTypeTest {
    private final String S3_PROTOCOL = "s3";
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
    public void testNonFileDefaultFSTakesPrecedenceOverProtocol() {
        // Test that defaultFs takes precedence over protocol (when it's not file)
        configuration.set("fs.defaultFS", "hdfs://0.0.0.0:8020");
        context.setProtocol(S3_PROTOCOL);

        HcfsType type = HcfsType.getHcfsType(configuration, context);

        assertEquals(HcfsType.HDFS, type);
        assertEquals("hdfs://0.0.0.0:8020/foo/bar.txt", type.getDataUri(configuration, context));
    }

    @Test
    public void testProtocolTakesPrecedenceOverFileDefaultFs() {
        // Test that we can specify protocol when configuration defaults are loaded
        context.setProtocol(S3_PROTOCOL);

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals(HcfsType.S3, type);
        assertEquals("s3:///foo/bar.txt", type.getDataUri(configuration, context));
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
        context.setProtocol("xyz");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals(HcfsType.CUSTOM, type);
        assertEquals("xyz:///foo/bar.txt", type.getDataUri(configuration, context));
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
        context.setProtocol("localfile");

        HcfsType type = HcfsType.getHcfsType(configuration, context);
        assertEquals(HcfsType.LOCALFILE, type);
        assertEquals("file:///foo/bar.txt", type.getDataUri(configuration, context));
    }
}
