package org.greenplum.pxf.api.utilities;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.ReadVectorizedResolver;
import org.greenplum.pxf.api.StatsAccessor;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class UtilitiesTest {

    private String PROPERTY_KEY_FRAGMENTER_CACHE = "pxf.service.fragmenter.cache.enabled";

    class StatsAccessorImpl implements StatsAccessor {

        @Override
        public boolean openForRead() throws Exception {
            return false;
        }

        @Override
        public OneRow readNextObject() throws Exception {
            return null;
        }

        @Override
        public void closeForRead() throws Exception {
        }

        @Override
        public boolean openForWrite() throws Exception {
            return false;
        }

        @Override
        public boolean writeNextObject(OneRow onerow) throws Exception {
            return false;
        }

        @Override
        public void closeForWrite() throws Exception {

        }

        @Override
        public void retrieveStats() throws Exception {
        }

        @Override
        public OneRow emitAggObject() {
            return null;
        }

        @Override
        public void initialize(RequestContext requestContext) {
        }

        @Override
        public Configuration getConfiguration() {
            return null;
        }
    }

    class NonStatsAccessorImpl implements Accessor {

        @Override
        public boolean openForRead() throws Exception {
            return false;
        }

        @Override
        public OneRow readNextObject() throws Exception {
            return null;
        }

        @Override
        public void closeForRead() throws Exception {
        }

        @Override
        public boolean openForWrite() throws Exception {
            return false;
        }

        @Override
        public boolean writeNextObject(OneRow onerow) throws Exception {
            return false;
        }

        @Override
        public void closeForWrite() throws Exception {
        }

        @Override
        public void initialize(RequestContext requestContext) {
        }

        @Override
        public Configuration getConfiguration() {
            return null;
        }
    }

    class ReadVectorizedResolverImpl implements ReadVectorizedResolver {

        @Override
        public List<List<OneField>> getFieldsForBatch(OneRow batch) {
            return null;
        }
    }

    class ReadResolverImpl implements Resolver {

        @Override
        public List<OneField> getFields(OneRow row) throws Exception {
            return null;
        }

        @Override
        public OneRow setFields(List<OneField> record) throws Exception {
            return null;
        }

        @Override
        public void initialize(RequestContext requestContext) {
        }

        @Override
        public Configuration getConfiguration() {
            return null;
        }
    }

    @Test
    public void testRightTrimWhitespace() {
        assertNull(Utilities.rightTrimWhiteSpace(null));
        assertEquals("", Utilities.rightTrimWhiteSpace(""));
        assertEquals("abc", Utilities.rightTrimWhiteSpace("abc"));
        assertEquals(" abc", Utilities.rightTrimWhiteSpace(" abc"));
        assertEquals("abc", Utilities.rightTrimWhiteSpace("abc "));
        assertEquals("abc", Utilities.rightTrimWhiteSpace("abc      "));
        assertEquals("", Utilities.rightTrimWhiteSpace("    "));
        assertEquals("abc \t", Utilities.rightTrimWhiteSpace("abc \t "));
        assertEquals("abc \t\t", Utilities.rightTrimWhiteSpace("abc \t\t"));
        assertEquals("abc \n", Utilities.rightTrimWhiteSpace("abc \n "));
        assertEquals("abc \n", Utilities.rightTrimWhiteSpace("abc \n"));
    }

    @Test
    public void validDirectoryName() {
        assertTrue(Utilities.isValidDirectoryName("/etc/hadoop/conf"));
        assertTrue(Utilities.isValidDirectoryName("foo"));
    }

    @Test
    public void invalidDirectoryName() {
        assertFalse(Utilities.isValidDirectoryName(null));
        assertFalse(Utilities.isValidDirectoryName("\0"));
    }

    @Test
    public void invalidRestrictedDirectoryName() {
        assertFalse(Utilities.isValidRestrictedDirectoryName(null));
        assertFalse(Utilities.isValidRestrictedDirectoryName("\0"));
        assertFalse(Utilities.isValidRestrictedDirectoryName("a/a"));
        assertFalse(Utilities.isValidRestrictedDirectoryName("."));
        assertFalse(Utilities.isValidRestrictedDirectoryName(".."));
        assertFalse(Utilities.isValidRestrictedDirectoryName("abc ac"));
        assertFalse(Utilities.isValidRestrictedDirectoryName("abc;ac"));
        assertFalse(Utilities.isValidRestrictedDirectoryName("\\"));
        assertFalse(Utilities.isValidRestrictedDirectoryName("a,b"));
    }

    @Test
    public void validRestrictedDirectoryName() {
        assertTrue(Utilities.isValidRestrictedDirectoryName("pxf"));
        assertTrue(Utilities.isValidRestrictedDirectoryName("\uD83D\uDE0A"));
    }

    @Test
    public void byteArrayToOctalStringNull() throws Exception {
        StringBuilder sb = null;
        byte[] bytes = "nofink".getBytes();

        Utilities.byteArrayToOctalString(bytes, sb);

        assertNull(sb);

        sb = new StringBuilder();
        bytes = null;

        Utilities.byteArrayToOctalString(bytes, sb);

        assertEquals(0, sb.length());
    }

    @Test
    public void byteArrayToOctalString() throws Exception {
        String orig = "Have Narisha";
        String octal = "Rash Rash Rash!";
        String expected = orig + "\\\\122\\\\141\\\\163\\\\150\\\\040"
                + "\\\\122\\\\141\\\\163\\\\150\\\\040"
                + "\\\\122\\\\141\\\\163\\\\150\\\\041";
        StringBuilder sb = new StringBuilder();
        sb.append(orig);

        Utilities.byteArrayToOctalString(octal.getBytes(), sb);

        assertEquals(orig.length() + (octal.length() * 5), sb.length());
        assertEquals(expected, sb.toString());
    }

    @Test
    public void createAnyInstanceOldPackageName() throws Exception {

        RequestContext metaData = mock(RequestContext.class);
        String className = "com.pivotal.pxf.Lucy";

        try {
            Utilities.createAnyInstance(RequestContext.class,
                    className, metaData);
            fail("creating an instance should fail because the class doesn't exist in classpath");
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
            assertEquals(
                    e.getMessage(),
                    "Class " + className + " does not appear in classpath. "
                            + "Plugins provided by PXF must start with \"org.greenplum.pxf\"");
        }
    }

    @Test
    public void maskNonPrintable() {
        String input = "";
        String result = Utilities.maskNonPrintables(input);
        assertEquals("", result);

        input = null;
        result = Utilities.maskNonPrintables(input);
        assertNull(result);

        input = "Lucy in the sky";
        result = Utilities.maskNonPrintables(input);
        assertEquals("Lucy.in.the.sky", result);

        input = "with <$$$@#$!000diamonds!!?!$#&%/>";
        result = Utilities.maskNonPrintables(input);
        assertEquals("with.........000diamonds......../.", result);

        input = "http://www.beatles.com/info?query=whoisthebest";
        result = Utilities.maskNonPrintables(input);
        assertEquals("http://www.beatles.com/info.query.whoisthebest", result);
    }

    @Test
    public void parseFragmentMetadata() throws Exception {
        RequestContext metaData = mock(RequestContext.class);
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(bas);
        os.writeLong(10);
        os.writeLong(100);
        os.writeObject(new String[]{"hostname"});
        os.close();
        when(metaData.getFragmentMetadata()).thenReturn(bas.toByteArray());
        FragmentMetadata fragmentMetadata = Utilities.parseFragmentMetadata(metaData);

        assertEquals(10, fragmentMetadata.getStart());
        assertEquals(100, fragmentMetadata.getEnd());
        assertArrayEquals(new String[]{"hostname"}, fragmentMetadata.getHosts());
    }

    @Test
    public void useAggBridge() {
        RequestContext metaData = mock(RequestContext.class);
        when(metaData.getAccessor()).thenReturn(StatsAccessorImpl.class.getName());
        when(metaData.getAggType()).thenReturn(EnumAggregationType.COUNT);
        when(metaData.getAccessor()).thenReturn("org.greenplum.pxf.api.utilities.UtilitiesTest$StatsAccessorImpl");
        assertTrue(Utilities.aggregateOptimizationsSupported(metaData));

        when(metaData.getAccessor()).thenReturn(UtilitiesTest.class.getName());
        when(metaData.getAggType()).thenReturn(EnumAggregationType.COUNT);
        assertFalse(Utilities.aggregateOptimizationsSupported(metaData));

        //Do not use AggBridge when input data has filter
        when(metaData.getAccessor()).thenReturn(StatsAccessorImpl.class.getName());
        when(metaData.getAggType()).thenReturn(EnumAggregationType.COUNT);
        when(metaData.hasFilter()).thenReturn(true);
        assertFalse(Utilities.aggregateOptimizationsSupported(metaData));
    }

    @Test
    public void useStats() {
        RequestContext mockCtxSupporting = mock(RequestContext.class);
        when(mockCtxSupporting.getAggType()).thenReturn(EnumAggregationType.COUNT);
        when(mockCtxSupporting.getAccessor()).thenReturn("org.greenplum.pxf.api.utilities.UtilitiesTest$StatsAccessorImpl");
        assertTrue(Utilities.aggregateOptimizationsSupported(mockCtxSupporting));

        RequestContext mockCtxNonSupporting = mock(RequestContext.class);
        when(mockCtxNonSupporting.getAggType()).thenReturn(EnumAggregationType.COUNT);
        when(mockCtxNonSupporting.getAccessor()).thenReturn("org.greenplum.pxf.api.utilities.UtilitiesTest$NonStatsAccessorImpl");
        assertFalse(Utilities.aggregateOptimizationsSupported(mockCtxNonSupporting));

        //Do not use stats when input data has filter
        RequestContext mockCtxFilter = mock(RequestContext.class);
        when(mockCtxFilter.getAggType()).thenReturn(EnumAggregationType.COUNT);
        when(mockCtxFilter.getAccessor()).thenReturn("org.greenplum.pxf.api.utilities.UtilitiesTest$StatsAccessorImpl");
        when(mockCtxFilter.hasFilter()).thenReturn(true);
        assertFalse(Utilities.aggregateOptimizationsSupported(mockCtxFilter));

        //Do not use stats when more than one column is projected
        RequestContext mockCtxProjection = mock(RequestContext.class);
        when(mockCtxProjection.getAggType()).thenReturn(EnumAggregationType.COUNT);
        when(mockCtxProjection.getAccessor()).thenReturn("org.greenplum.pxf.api.utilities.UtilitiesTest$StatsAccessorImpl");
        when(mockCtxProjection.hasFilter()).thenReturn(false);
        when(mockCtxProjection.getNumAttrsProjected()).thenReturn(1);
        assertFalse(Utilities.aggregateOptimizationsSupported(mockCtxProjection));
    }

    /* TODO move to the proper class
    @Test
    public void useVectorization() {
        RequestContext metaData = mock(RequestContext.class);
        when(metaData.getResolver()).thenReturn("org.greenplum.pxf.api.utilities.UtilitiesTest$ReadVectorizedResolverImpl");
        assertTrue(Utilities.useVectorization(metaData));
        when(metaData.getResolver()).thenReturn("org.greenplum.pxf.api.utilities.UtilitiesTest$ReadResolverImpl");
        assertFalse(Utilities.useVectorization(metaData));
    }
    */

    @Test
    public void testFragmenterCachePropertyAbsent() {
        System.clearProperty(PROPERTY_KEY_FRAGMENTER_CACHE);
        assertTrue(Utilities.isFragmenterCacheEnabled());
    }

    @Test
    public void testFragmenterCachePropertyEmpty() {
        System.setProperty(PROPERTY_KEY_FRAGMENTER_CACHE, "");
        assertTrue(Utilities.isFragmenterCacheEnabled());
    }

    @Test
    public void testFragmenterCachePropertyFoo() {
        System.setProperty(PROPERTY_KEY_FRAGMENTER_CACHE, "foo");
        assertTrue(Utilities.isFragmenterCacheEnabled());
    }

    @Test
    public void testFragmenterCachePropertyFALSE() {
        System.setProperty(PROPERTY_KEY_FRAGMENTER_CACHE, "FALSE");
        assertFalse(Utilities.isFragmenterCacheEnabled());
    }

    @Test
    public void testFragmenterCachePropertyFalse() {
        System.setProperty(PROPERTY_KEY_FRAGMENTER_CACHE, "false");
        assertFalse(Utilities.isFragmenterCacheEnabled());
    }

    @Test
    public void testSecurityIsDisabledOnNewConfiguration() {
        Configuration configuration = new Configuration();
        assertFalse(Utilities.isSecurityEnabled(configuration));
    }

    @Test
    public void testSecurityIsDisabledWithSimpleAuthentication() {
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "simple");
        assertFalse(Utilities.isSecurityEnabled(configuration));
    }

    @Test
    public void testSecurityIsEnabledWithKerberosAuthentication() {
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "kerberos");
        assertTrue(Utilities.isSecurityEnabled(configuration));
    }

    @Test
    public void testGetHost() {

        assertNull(Utilities.getHost(null));
        assertNull(Utilities.getHost(""));
        assertNull(Utilities.getHost("  "));
        assertNull(Utilities.getHost(":"));
        assertNull(Utilities.getHost("#"));
        assertNull(Utilities.getHost("/"));
        assertNull(Utilities.getHost("/file/path/abc"));

        assertEquals("www.google.com", Utilities.getHost("https://www.google.com/"));
        assertEquals("www.google.com", Utilities.getHost("http://www.google.com/"));

        assertEquals("www.blog.classroom.me.uk", Utilities.getHost("http://www.blog.classroom.me.uk/index.php"));
        assertEquals("www.youtube.com", Utilities.getHost("http://www.youtube.com/watch?v=ClkQA2Lb_iE"));
        assertEquals("www.youtube.com", Utilities.getHost("https://www.youtube.com/watch?v=ClkQA2Lb_iE"));
        assertEquals("www.youtube.com", Utilities.getHost("www.youtube.com/watch?v=ClkQA2Lb_iE"));
        assertEquals("ftp.websitename.com", Utilities.getHost("ftps://ftp.websitename.com/dir/file.txt"));
        assertEquals("websitename.com", Utilities.getHost("websitename.com:1234/dir/file.txt"));
        assertEquals("websitename.com", Utilities.getHost("ftps://websitename.com:1234/dir/file.txt"));
        assertEquals("example.com", Utilities.getHost("example.com?param=value"));
        assertEquals("facebook.github.io", Utilities.getHost("https://facebook.github.io/jest/"));
        assertEquals("youtube.com", Utilities.getHost("//youtube.com/watch?v=ClkQA2Lb_iE"));
        assertEquals("localhost", Utilities.getHost("http://localhost:4200/watch?v=ClkQA2Lb_iE"));

        assertEquals("127.0.0.1", Utilities.getHost("hdfs://127.0.0.1:8020"));
        assertEquals("my-bucket", Utilities.getHost("s3a://my-bucket/foo/ba[rc]"));
        assertEquals("foo", Utilities.getHost("s3://foo/bar.txt"));
        assertEquals("foo.azuredatalakestore.net", Utilities.getHost("adl://foo.azuredatalakestore.net/foo/bar.txt"));
        assertEquals("foo", Utilities.getHost("xyz://foo/bar.txt"));
        assertEquals("0.0.0.0", Utilities.getHost("xyz://0.0.0.0:80/foo/bar.txt"));
        assertEquals("abc", Utilities.getHost("xyz://abc/foo/bar.txt"));
        assertNull(Utilities.getHost("file:///foo/bar.txt"));
        assertEquals("0.0.0.0", Utilities.getHost("hdfs://0.0.0.0:8020"));
        assertEquals("abc", Utilities.getHost("hdfs://abc:8020/foo/bar.txt"));
        assertEquals("0.0.0.0", Utilities.getHost("hdfs://0.0.0.0:8020/tmp/issues/172848577/[a-b].csv"));

        assertEquals("0.0.0.0", Utilities.getHost("hdfs://0.0.0.0#anchor"));
        assertEquals("0.0.0.0", Utilities.getHost("hdfs://0.0.0.0/p"));
        assertEquals("0.0.0.0", Utilities.getHost("hdfs://0.0.0.0?PROFILE=foo"));
        assertEquals("www.example.com", Utilities.getHost("www.example.com"));
        assertEquals("10.0.0.15", Utilities.getHost("10.0.0.15"));
    }
}
