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
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.ReadVectorizedResolver;
import org.greenplum.pxf.api.StatsAccessor;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UtilitiesTest {

    static class StatsAccessorImpl implements StatsAccessor {

        @Override
        public void afterPropertiesSet() {
        }

        @Override
        public boolean openForRead() {
            return false;
        }

        @Override
        public OneRow readNextObject() {
            return null;
        }

        @Override
        public void closeForRead() {
        }

        @Override
        public boolean openForWrite() {
            return false;
        }

        @Override
        public boolean writeNextObject(OneRow onerow) {
            return false;
        }

        @Override
        public void closeForWrite() {

        }

        @Override
        public void retrieveStats() {
        }

        @Override
        public OneRow emitAggObject() {
            return null;
        }

        @Override
        public void setRequestContext(RequestContext context) {
        }
    }

    static class NonStatsAccessorImpl implements Accessor {

        @Override
        public void afterPropertiesSet() {
        }

        @Override
        public boolean openForRead() {
            return false;
        }

        @Override
        public OneRow readNextObject() {
            return null;
        }

        @Override
        public void closeForRead() {
        }

        @Override
        public boolean openForWrite() {
            return false;
        }

        @Override
        public boolean writeNextObject(OneRow onerow) {
            return false;
        }

        @Override
        public void closeForWrite() {
        }

        @Override
        public void setRequestContext(RequestContext context) {
        }
    }

    static class ReadVectorizedResolverImpl implements ReadVectorizedResolver {

        @Override
        public List<List<OneField>> getFieldsForBatch(OneRow batch) {
            return null;
        }
    }

    static class ReadResolverImpl implements Resolver {

        @Override
        public void afterPropertiesSet() {
        }

        @Override
        public List<OneField> getFields(OneRow row) {
            return null;
        }

        @Override
        public OneRow setFields(List<OneField> record) {
            return null;
        }

        @Override
        public void setRequestContext(RequestContext context) {
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
    public void byteArrayToOctalStringNull() {
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
    public void byteArrayToOctalString() {
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
    public void createAnyInstanceOldPackageName() {

        RequestContext metaData = mock(RequestContext.class);
        String className = "com.pivotal.pxf.Lucy";

        Exception e = assertThrows(Exception.class,
                () -> Utilities.createAnyInstance(RequestContext.class,
                        className, metaData),
                "creating an instance should fail because the class doesn't exist in classpath");

        assertEquals(
                e.getMessage(),
                "Class " + className + " does not appear in classpath. "
                        + "Plugins provided by PXF must start with \"org.greenplum.pxf\"");
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

    @Test
    public void testParseBooleanProperty() {
        Configuration configuration = new Configuration();

        configuration.set("trueProperty", "tRUe");
        configuration.set("falseProperty", "fALSe");
        configuration.set("invalidProperty", "foo");
        configuration.set("trueWithExtraWhitespace", "    true    ");

        assertTrue(Utilities.parseBooleanProperty(configuration, "trueProperty", true));
        assertTrue(Utilities.parseBooleanProperty(configuration, "trueProperty", false));

        assertFalse(Utilities.parseBooleanProperty(configuration, "falseProperty", true));
        assertFalse(Utilities.parseBooleanProperty(configuration, "falseProperty", false));

        assertTrue(Utilities.parseBooleanProperty(configuration, "unsetProperty", true));
        assertFalse(Utilities.parseBooleanProperty(configuration, "unsetProperty", false));

        Exception e = assertThrows(PxfRuntimeException.class, () -> Utilities.parseBooleanProperty(configuration, "invalidProperty", false));
        assertEquals("Property invalidProperty has invalid value 'foo'; value should be either 'true' or 'false'", e.getMessage());

        assertTrue(Utilities.parseBooleanProperty(configuration, "trueWithExtraWhitespace", false));
    }
}
