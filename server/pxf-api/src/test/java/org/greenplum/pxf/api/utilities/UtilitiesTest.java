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


import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.ReadVectorizedResolver;
import org.greenplum.pxf.api.StatsAccessor;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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

@RunWith(PowerMockRunner.class)
@PrepareForTest({Class.class})
public class UtilitiesTest {

    private String PROPERTY_KEY_USER_IMPERSONATION = "pxf.service.user.impersonation.enabled";

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
        public boolean isThreadSafe() {
            return false;
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
        public boolean isThreadSafe() {
            return false;
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
        public boolean isThreadSafe() {
            return false;
        }
    }

    @Test
    public void invalidDirectoryName() {
        assertFalse(Utilities.isValidDirectoryName(null));
        assertFalse(Utilities.isValidDirectoryName("\0"));
        assertFalse(Utilities.isValidDirectoryName("a/a"));
        assertFalse(Utilities.isValidDirectoryName("."));
        assertFalse(Utilities.isValidDirectoryName(".."));
        assertFalse(Utilities.isValidDirectoryName("abc ac"));
        assertFalse(Utilities.isValidDirectoryName("abc;ac"));
        assertFalse(Utilities.isValidDirectoryName("\\"));
        assertFalse(Utilities.isValidDirectoryName("a,b"));
    }

    @Test
    public void validDirectoryName() {
        assertTrue(Utilities.isValidDirectoryName("pxf"));
        assertTrue(Utilities.isValidDirectoryName("\uD83D\uDE0A"));
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
        ClassNotFoundException exception = new ClassNotFoundException(className);
        PowerMockito.mockStatic(Class.class);
        when(Class.forName(className)).thenThrow(exception);

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
    public void maskNonPrintable() throws Exception {
        String input = "";
        String result = Utilities.maskNonPrintables(input);
        assertEquals("", result);

        input = null;
        result = Utilities.maskNonPrintables(input);
        assertEquals(null, result);

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
        os.writeObject(new String[] { "hostname" });
        os.close();
        when(metaData.getFragmentMetadata()).thenReturn(bas.toByteArray());
        FragmentMetadata fragmentMetadata = Utilities.parseFragmentMetadata(metaData);

        assertEquals(10, fragmentMetadata.getStart());
        assertEquals(100, fragmentMetadata.getEnd());
        assertArrayEquals(new String[] { "hostname" }, fragmentMetadata.getHosts());
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
    public void testImpersonationPropertyAbsent() {
        System.clearProperty(PROPERTY_KEY_USER_IMPERSONATION);
        assertFalse(Utilities.isUserImpersonationEnabled());
    }

    @Test
    public void testImpersonationPropertyEmpty() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "");
        assertFalse(Utilities.isUserImpersonationEnabled());
    }

    @Test
    public void testImpersonationPropertyFalse() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "foo");
        assertFalse(Utilities.isUserImpersonationEnabled());
    }

    @Test
    public void testImpersonationPropertyTRUE() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "TRUE");
        assertTrue(Utilities.isUserImpersonationEnabled());
    }

    @Test
    public void testImpersonationPropertyTrue() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "true");
        assertTrue(Utilities.isUserImpersonationEnabled());
    }
}
