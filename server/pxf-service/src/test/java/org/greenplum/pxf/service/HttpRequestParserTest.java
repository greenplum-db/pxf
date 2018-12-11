package org.greenplum.pxf.service;

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


import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.commons.codec.CharEncoding;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.PluginConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.profile.ProfileConfException;
import org.greenplum.pxf.service.profile.ProfilesConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.greenplum.pxf.service.profile.ProfileConfException.MessageFormat.NO_PROFILE_DEF;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ProfilesConf.class})
public class HttpRequestParserTest {

    private MultivaluedMap<String, String> multivaluedMap;
    private MultivaluedMap<String, String> parameters;
    private HttpHeaders mockRequestHeaders;

    /*
     * setUp function called before each test
     */
    @Before
    public void setUp() {
        parameters = new MultivaluedMapImpl();
        multivaluedMap = new MultivaluedMapImpl();
        mockRequestHeaders = mock(HttpHeaders.class);

        parameters.putSingle("X-GP-ALIGNMENT", "all");
        parameters.putSingle("X-GP-SEGMENT-ID", "-44");
        parameters.putSingle("X-GP-SEGMENT-COUNT", "2");
        parameters.putSingle("X-GP-HAS-FILTER", "0");
        parameters.putSingle("X-GP-FORMAT", "TEXT");
        parameters.putSingle("X-GP-URL-HOST", "my://bags");
        parameters.putSingle("X-GP-URL-PORT", "-8020");
        parameters.putSingle("X-GP-ATTRS", "-1");
        parameters.putSingle("X-GP-OPTIONS-ACCESSOR", "are");
        parameters.putSingle("X-GP-OPTIONS-RESOLVER", "packed");
        parameters.putSingle("X-GP-DATA-DIR", "i'm/ready/to/go");
        parameters.putSingle("X-GP-FRAGMENT-METADATA", "U29tZXRoaW5nIGluIHRoZSB3YXk=");
        parameters.putSingle("X-GP-OPTIONS-I'M-STANDING-HERE", "outside-your-door");
        parameters.putSingle("X-GP-USER", "alex");
        parameters.putSingle("X-GP-OPTIONS-SERVER", "custom_server");

        when(mockRequestHeaders.getRequestHeaders()).thenReturn(parameters);
    }

    /*
     * tearDown function called after each test
     */
    @After
    public void tearDown() {
        // Cleanup the system property RequestContext sets
        System.clearProperty("greenplum.alignment");
    }

    @Test
    public void testConvertToCaseInsensitiveMap() throws Exception {
        List<String> multiCaseKeys = Arrays.asList("X-GP-SHLOMO", "x-gp-shlomo", "X-Gp-ShLoMo");
        String value = "\\\"The king";
        String replacedValue = "\"The king";

        for (String key : multiCaseKeys) {
            multivaluedMap.put(key, Collections.singletonList(value));
        }

        assertEquals("All keys should have existed", multivaluedMap.keySet().size(), multiCaseKeys.size());

        Map<String, String> caseInsensitiveMap = new HttpRequestParser.RequestMap(multivaluedMap);

        assertEquals("Only one key should have exist", caseInsensitiveMap.keySet().size(), 1);

        for (String key : multiCaseKeys) {
            assertEquals("All keys should have returned the same value", caseInsensitiveMap.get(key), replacedValue);
        }
    }

    @Test
    public void testConvertToCaseInsensitiveMapUtf8() throws Exception {
        byte[] bytes = {
                (byte) 0x61, (byte) 0x32, (byte) 0x63, (byte) 0x5c, (byte) 0x22,
                (byte) 0x55, (byte) 0x54, (byte) 0x46, (byte) 0x38, (byte) 0x5f,
                (byte) 0xe8, (byte) 0xa8, (byte) 0x88, (byte) 0xe7, (byte) 0xae,
                (byte) 0x97, (byte) 0xe6, (byte) 0xa9, (byte) 0x9f, (byte) 0xe7,
                (byte) 0x94, (byte) 0xa8, (byte) 0xe8, (byte) 0xaa, (byte) 0x9e,
                (byte) 0x5f, (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x30,
                (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x5c,
                (byte) 0x22, (byte) 0x6f, (byte) 0x35
        };
        String value = new String(bytes, CharEncoding.ISO_8859_1);

        multivaluedMap.put("one", Collections.singletonList(value));

        Map<String, String> caseInsensitiveMap = new HttpRequestParser.RequestMap(multivaluedMap);

        assertEquals("Only one key should have exist", caseInsensitiveMap.keySet().size(), 1);

        assertEquals("Value should be converted to UTF-8",
                caseInsensitiveMap.get("one"), "a2c\"UTF8_計算機用語_00000000\"o5");
    }

    @Test
    public void protocolDataCreated() {
        RequestContext context = new HttpRequestParser().parseRequest(mockRequestHeaders);

        assertEquals(System.getProperty("greenplum.alignment"), "all");
        assertEquals(context.getTotalSegments(), 2);
        assertEquals(context.getSegmentId(), -44);
        Assert.assertEquals(context.getOutputFormat(), OutputFormat.TEXT);
        assertEquals(context.getHost(), "my://bags");
        assertEquals(context.getPort(), -8020);
        assertFalse(context.hasFilter());
        assertNull(context.getFilterString());
        assertEquals(context.getColumns(), 0);
        assertEquals(context.getDataFragment(), -1);
        assertNull(context.getRecordkeyColumn());
        assertEquals(context.getAccessor(), "are");
        assertEquals(context.getResolver(), "packed");
        assertEquals(context.getDataSource(), "i'm/ready/to/go");
        assertEquals(context.getOption("i'm-standing-here"), "outside-your-door");
        assertEquals(context.getUser(), "alex");
//        assertEquals(context.getParametersMap(), parameters);
        assertNull(context.getLogin());
        assertNull(context.getSecret());
        assertEquals(context.getServerName(), "custom_server");
    }

    @Test
    public void profileWithDuplicateProperty() {

        PluginConf pluginConf = mock(PluginConf.class);
        Map<String, String> mockedProfiles = new HashMap<>();
        // What we read from the XML Plugins file
        mockedProfiles.put("wHEn you trY yOUR bESt", "but you dont succeed");
        mockedProfiles.put("when YOU get WHAT you WANT",
                "but not what you need");
        mockedProfiles.put("when you feel so tired", "but you cant sleep");

        when(pluginConf.getPlugins("a profile")).thenReturn(mockedProfiles);

        // Parameters that are coming from the request
        parameters.putSingle("x-gp-options-profile", "a profile");
        parameters.putSingle("x-gp-options-when you try your best", "and you do succeed");
        parameters.putSingle("x-gp-options-WHEN you GET what YOU want", "and what you need");

        try {
            new HttpRequestParser(pluginConf).parseRequest(mockRequestHeaders);
            fail("Duplicate property should throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals(
                    "Profile 'a profile' already defines: [when YOU get WHAT you WANT, wHEn you trY yOUR bESt]",
                    iae.getMessage());
        }
    }

    @Test
    public void definedProfile() {
        parameters.putSingle("X-GP-OPTIONS-PROFILE", "HIVE");
        parameters.remove("X-GP-OPTIONS-ACCESSOR");
        parameters.remove("X-GP-OPTIONS-RESOLVER");
        RequestContext protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertEquals(protocolData.getFragmenter(), "org.greenplum.pxf.plugins.hive.HiveDataFragmenter");
        assertEquals(protocolData.getAccessor(), "org.greenplum.pxf.plugins.hive.HiveAccessor");
        assertEquals(protocolData.getResolver(), "org.greenplum.pxf.plugins.hive.HiveResolver");
    }

    @Test
    public void undefinedProfile() {
        parameters.putSingle("X-GP-OPTIONS-PROFILE", "THIS_PROFILE_NEVER_EXISTED!");
        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("Undefined profile should throw ProfileConfException");
        } catch (ProfileConfException pce) {
            assertEquals(pce.getMsgFormat(), NO_PROFILE_DEF);
        }
    }

    @Test
    public void undefinedServer() {
        parameters.remove("X-GP-OPTIONS-SERVER");
        RequestContext protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertEquals("default", protocolData.getServerName());
    }

    @Test
    public void threadSafeTrue() {
        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "TRUE");
        RequestContext protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertTrue(protocolData.isThreadSafe());

        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "true");
        protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertTrue(protocolData.isThreadSafe());
    }

    @Test
    public void threadSafeFalse() {
        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "False");
        RequestContext protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertFalse(protocolData.isThreadSafe());

        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "falSE");
        protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertFalse(protocolData.isThreadSafe());
    }

    @Test
    public void threadSafeMaybe() {
        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "maybe");
        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("illegal THREAD-SAFE value should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Illegal boolean value 'maybe'. Usage: [TRUE|FALSE]");
        }
    }

    @Test
    public void threadSafeDefault() {
        parameters.remove("X-GP-OPTIONS-THREAD-SAFE");
        RequestContext protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertTrue(protocolData.isThreadSafe());
    }

    @Test
    public void getFragmentMetadata() {
        RequestContext protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);
        byte[] location = protocolData.getFragmentMetadata();
        assertEquals(new String(location), "Something in the way");
    }

    @Test
    public void getFragmentMetadataNull() {
        parameters.remove("X-GP-FRAGMENT-METADATA");
        RequestContext requestContext = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertNull(requestContext.getFragmentMetadata());
    }

    @Test
    public void getFragmentMetadataNotBase64() {
        String badValue = "so b@d";
        parameters.putSingle("X-GP-FRAGMENT-METADATA", badValue);
        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("should fail with bad fragment metadata");
        } catch (Exception e) {
            assertEquals("Fragment metadata information must be Base64 encoded."
                    + " (Bad value: " + badValue + ")", e.getMessage());
        }
    }

    @Test
    public void nullUserThrowsException() {
        parameters.remove("X-GP-USER");
        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("null X-GP-USER should throw exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Property USER has no value in the current request", e.getMessage());
        }
    }

    @Test
    public void filterUtf8() {
        parameters.remove("X-GP-HAS-FILTER");
        parameters.putSingle("X-GP-HAS-FILTER", "1");
        String isoString = new String("UTF8_計算機用語_00000000".getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
        parameters.putSingle("X-GP-FILTER", isoString);
        RequestContext protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertTrue(protocolData.hasFilter());
        assertEquals("UTF8_計算機用語_00000000", protocolData.getFilterString());
    }

    @Test
    public void statsParams() {
        parameters.putSingle("X-GP-OPTIONS-STATS-MAX-FRAGMENTS", "10101");
        parameters.putSingle("X-GP-OPTIONS-STATS-SAMPLE-RATIO", "0.039");

        RequestContext context = new HttpRequestParser().parseRequest(mockRequestHeaders);

        assertEquals(10101, context.getStatsMaxFragments());
        assertEquals(0.039, context.getStatsSampleRatio(), 0.01);
    }

    @Test
    public void testInvalidStatsSampleRatioValue() {
        parameters.putSingle("X-GP-OPTIONS-STATS-SAMPLE-RATIO", "a");
        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("wrong X-GP-OPTIONS-STATS-SAMPLE-RATIO value");
        } catch (NumberFormatException e) {
            assertEquals(e.getMessage(), "For input string: \"a\"");
        }
    }

    @Test
    public void testInvalidStatsMaxFragmentsValue() {
        parameters.putSingle("X-GP-OPTIONS-STATS-MAX-FRAGMENTS", "10.101");

        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("wrong X-GP-OPTIONS-STATS-MAX-FRAGMENTS value");
        } catch (NumberFormatException e) {
            assertEquals(e.getMessage(), "For input string: \"10.101\"");
        }
    }

    @Test
    public void typeMods() {

        parameters.putSingle("X-GP-ATTRS", "2");
        parameters.putSingle("X-GP-ATTR-NAME0", "vc1");
        parameters.putSingle("X-GP-ATTR-TYPECODE0", "1043");
        parameters.putSingle("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-COUNT", "1");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-0", "5");

        parameters.putSingle("X-GP-ATTR-NAME1", "dec1");
        parameters.putSingle("X-GP-ATTR-TYPECODE1", "1700");
        parameters.putSingle("X-GP-ATTR-TYPENAME1", "numeric");
        parameters.putSingle("X-GP-ATTR-TYPEMOD1-COUNT", "2");
        parameters.putSingle("X-GP-ATTR-TYPEMOD1-0", "10");
        parameters.putSingle("X-GP-ATTR-TYPEMOD1-1", "2");

        RequestContext protocolData = new HttpRequestParser().parseRequest(mockRequestHeaders);

        assertArrayEquals(protocolData.getColumn(0).columnTypeModifiers(), new Integer[]{5});
        assertArrayEquals(protocolData.getColumn(1).columnTypeModifiers(), new Integer[]{10, 2});
    }

    @Test
    public void typeModsNegative() {

        parameters.putSingle("X-GP-ATTRS", "1");
        parameters.putSingle("X-GP-ATTR-NAME0", "vc1");
        parameters.putSingle("X-GP-ATTR-TYPECODE0", "1043");
        parameters.putSingle("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-COUNT", "X");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-0", "Y");


        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("should throw IllegalArgumentException when bad value received for X-GP-ATTR-TYPEMOD0-COUNT");
        } catch (IllegalArgumentException iae) {
            assertEquals("ATTR-TYPEMOD0-COUNT must be an integer", iae.getMessage());
        }

        parameters.putSingle("X-GP-ATTR-TYPEMOD0-COUNT", "-1");

        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("should throw IllegalArgumentException when negative value received for X-GP-ATTR-TYPEMOD0-COUNT");
        } catch (IllegalArgumentException iae) {
            assertEquals("ATTR-TYPEMOD0-COUNT must be a positive integer", iae.getMessage());
        }

        parameters.putSingle("X-GP-ATTR-TYPEMOD0-COUNT", "1");

        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("should throw IllegalArgumentException when bad value received for X-GP-ATTR-TYPEMOD0-0");
        } catch (IllegalArgumentException iae) {
            assertEquals("ATTR-TYPEMOD0-0 must be an integer", iae.getMessage());
        }

        parameters.putSingle("X-GP-ATTR-TYPEMOD0-COUNT", "2");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-0", "42");

        try {
            new HttpRequestParser().parseRequest(mockRequestHeaders);
            fail("should throw IllegalArgumentException number of actual type modifiers is less than X-GP-ATTR-TYPEMODX-COUNT");
        } catch (IllegalArgumentException iae) {
            assertEquals("Property ATTR-TYPEMOD0-1 has no value in the current request", iae.getMessage());
        }
    }
}
