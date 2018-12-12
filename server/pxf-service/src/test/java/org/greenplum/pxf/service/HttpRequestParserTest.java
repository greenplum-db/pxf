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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpRequestParserTest {

    private MultivaluedMap<String, String> parameters;
    private HttpRequestParser parser;
    @Mock private HttpHeaders mockRequestHeaders;
    @Mock private PluginConf mockPluginConf;

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        parameters = new MultivaluedMapImpl();
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

        parser = new HttpRequestParser(mockPluginConf);
    }

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

        MultivaluedMap<String, String> multivaluedMap = new MultivaluedMapImpl();
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

        MultivaluedMap<String, String> multivaluedMap = new MultivaluedMapImpl();
        multivaluedMap.put("one", Collections.singletonList(value));

        Map<String, String> caseInsensitiveMap = new HttpRequestParser.RequestMap(multivaluedMap);

        assertEquals("Only one key should have exist", caseInsensitiveMap.keySet().size(), 1);

        assertEquals("Value should be converted to UTF-8",
                caseInsensitiveMap.get("one"), "a2c\"UTF8_計算機用語_00000000\"o5");
    }

    @Test
    public void contextCreated() {
        RequestContext context = parser.parseRequest(mockRequestHeaders);

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
        assertNull(context.getLogin());
        assertNull(context.getSecret());
        assertEquals(context.getServerName(), "custom_server");
        // since no profile was defined, these below are null or empty
        assertNull(context.getProfile());
        assertNull(context.getProtocol());
        assertTrue(context.getAdditionalConfigProps().isEmpty());
    }

    @Test
    public void profileWithDuplicateProperty() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Profile 'test-profile' already defines: [when YOU get WHAT you WANT, wHEn you trY yOUR bESt]");

        Map<String, String> mockedPlugins = new HashMap<>();
        // What we read from the XML Plugins file
        mockedPlugins.put("wHEn you trY yOUR bESt", "but you dont succeed");
        mockedPlugins.put("when YOU get WHAT you WANT",
                "but not what you need");
        mockedPlugins.put("when you feel so tired", "but you cant sleep");

        when(mockPluginConf.getPlugins("test-profile")).thenReturn(mockedPlugins);

        // Parameters that are coming from the request
        parameters.putSingle("x-gp-options-profile", "test-profile");
        parameters.putSingle("x-gp-options-when you try your best", "and you do succeed");
        parameters.putSingle("x-gp-options-WHEN you GET what YOU want", "and what you need");

        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void pluginsDefinedByProfileOnly() {
        Map<String, String> mockedPlugins = new HashMap<>();
        mockedPlugins.put("fragmenter", "test-fragmenter");
        mockedPlugins.put("accessor", "test-accessor");
        mockedPlugins.put("resolver", "test-resolver");
        when(mockPluginConf.getPlugins("test-profile")).thenReturn(mockedPlugins);

        // add profile and remove plugin parameters
        parameters.putSingle("X-GP-OPTIONS-PROFILE", "test-profile");
        parameters.remove("X-GP-OPTIONS-ACCESSOR");
        parameters.remove("X-GP-OPTIONS-RESOLVER");
        RequestContext context = parser.parseRequest(mockRequestHeaders);
        assertEquals(context.getFragmenter(), "test-fragmenter");
        assertEquals(context.getAccessor(), "test-accessor");
        assertEquals(context.getResolver(), "test-resolver");
    }

    @Test
    public void pluginsRedefinedByProfileFails() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Profile 'test-profile' already defines: [resolver, accessor]");

        Map<String, String> mockedPlugins = new HashMap<>();
        mockedPlugins.put("fragmenter", "test-fragmenter");
        mockedPlugins.put("accessor", "test-accessor");
        mockedPlugins.put("resolver", "test-resolver");
        when(mockPluginConf.getPlugins("test-profile")).thenReturn(mockedPlugins);

        // add profile in addition to plugins
        parameters.putSingle("X-GP-OPTIONS-PROFILE", "test-profile");
        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void undefinedServer() {
        parameters.remove("X-GP-OPTIONS-SERVER");
        RequestContext context = parser.parseRequest(mockRequestHeaders);
        assertEquals("default", context.getServerName());
    }

    @Test
    public void threadSafeTrue() {
        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "TRUE");
        RequestContext context = parser.parseRequest(mockRequestHeaders);
        assertTrue(context.isThreadSafe());

        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "true");
        context = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertTrue(context.isThreadSafe());
    }

    @Test
    public void threadSafeFalse() {
        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "False");
        RequestContext context = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertFalse(context.isThreadSafe());

        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "falSE");
        context = new HttpRequestParser().parseRequest(mockRequestHeaders);
        assertFalse(context.isThreadSafe());
    }

    @Test
    public void threadSafeMaybe() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal boolean value 'maybe'. Usage: [TRUE|FALSE]");

        parameters.putSingle("X-GP-OPTIONS-THREAD-SAFE", "maybe");
        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void threadSafeDefault() {
        parameters.remove("X-GP-OPTIONS-THREAD-SAFE");
        RequestContext context = parser.parseRequest(mockRequestHeaders);
        assertTrue(context.isThreadSafe());
    }

    @Test
    public void getFragmentMetadata() {
        RequestContext context = parser.parseRequest(mockRequestHeaders);
        byte[] location = context.getFragmentMetadata();
        assertEquals("Something in the way", new String(location));
    }

    @Test
    public void getFragmentMetadataNull() {
        parameters.remove("X-GP-FRAGMENT-METADATA");
        RequestContext requestContext = parser.parseRequest(mockRequestHeaders);
        assertNull(requestContext.getFragmentMetadata());
    }

    @Test
    public void getFragmentMetadataNotBase64() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Fragment metadata information must be Base64 encoded. (Bad value: so b@d)");

        String badValue = "so b@d";
        parameters.putSingle("X-GP-FRAGMENT-METADATA", badValue);
        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void nullUserThrowsException() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property USER has no value in the current request");

        parameters.remove("X-GP-USER");
        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void filterUtf8() {
        parameters.remove("X-GP-HAS-FILTER");
        parameters.putSingle("X-GP-HAS-FILTER", "1");
        String isoString = new String("UTF8_計算機用語_00000000".getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
        parameters.putSingle("X-GP-FILTER", isoString);
        RequestContext context = parser.parseRequest(mockRequestHeaders);
        assertTrue(context.hasFilter());
        assertEquals("UTF8_計算機用語_00000000", context.getFilterString());
    }

    @Test
    public void statsParams() {
        parameters.putSingle("X-GP-OPTIONS-STATS-MAX-FRAGMENTS", "10101");
        parameters.putSingle("X-GP-OPTIONS-STATS-SAMPLE-RATIO", "0.039");

        RequestContext context = parser.parseRequest(mockRequestHeaders);

        assertEquals(10101, context.getStatsMaxFragments());
        assertEquals(0.039, context.getStatsSampleRatio(), 0.01);
    }

    @Test
    public void testInvalidStatsSampleRatioValue() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("For input string: \"a\"");

        parameters.putSingle("X-GP-OPTIONS-STATS-SAMPLE-RATIO", "a");
        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void testInvalidStatsMaxFragmentsValue() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("For input string: \"10.101\"");

        parameters.putSingle("X-GP-OPTIONS-STATS-MAX-FRAGMENTS", "10.101");
        parser.parseRequest(mockRequestHeaders);
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

        RequestContext context = parser.parseRequest(mockRequestHeaders);

        assertArrayEquals(new Integer[]{5}, context.getColumn(0).columnTypeModifiers());
        assertArrayEquals(new Integer[]{10, 2}, context.getColumn(1).columnTypeModifiers());
    }

    @Test
    public void typeModCountNonIntegerFails() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("ATTR-TYPEMOD0-COUNT must be an integer");

        parameters.putSingle("X-GP-ATTRS", "1");
        parameters.putSingle("X-GP-ATTR-NAME0", "vc1");
        parameters.putSingle("X-GP-ATTR-TYPECODE0", "1043");
        parameters.putSingle("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-COUNT", "X");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-0", "42");

        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void typeModCountNegativeIntegerFails() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("ATTR-TYPEMOD0-COUNT must be a positive integer");

        parameters.putSingle("X-GP-ATTRS", "1");
        parameters.putSingle("X-GP-ATTR-NAME0", "vc1");
        parameters.putSingle("X-GP-ATTR-TYPECODE0", "1043");
        parameters.putSingle("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-COUNT", "-1");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-0", "42");

        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void typeModNonIntegerFails() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("ATTR-TYPEMOD0-0 must be an integer");

        parameters.putSingle("X-GP-ATTRS", "1");
        parameters.putSingle("X-GP-ATTR-NAME0", "vc1");
        parameters.putSingle("X-GP-ATTR-TYPECODE0", "1043");
        parameters.putSingle("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-COUNT", "1");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-0", "Y");

        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void providedTypeModsLessThanTypeModCountFails() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property ATTR-TYPEMOD0-1 has no value in the current request");

        parameters.putSingle("X-GP-ATTRS", "1");
        parameters.putSingle("X-GP-ATTR-NAME0", "vc1");
        parameters.putSingle("X-GP-ATTR-TYPECODE0", "1043");
        parameters.putSingle("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-COUNT", "2");
        parameters.putSingle("X-GP-ATTR-TYPEMOD0-0", "42");

        parser.parseRequest(mockRequestHeaders);
    }

    @Test
    public void protocolIsSetWhenProfileIsSpecified() {
        parameters.putSingle("X-GP-OPTIONS-PROFILE", "test-profile");
        when(mockPluginConf.getProtocol("test-profile")).thenReturn("test-protocol");

        RequestContext context = parser.parseRequest(mockRequestHeaders);

        assertEquals("test-protocol", context.getProtocol());

    }

    @Test
    public void whitelistedOptionsAreAddedAsProperties() {
        parameters.putSingle("X-GP-OPTIONS-PROFILE", "test-profile");
        parameters.putSingle("X-GP-OPTIONS-CONFIGPROP1", "config-prop-value1");
        parameters.putSingle("X-GP-OPTIONS-CONFIGPROP3", "config-prop-value3");
        parameters.putSingle("X-GP-OPTIONS-CONFIGPROP4", null);
        parameters.putSingle("X-GP-OPTIONS-CONFIGPROP5", "config-prop-value5");

        Map<String, String> mappings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        mappings.put("configprop1", "cfg.prop1"); // normal
        mappings.put("configprop2", "cfg.prop2"); // missing in request
        mappings.put("configprop3", "cfg.prop3"); // normal
        mappings.put("configprop4", "cfg.prop4"); // null value in request
        mappings.put("configprop5", "");          // empty mapping
        when(mockPluginConf.getOptionMappings("test-profile")).thenReturn(mappings);

        RequestContext context = parser.parseRequest(mockRequestHeaders);

        // mappings has 5 props, but only 4 are provided in the request and one has empty mapping
        assertEquals(3, context.getAdditionalConfigProps().size());
        assertEquals("config-prop-value1", context.getAdditionalConfigProps().get("cfg.prop1"));
        assertEquals("config-prop-value3", context.getAdditionalConfigProps().get("cfg.prop3"));
        assertEquals("", context.getAdditionalConfigProps().get("cfg.prop4"));

        // ensure the options are still set as well
        assertEquals("config-prop-value1", context.getOption("configprop1"));
        assertEquals("config-prop-value3", context.getOption("configprop3"));
        assertEquals("", context.getOption("configprop4"));
        assertEquals("config-prop-value5", context.getOption("configprop5"));
    }

}
