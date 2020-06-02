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


import org.greenplum.pxf.api.examples.DemoFragmentMetadata;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.PluginConf;
import org.greenplum.pxf.api.model.ProtocolHandler;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.RequestContext.RequestType;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpRequestParserTest {

    private HttpRequestParser parser;
    private MultiValueMap<String, String> parameters;
    private PluginConf mockPluginConf;

    @BeforeEach
    public void setUp() {
        mockPluginConf = mock(PluginConf.class);

        parameters = new LinkedMultiValueMap<>();
        parameters.add("X-GP-ALIGNMENT", "all");
        parameters.add("X-GP-SEGMENT-ID", "-44");
        parameters.add("X-GP-SEGMENT-COUNT", "2");
        parameters.add("X-GP-HAS-FILTER", "0");
        parameters.add("X-GP-FORMAT", "TEXT");
        parameters.add("X-GP-URL-HOST", "my://bags");
        parameters.add("X-GP-URL-PORT", "-8020");
        parameters.add("X-GP-ATTRS", "-1");
        parameters.add("X-GP-OPTIONS-ACCESSOR", "are");
        parameters.add("X-GP-OPTIONS-RESOLVER", "packed");
        parameters.add("X-GP-DATA-DIR", "i'm/ready/to/go");
        parameters.add("X-GP-FRAGMENT-METADATA", "{\"path\": \"i'm a json\", \"className\": \"org.greenplum.pxf.api.examples.DemoFragmentMetadata\"}");
        parameters.add("X-GP-OPTIONS-I'M-STANDING-HERE", "outside-your-door");
        parameters.add("X-GP-USER", "alex");
        parameters.add("X-GP-OPTIONS-SERVER", "custom_server");
        parameters.add("X-GP-XID", "transaction:id");

        parser = new HttpRequestParser(mockPluginConf, new RequestContext());
    }

    @AfterEach
    public void tearDown() {
        // Cleanup the system property RequestContext sets
        System.clearProperty("greenplum.alignment");
    }

    @Test
    public void testConvertToCaseInsensitiveMap() {
        List<String> multiCaseKeys = Arrays.asList("X-GP-SHLOMO", "x-gp-shlomo", "X-Gp-ShLoMo");
        String value = "\\\"The king";
        String replacedValue = "\"The king";

        MultiValueMap<String, String> multivaluedMap = new LinkedMultiValueMap<>();
        for (String key : multiCaseKeys) {
            multivaluedMap.put(key, Collections.singletonList(value));
        }

        assertEquals(multivaluedMap.keySet().size(), multiCaseKeys.size(), "All keys should have existed");

        Map<String, String> caseInsensitiveMap = new HttpRequestParser.RequestMap(multivaluedMap);

        assertEquals(caseInsensitiveMap.keySet().size(), 1, "Only one key should have exist");

        for (String key : multiCaseKeys) {
            assertEquals(caseInsensitiveMap.get(key), replacedValue, "All keys should have returned the same value");
        }
    }

    @Test
    public void testConvertToCaseInsensitiveMapUtf8() {
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
        String value = new String(bytes, StandardCharsets.ISO_8859_1);

        MultiValueMap<String, String> multivaluedMap = new LinkedMultiValueMap<>();
        multivaluedMap.put("one", Collections.singletonList(value));

        Map<String, String> caseInsensitiveMap = new HttpRequestParser.RequestMap(multivaluedMap);

        assertEquals(caseInsensitiveMap.keySet().size(), 1, "Only one key should have exist");

        assertEquals(caseInsensitiveMap.get("one"), "a2c\"UTF8_計算機用語_00000000\"o5", "Value should be converted to UTF-8");
    }

    @Test
    public void contextCreated() {

        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);

        assertEquals(System.getProperty("greenplum.alignment"), "all");
        assertEquals(context.getTotalSegments(), 2);
        assertEquals(context.getSegmentId(), -44);
        assertEquals(context.getOutputFormat(), OutputFormat.TEXT);
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
        assertNull(context.getProfileScheme());
        assertTrue(context.getAdditionalConfigProps().isEmpty());
        assertTrue(context.getFragmentMetadata() instanceof DemoFragmentMetadata);
        assertEquals("i'm a json", ((DemoFragmentMetadata) context.getFragmentMetadata()).getPath());
        assertFalse(context.isLastFragment());
    }

    @Test
    public void profileWithDuplicateProperty() {

        Map<String, String> mockedPlugins = new HashMap<>();
        // What we read from the XML Plugins file
        mockedPlugins.put("wHEn you trY yOUR bESt", "but you dont succeed");
        mockedPlugins.put("when YOU get WHAT you WANT",
                "but not what you need");
        mockedPlugins.put("when you feel so tired", "but you cant sleep");

        when(mockPluginConf.getPlugins("test-profile")).thenReturn(mockedPlugins);

        // Parameters that are coming from the request
        parameters.add("x-gp-options-profile", "test-profile");
        parameters.add("x-gp-options-when you try your best", "and you do succeed");
        parameters.add("x-gp-options-WHEN you GET what YOU want", "and what you need");

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("Profile 'test-profile' already defines: [when YOU get WHAT you WANT, wHEn you trY yOUR bESt]", e.getMessage());
    }

    @Test
    public void pluginsDefinedByProfileOnly() {
        Map<String, String> mockedPlugins = new HashMap<>();
        mockedPlugins.put("fragmenter", "test-fragmenter");
        mockedPlugins.put("accessor", "test-accessor");
        mockedPlugins.put("resolver", "test-resolver");
        when(mockPluginConf.getPlugins("test-profile")).thenReturn(mockedPlugins);

        // add profile and remove plugin parameters
        parameters.add("X-GP-OPTIONS-PROFILE", "test-profile");
        parameters.remove("X-GP-OPTIONS-ACCESSOR");
        parameters.remove("X-GP-OPTIONS-RESOLVER");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals(context.getFragmenter(), "test-fragmenter");
        assertEquals(context.getAccessor(), "test-accessor");
        assertEquals(context.getResolver(), "test-resolver");
    }

    @Test
    public void pluginsRedefinedByProfileFails() {
        Map<String, String> mockedPlugins = new HashMap<>();
        mockedPlugins.put("fragmenter", "test-fragmenter");
        mockedPlugins.put("accessor", "test-accessor");
        mockedPlugins.put("resolver", "test-resolver");
        when(mockPluginConf.getPlugins("test-profile")).thenReturn(mockedPlugins);

        // add profile in addition to plugins
        parameters.add("X-GP-OPTIONS-PROFILE", "test-profile");

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("Profile 'test-profile' already defines: [resolver, accessor]", e.getMessage());
    }

    @Test
    public void undefinedServer() {
        parameters.remove("X-GP-OPTIONS-SERVER");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals("default", context.getServerName());
    }

    @Test
    public void threadSafeTrue() {
        parameters.set("X-GP-OPTIONS-THREAD-SAFE", "TRUE");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertTrue(context.isThreadSafe());

        parameters.set("X-GP-OPTIONS-THREAD-SAFE", "true");
        context = new HttpRequestParser(mockPluginConf, new RequestContext()).parseRequest(parameters, RequestType.FRAGMENTER);
        assertTrue(context.isThreadSafe());
    }

    @Test
    public void threadSafeFalse() {
        parameters.add("X-GP-OPTIONS-THREAD-SAFE", "False");
        RequestContext context = new HttpRequestParser(mockPluginConf, new RequestContext()).parseRequest(parameters, RequestType.FRAGMENTER);
        assertFalse(context.isThreadSafe());

        parameters.set("X-GP-OPTIONS-THREAD-SAFE", "falSE");
        context = new HttpRequestParser(mockPluginConf, new RequestContext()).parseRequest(parameters, RequestType.FRAGMENTER);
        assertFalse(context.isThreadSafe());
    }

    @Test
    public void threadSafeMaybe() {
        parameters.add("X-GP-OPTIONS-THREAD-SAFE", "maybe");

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("Illegal boolean value 'maybe'. Usage: [TRUE|FALSE]", e.getMessage());
    }

    @Test
    public void threadSafeDefault() {
        parameters.remove("X-GP-OPTIONS-THREAD-SAFE");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertTrue(context.isThreadSafe());
    }

    @Test
    public void getFragmentMetadata() {
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        FragmentMetadata metadata = context.getFragmentMetadata();
        assertTrue(metadata instanceof DemoFragmentMetadata);
        assertEquals("i'm a json", ((DemoFragmentMetadata) metadata).getPath());
    }

    @Test
    public void getFragmentMetadataNull() {
        parameters.remove("X-GP-FRAGMENT-METADATA");
        RequestContext requestContext = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertNull(requestContext.getFragmentMetadata());
    }

    @Test
    public void getFragmentMetadataNotBase64() {

        String badValue = "so b@d";
        parameters.set("X-GP-FRAGMENT-METADATA", badValue);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("unable to deserialize fragment meta 'so b@d'", e.getMessage());
    }

    @Test
    public void nullUserThrowsException() {

        parameters.remove("X-GP-USER");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("Property USER has no value in the current request", e.getMessage());
    }

    @Test
    public void filterUtf8() {
        parameters.remove("X-GP-HAS-FILTER");
        parameters.add("X-GP-HAS-FILTER", "1");
        String isoString = new String("UTF8_計算機用語_00000000".getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
        parameters.add("X-GP-FILTER", isoString);
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertTrue(context.hasFilter());
        assertEquals("UTF8_計算機用語_00000000", context.getFilterString());
    }

    @Test
    public void statsParams() {
        parameters.add("X-GP-OPTIONS-STATS-MAX-FRAGMENTS", "10101");
        parameters.add("X-GP-OPTIONS-STATS-SAMPLE-RATIO", "0.039");

        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);

        assertEquals(10101, context.getStatsMaxFragments());
        assertEquals(0.039, context.getStatsSampleRatio(), 0.01);
    }

    @Test
    public void testInvalidStatsSampleRatioValue() {
        parameters.add("X-GP-OPTIONS-STATS-SAMPLE-RATIO", "a");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("For input string: \"a\"", e.getMessage());
    }

    @Test
    public void testInvalidStatsMaxFragmentsValue() {
        parameters.add("X-GP-OPTIONS-STATS-MAX-FRAGMENTS", "10.101");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("For input string: \"10.101\"", e.getMessage());
    }

    @Test
    public void typeMods() {

        parameters.set("X-GP-ATTRS", "2");
        parameters.set("X-GP-ATTR-NAME0", "vc1");
        parameters.set("X-GP-ATTR-TYPECODE0", "1043");
        parameters.set("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.set("X-GP-ATTR-TYPEMOD0-COUNT", "1");
        parameters.set("X-GP-ATTR-TYPEMOD0-0", "5");

        parameters.set("X-GP-ATTR-NAME1", "dec1");
        parameters.set("X-GP-ATTR-TYPECODE1", "1700");
        parameters.set("X-GP-ATTR-TYPENAME1", "numeric");
        parameters.set("X-GP-ATTR-TYPEMOD1-COUNT", "2");
        parameters.set("X-GP-ATTR-TYPEMOD1-0", "10");
        parameters.set("X-GP-ATTR-TYPEMOD1-1", "2");

        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);

        assertArrayEquals(new Integer[]{5}, context.getColumn(0).columnTypeModifiers());
        assertArrayEquals(new Integer[]{10, 2}, context.getColumn(1).columnTypeModifiers());
    }

    @Test
    public void typeModCountNonIntegerFails() {
        parameters.set("X-GP-ATTRS", "1");
        parameters.set("X-GP-ATTR-NAME0", "vc1");
        parameters.set("X-GP-ATTR-TYPECODE0", "1043");
        parameters.set("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.set("X-GP-ATTR-TYPEMOD0-COUNT", "X");
        parameters.set("X-GP-ATTR-TYPEMOD0-0", "42");

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("ATTR-TYPEMOD0-COUNT must be an integer", e.getMessage());
    }

    @Test
    public void typeModCountNegativeIntegerFails() {
        parameters.set("X-GP-ATTRS", "1");
        parameters.set("X-GP-ATTR-NAME0", "vc1");
        parameters.set("X-GP-ATTR-TYPECODE0", "1043");
        parameters.set("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.set("X-GP-ATTR-TYPEMOD0-COUNT", "-1");
        parameters.set("X-GP-ATTR-TYPEMOD0-0", "42");

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("ATTR-TYPEMOD0-COUNT must be a positive integer", e.getMessage());
    }

    @Test
    public void typeModNonIntegerFails() {
        parameters.set("X-GP-ATTRS", "1");
        parameters.set("X-GP-ATTR-NAME0", "vc1");
        parameters.set("X-GP-ATTR-TYPECODE0", "1043");
        parameters.set("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.set("X-GP-ATTR-TYPEMOD0-COUNT", "1");
        parameters.set("X-GP-ATTR-TYPEMOD0-0", "Y");

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("ATTR-TYPEMOD0-0 must be an integer", e.getMessage());
    }

    @Test
    public void providedTypeModsLessThanTypeModCountFails() {

        parameters.set("X-GP-ATTRS", "1");
        parameters.set("X-GP-ATTR-NAME0", "vc1");
        parameters.set("X-GP-ATTR-TYPECODE0", "1043");
        parameters.set("X-GP-ATTR-TYPENAME0", "varchar");
        parameters.set("X-GP-ATTR-TYPEMOD0-COUNT", "2");
        parameters.set("X-GP-ATTR-TYPEMOD0-0", "42");

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("Property ATTR-TYPEMOD0-1 has no value in the current request", e.getMessage());
    }

    @Test
    public void protocolIsSetWhenProfileIsSpecified() {
        parameters.add("X-GP-OPTIONS-PROFILE", "test-profile");
        when(mockPluginConf.getProtocol("test-profile")).thenReturn("test-protocol");

        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);

        assertEquals("test-protocol", context.getProfileScheme());

    }

    @Test
    public void whitelistedOptionsAreAddedAsProperties() {
        parameters.set("X-GP-OPTIONS-PROFILE", "test-profile");
        parameters.set("X-GP-OPTIONS-CONFIGPROP1", "config-prop-value1");
        parameters.set("X-GP-OPTIONS-CONFIGPROP3", "config-prop-value3");
        parameters.set("X-GP-OPTIONS-CONFIGPROP4", "");
        parameters.set("X-GP-OPTIONS-CONFIGPROP5", "config-prop-value5");

        Map<String, String> mappings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        mappings.put("configprop1", "cfg.prop1"); // normal
        mappings.put("configprop2", "cfg.prop2"); // missing in request
        mappings.put("configprop3", "cfg.prop3"); // normal
        mappings.put("configprop4", "cfg.prop4"); // empty value in request
        mappings.put("configprop5", "");          // empty mapping
        when(mockPluginConf.getOptionMappings("test-profile")).thenReturn(mappings);

        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);

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

    @Test
    public void testWireFormatIsAbsent() {
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals(OutputFormat.TEXT, context.getOutputFormat());
        assertNull(context.getFormat());
    }

    @Test
    public void testWireFormatIsPresentAndFormatIsInferred() {
        parameters.set("X-GP-FORMAT", "GPDBWritable");
        parameters.add("X-GP-OPTIONS-PROFILE", "foo:bar");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals(OutputFormat.GPDBWritable, context.getOutputFormat());
        assertEquals("bar", context.getFormat());
    }

    @Test
    public void testWireFormatIsPresentAndFormatIsInferredToNothing() {
        parameters.set("X-GP-FORMAT", "GPDBWritable");
        parameters.add("X-GP-OPTIONS-PROFILE", "foobar");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals(OutputFormat.GPDBWritable, context.getOutputFormat());
        assertNull(context.getFormat());
    }

    @Test
    public void testWireFormatIsPresent() {
        parameters.set("X-GP-FORMAT", "TEXT");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals(OutputFormat.TEXT, context.getOutputFormat());
        assertNull(context.getFormat());
    }

    @Test
    public void testWireFormatAndFormatArePresent() {
        // wire format
        parameters.set("X-GP-FORMAT", "TEXT");
        // data format
        parameters.add("X-GP-OPTIONS-FORMAT", "foobar");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals(OutputFormat.TEXT, context.getOutputFormat());
        assertEquals("foobar", context.getFormat());
    }

    @Test
    public void testHandlerIsCalled() {
        when(mockPluginConf.getHandler("test-profile")).thenReturn(TestHandler.class.getName());
        parameters.add("X-GP-OPTIONS-PROFILE", "test-profile");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals("overridden-fragmenter", context.getFragmenter());
        assertEquals("overridden-accessor", context.getAccessor());
        assertEquals("overridden-resolver", context.getResolver());
    }

    @Test
    public void testHandlerIsNotCalledWhenNotDefined() {
        when(mockPluginConf.getHandler("test-profile")).thenReturn(null);
        parameters.add("X-GP-OPTIONS-PROFILE", "test-profile");
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals("are", context.getAccessor());
        assertEquals("packed", context.getResolver());
    }

    @Test
    public void testInvalidHandlerCausesException() {
        when(mockPluginConf.getHandler("test-profile")).thenReturn("foo");
        parameters.add("X-GP-OPTIONS-PROFILE", "test-profile");
        Exception e = assertThrows(RuntimeException.class,
                () -> parser.parseRequest(parameters, RequestType.FRAGMENTER));
        assertEquals("Error when invoking handlerClass 'foo' : java.lang.ClassNotFoundException: foo", e.getMessage());
    }

    @Test
    public void testWritePath() {
        RequestContext context = parser.parseRequest(parameters, RequestType.WRITE_BRIDGE);
        assertEquals(RequestType.WRITE_BRIDGE, context.getRequestType());
    }

    @Test
    public void testFragmenterPath() {
        RequestContext context = parser.parseRequest(parameters, RequestType.FRAGMENTER);
        assertEquals(RequestType.FRAGMENTER, context.getRequestType());
    }

    @Test
    public void testReadPath() {
        RequestContext context = parser.parseRequest(parameters, RequestType.READ_BRIDGE);
        assertEquals(RequestType.READ_BRIDGE, context.getRequestType());
    }

    @Test
    public void testEncodedHeaderValuesIsFalse() throws UnsupportedEncodingException {
        parameters.remove("X-GP-DATA-DIR");
        parameters.add("X-GP-DATA-DIR", URLEncoder.encode("\u0001", "UTF-8"));
        parameters.add("X-GP-ENCODED-HEADER-VALUES", "false");

        RequestContext context = parser.parseRequest(parameters, RequestType.READ_BRIDGE);
        assertEquals("%01", context.getDataSource());

        // non encoded fields shouldn't be affected
        assertEquals(context.getAccessor(), "are");
        assertEquals(context.getResolver(), "packed");
        assertEquals(context.getOption("i'm-standing-here"), "outside-your-door");
        assertEquals(context.getUser(), "alex");
    }

    @Test
    public void testEncodedHeaderValuesIsTrue() throws UnsupportedEncodingException {
        parameters.remove("X-GP-DATA-DIR");
        parameters.add("X-GP-DATA-DIR", URLEncoder.encode("\u0001", "UTF-8"));
        parameters.add("X-GP-ENCODED-HEADER-VALUES", "trUe");

        RequestContext context = parser.parseRequest(parameters, RequestType.READ_BRIDGE);
        assertEquals("\u0001", context.getDataSource());

        // non encoded fields shouldn't be affected
        assertEquals(context.getAccessor(), "are");
        assertEquals(context.getResolver(), "packed");
        assertEquals(context.getOption("i'm-standing-here"), "outside-your-door");
        assertEquals(context.getUser(), "alex");
    }

    @Test
    public void testLastFragment() {
        parameters.add("X-GP-LAST-FRAGMENT", "true");

        RequestContext context = parser.parseRequest(parameters, RequestType.READ_BRIDGE);
        assertTrue(context.isLastFragment());
    }

    static class TestHandler implements ProtocolHandler {

        @Override
        public String getFragmenterClassName(RequestContext context) {
            return "overridden-fragmenter";
        }

        @Override
        public String getAccessorClassName(RequestContext context) {
            return "overridden-accessor";
        }

        @Override
        public String getResolverClassName(RequestContext context) {
            return "overridden-resolver";
        }
    }
}
