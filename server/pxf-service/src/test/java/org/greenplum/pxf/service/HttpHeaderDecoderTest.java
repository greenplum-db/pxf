package org.greenplum.pxf.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MultiValueMap;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HttpHeaderDecoderTest {

    private HttpHeaderDecoder decoder;

    private MultiValueMap<String, String> headers;
    private Map<String, List<String>> headersMap;
    private MockHttpServletRequest mockRequest;

    @BeforeEach
    public void setup() {
        decoder = new HttpHeaderDecoder();
        headersMap = new HashMap<>();
        mockRequest = new MockHttpServletRequest();
    }

    @Test
    public void testHeadersEncodedInfoSpecified() {
        headersMap.put("X-GP-ENCODED-HEADER-VALUES", Collections.singletonList("TrUe"));
        headers = CollectionUtils.toMultiValueMap(headersMap);
        assertTrue(decoder.areHeadersEncoded(headers));

        mockRequest.addHeader("X-GP-ENCODED-HEADER-VALUES", "TruE");
        assertTrue(decoder.areHeadersEncoded(mockRequest));
    }

    @Test
    public void testHeadersNotEncodedInfoSpecified() {
        headersMap.put("X-GP-ENCODED-HEADER-VALUES", Collections.singletonList("foo"));
        headers = CollectionUtils.toMultiValueMap(headersMap);
        assertFalse(decoder.areHeadersEncoded(headers));

        mockRequest.addHeader("X-GP-ENCODED-HEADER-VALUES", "foo");
        assertFalse(decoder.areHeadersEncoded(mockRequest));
    }

    @Test
    public void testHeadersNotEncodedInfoMissing() {
        headers = CollectionUtils.toMultiValueMap(headersMap);
        assertFalse(decoder.areHeadersEncoded(headers));

        assertFalse(decoder.areHeadersEncoded(mockRequest));
    }

    @Test
    public void testGetHeaderNoValues() {
        assertNull(decoder.getHeaderValue("foo", (List<String>) null, true));

        assertNull(decoder.getHeaderValue("foo", mockRequest, true));
    }

    @Test
    public void testGetHeaderListWithNoValues() {
        headersMap.put("X-GP-FOO", Collections.emptyList());
        headers = CollectionUtils.toMultiValueMap(headersMap);
        assertNull(decoder.getHeaderValue("X-GP-FOO", Collections.emptyList(), true));
    }

    @Test
    public void testGetHeaderSingleValue() {
        assertEquals("bar", decoder.getHeaderValue("X-GP-FOO", Collections.singletonList("bar"), false));

        mockRequest.addHeader("X-GP-FOO", "bar");
        assertEquals("bar", decoder.getHeaderValue("X-GP-FOO", mockRequest, false));
    }

    @Test
    public void testGetHeaderFlattenedValue() {
        assertEquals("first,second", decoder.getHeaderValue("X-GP-FOO", Arrays.asList("first", "second"), false));

        mockRequest.addHeader("X-GP-FOO", "first");
        mockRequest.addHeader("X-GP-FOO", "second");
        assertEquals("first,second", decoder.getHeaderValue("X-GP-FOO", mockRequest, false));
    }

    @Test
    public void testGetPxfHeaderDecode() {
        assertEquals("hdfs:csv", decoder.getHeaderValue("X-GP-FOO", Collections.singletonList("hdfs%3Acsv"), true));
        assertEquals("hdfs%3Acsv", decoder.getHeaderValue("X-GP-FOO", Collections.singletonList("hdfs%3Acsv"), false));

        mockRequest.addHeader("X-GP-FOO", "hdfs%3Acsv");
        assertEquals("hdfs:csv", decoder.getHeaderValue("X-GP-FOO", mockRequest, true));
        assertEquals("hdfs%3Acsv", decoder.getHeaderValue("X-GP-FOO", mockRequest, false));
    }

    @Test
    public void testGetNonPxfHeaderDecode() {
        assertEquals("hdfs%3Acsv", decoder.getHeaderValue("JUST-FOO", Collections.singletonList("hdfs%3Acsv"), true));
        assertEquals("hdfs%3Acsv", decoder.getHeaderValue("JUST-FOO", Collections.singletonList("hdfs%3Acsv"), false));

        mockRequest.addHeader("JUST-FOO", "hdfs%3Acsv");
        assertEquals("hdfs%3Acsv", decoder.getHeaderValue("JUST-FOO", mockRequest, true));
        assertEquals("hdfs%3Acsv", decoder.getHeaderValue("JUST-FOO", mockRequest, false));
    }

    @Test
    public void testMultibyteDecoding() {
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
        assertEquals("a2c\\\"UTF8_計算機用語_00000000\\\"o5",
                decoder.getHeaderValue("X-GP-FOO", Collections.singletonList(value), true));

        mockRequest.addHeader("X-GP-FOO", value);
        assertEquals("a2c\\\"UTF8_計算機用語_00000000\\\"o5",
                decoder.getHeaderValue("X-GP-FOO", mockRequest, true));
    }

}
