package org.greenplum.pxf.service.rest;
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

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.RequestContext.RequestType;
import org.greenplum.pxf.service.HttpRequestParser;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.bridge.WriteBridge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WritableResourceTest {

    private WritableResource writableResource;

    // input parameters
    private MultiValueMap<String, String> mockHeaders;
    private HttpServletRequest mockHttpServletRequest;

    @BeforeEach
    public void before() throws IOException {

        // constructor dependencies
        HttpRequestParser mockParser = mock(HttpRequestParser.class);
        BridgeFactory mockFactory = mock(BridgeFactory.class);
        mockHeaders = mock(MultiValueMap.class);
        ServletInputStream mockInputStream = mock(ServletInputStream.class);
        RequestContext mockContext = mock(RequestContext.class);
        WriteBridge mockBridge = mock(WriteBridge.class);
        mockHttpServletRequest = mock(HttpServletRequest.class);

        writableResource = new WritableResource(mockParser, mockFactory);

        when(mockParser.parseRequest(mockHeaders, RequestType.WRITE_BRIDGE)).thenReturn(mockContext);
        when(mockFactory.getWriteBridge(mockContext)).thenReturn(mockBridge);
        when(mockContext.isThreadSafe()).thenReturn(true);
        when(mockBridge.isThreadSafe()).thenReturn(true);
        when(mockHttpServletRequest.getInputStream()).thenReturn(mockInputStream);
    }

    @Test
    public void streamPathWithSpecialChars() throws Exception {
        // test path with special characters
        String path = "I'mso<bad>!";
        ResponseEntity<String> result = writableResource.stream(mockHeaders, path, mockHttpServletRequest);

        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertEquals("wrote 0 bulks to I.mso.bad..", result.getBody());
    }

    @Test
    public void streamPathWithRegularChars() throws Exception {
        // test path with regular characters
        String path = "whatCAN1tellYOU";
        ResponseEntity<String> result = writableResource.stream(mockHeaders, path, mockHttpServletRequest);

        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertEquals("wrote 0 bulks to " + path, result.getBody());
    }
}
