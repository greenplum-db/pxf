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
import org.greenplum.pxf.service.HttpRequestParser;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.bridge.WriteBridge;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.ServletContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WritableResourceTest {

    private WritableResource writableResource;

    // constructor dependencies
    @Mock private HttpRequestParser mockParser;
    @Mock private BridgeFactory mockFactory;

    // input parameters
    @Mock private ServletContext mockServletContext;
    @Mock private HttpHeaders mockHeaders;
    @Mock private InputStream mockInputStream;

    @Mock private RequestContext mockContext;
    @Mock private WriteBridge mockBridge;

    @Before
    public void before() throws Exception {

        writableResource = new WritableResource(mockParser, mockFactory);

        when(mockParser.parseRequest(mockHeaders)).thenReturn(mockContext);
        when(mockFactory.getWriteBridge(mockContext)).thenReturn(mockBridge);
        when(mockContext.isThreadSafe()).thenReturn(true);
        when(mockBridge.isThreadSafe()).thenReturn(true);
    }

    @Test
    public void streamPathWithSpecialChars() throws Exception {
        // test path with special characters
        String path = "I'mso<bad>!";
        Response result = writableResource.stream(mockServletContext, mockHeaders, path, mockInputStream);

        assertEquals(Response.Status.OK, Response.Status.fromStatusCode(result.getStatus()));
        assertEquals("wrote 0 bulks to I.mso.bad..", result.getEntity().toString());
    }

    @Test
    public void streamPathWithRegularChars() throws Exception {
        // test path with regular characters
        String path = "whatCAN1tellYOU";
        Response result = writableResource.stream(mockServletContext, mockHeaders, path, mockInputStream);

        assertEquals(Response.Status.OK, Response.Status.fromStatusCode(result.getStatus()));
        assertEquals("wrote 0 bulks to " + path, result.getEntity().toString());
    }
}
