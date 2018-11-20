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

import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.HttpRequestParser;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.WriteBridge;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.servlet.ServletContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({WritableResource.class})
public class WritableResourceTest {

    private HttpRequestParser mockParser;
    private WritableResource writableResource;
    private ServletContext servletContext;
    private HttpHeaders headers;
    private InputStream inputStream;
    private MultivaluedMap<String, String> headersMap;
    private Map<String, String> params;
    private RequestContext context;
    private WriteBridge bridge;

    @Before
    public void before() throws Exception {
        mockParser = mock(HttpRequestParser.class);
        writableResource = new WritableResource(mockParser);

        // mock input
        servletContext = mock(ServletContext.class);
        headers = mock(HttpHeaders.class);
        inputStream = mock(InputStream.class);
        // mock internal functions to do nothing
        headersMap = new MultivaluedMapImpl();
        when(headers.getRequestHeaders()).thenReturn(headersMap);
        params = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        context = mock(RequestContext.class);
        bridge = mock(WriteBridge.class);
        PowerMockito.whenNew(WriteBridge.class).withArguments(context).thenReturn(bridge);
        when(context.isThreadSafe()).thenReturn(true);
        when(bridge.isThreadSafe()).thenReturn(true);
        when(mockParser.parseRequest(headers)).thenReturn(context);
    }

    @Test
    public void streamPathWithSpecialChars() throws Exception {
        // test path with special characters
        String path = "I'mso<bad>!";

        Response result = writableResource.stream(servletContext, headers, path, inputStream);

        assertEquals(Response.Status.OK,
                Response.Status.fromStatusCode(result.getStatus()));
        assertEquals("wrote 0 bulks to I.mso.bad..",
                result.getEntity().toString());
    }

    @Test
    public void streamPathWithRegularChars() throws Exception {
        // test path with regular characters
        String path = "whatCAN1tellYOU";

        Response result = writableResource.stream(servletContext, headers,
                path, inputStream);

        assertEquals(Response.Status.OK,
                Response.Status.fromStatusCode(result.getStatus()));
        assertEquals("wrote 0 bulks to " + path, result.getEntity().toString());
    }
}
