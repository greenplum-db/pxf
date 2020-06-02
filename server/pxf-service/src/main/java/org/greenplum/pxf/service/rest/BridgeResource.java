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
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.security.SecurityService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;

/*
 * This class handles the subpath /<version>/Bridge/ of this
 * REST component
 */
@RestController
@RequestMapping("/pxf/" + Version.PXF_PROTOCOL_VERSION)
public class BridgeResource extends BaseResource {

    private final BridgeFactory bridgeFactory;

    private final SecurityService securityService;

    public BridgeResource(BridgeFactory bridgeFactory, SecurityService securityService) {
        super(RequestContext.RequestType.READ_BRIDGE);
        this.bridgeFactory = bridgeFactory;
        this.securityService = securityService;
    }

    /**
     * Handles read data request. Parses the request, creates a bridge instance and iterates over its
     * records, printing it out to the outgoing stream. Outputs GPDBWritable or Text formats.
     * <p>
     * Parameters come via HTTP headers.
     *
     * @param headers Holds HTTP headers from request
     * @return response object containing stream that will output records
     */
    @GetMapping(value = "/Bridge", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> read(
            @RequestHeader MultiValueMap<String, String> headers) throws IOException {

        RequestContext context = parseRequest(headers);
        Bridge bridge = bridgeFactory.getBridge(context);

        // THREAD-SAFE parameter has precedence
        boolean isThreadSafe = context.isThreadSafe() && bridge.isThreadSafe();
        LOG.debug("Request for {} will be handled {} synchronization", context.getDataSource(), (isThreadSafe ? "without" : "with"));

        // Create a streaming class which will iterate over the records and put
        // them on the output stream
        StreamingResponseBody response =
                new BridgeResponse(securityService, bridge, context, isThreadSafe);

        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
