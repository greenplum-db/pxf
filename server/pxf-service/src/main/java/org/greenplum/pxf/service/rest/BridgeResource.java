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

import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.HttpRequestParser;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.bridge.SimpleBridgeFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.DataOutputStream;
import java.io.IOException;

/*
 * This class handles the subpath /<version>/Bridge/ of this
 * REST component
 */
@RestController
@RequestMapping("/pxf/" + Version.PXF_PROTOCOL_VERSION)
public class BridgeResource extends BaseResource {

    private BridgeFactory bridgeFactory;

    /**
     * Creates an instance of the resource with the default singletons of RequestParser and BridgeFactory.
     */
    public BridgeResource() {
        this(HttpRequestParser.getInstance(), SimpleBridgeFactory.getInstance());
    }

    /**
     * Creates an instance of the resource with provided instances of RequestParser and BridgeFactory.
     *
     * @param parser        request parser
     * @param bridgeFactory bridge factory
     */
    BridgeResource(RequestParser<MultiValueMap<String, String>> parser, BridgeFactory bridgeFactory) {
        super(RequestContext.RequestType.READ_BRIDGE, parser);
        this.bridgeFactory = bridgeFactory;
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
            @RequestHeader MultiValueMap<String, String> headers) {

        RequestContext context = parseRequest(headers);
        Bridge bridge = bridgeFactory.getReadBridge(context);

        final int fragment = context.getDataFragment();
        final String dataDir = context.getDataSource();

        // Creating an internal streaming class which will iterate
        // the records and put them on the output stream
        StreamingResponseBody streaming = out -> {
            long recordCount = 0;

            try {
                if (!bridge.beginIteration()) {
                    return;
                }
                Writable record;
                DataOutputStream dos = new DataOutputStream(out);

                LOG.debug("Starting streaming fragment {} of resource {}", fragment, dataDir);
                while ((record = bridge.getNext()) != null) {
                    record.write(dos);
                    ++recordCount;
                }
                LOG.debug("Finished streaming fragment {} of resource {}, {} records.", fragment, dataDir, recordCount);
            } catch (ClientAbortException e) {
                // Occurs whenever client (GPDB) decides to end the connection
                if (LOG.isDebugEnabled()) {
                    // Stacktrace in debug
                    LOG.debug("Remote connection closed by GPDB", e);
                } else {
                    LOG.error("Remote connection closed by GPDB (Enable debug for stacktrace)");
                }
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            } finally {
                LOG.debug("Stopped streaming fragment {} of resource {}, {} records.", fragment, dataDir, recordCount);
                try {
                    bridge.endIteration();
                } catch (Exception e) {
                    // ignore ... any significant errors should already have been handled
                }
            }
        };

        return new ResponseEntity<>(streaming, HttpStatus.OK);
    }
}
