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
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.controller.ReadService;
import org.greenplum.pxf.service.controller.WriteService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.servlet.http.HttpServletRequest;

import static org.greenplum.pxf.api.model.RequestContext.RequestType;

/**
 * PXF REST endpoint mapped to /pxf path
 */
@RestController
@RequestMapping("/pxf")
public class PxfResource {

    private final RequestParser<MultiValueMap<String, String>> parser;
    private final ReadService readService;
    private final WriteService writeService;

    /**
     * Creates a new instance of the resource with Request parser and read service implementation.
     *
     * @param parser       http request parser
     * @param readService  read service implementation
     * @param writeService write service implementation
     */
    public PxfResource(RequestParser<MultiValueMap<String, String>> parser,
                       ReadService readService,
                       WriteService writeService) {
        this.parser = parser;
        this.readService = readService;
        this.writeService = writeService;
    }

    /**
     * REST endpoint for read data requests.
     *
     * @param headers http headers from request that carry all parameters
     * @return response object containing stream that will output records
     */
    @GetMapping(value = "/read", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> read(
            @RequestHeader MultiValueMap<String, String> headers) {

        // parse incoming HTTP request
        RequestContext context = parser.parseRequest(headers, RequestType.READ_BRIDGE);

        // create a streaming class that will iterate over the records and write them to the output stream
        StreamingResponseBody response = os -> readService.readData(context, os);

        // return response entity that will use streaming response asynchronously
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    /**
     * REST endpoint for write data requests.
     *
     * @param headers http headers from request that carry all parameters
     * @param request the HttpServletRequest
     * @return ok response if the operation finished successfully
     * @throws Exception in case of wrong request parameters or failure to write data
     */
    @PostMapping(value = "/write", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<String> stream(@RequestHeader MultiValueMap<String, String> headers,
                                         HttpServletRequest request) throws Exception {

        // parse incoming HTTP request
        RequestContext context = parser.parseRequest(headers, RequestType.WRITE_BRIDGE);

        // write data and get a response message
        String returnMsg = writeService.writeData(context, request.getInputStream());

        // send the response to the client
        return new ResponseEntity<>(returnMsg, HttpStatus.OK);
    }
}
