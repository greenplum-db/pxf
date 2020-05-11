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
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.HttpRequestParser;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.bridge.SimpleBridgeFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.DataInputStream;
import java.io.InputStream;

import static org.greenplum.pxf.api.model.RequestContext.RequestType;


/*
 * Running this resource manually:
 *
 * run:
 	curl -i -X post "http://localhost:51200/pxf/{version}/Writable/stream?path=/data/curl/curl`date \"+%h%d_%H%M%s\"`" \
 	--header "X-GP-Accessor: TextFileWAccessor" \
 	--header "X-GP-Resolver: TextWResolver" \
 	--header "Content-Type:application/octet-stream" \
 	--header "Expect: 100-continue" \
  	--header "X-GP-ALIGNMENT: 4" \
 	--header "X-GP-SEGMENT-ID: 0" \
 	--header "X-GP-SEGMENT-COUNT: 3" \
 	--header "X-GP-FORMAT: TEXT" \
 	--header "X-GP-URI: pxf://localhost:51200/data/curl/?Accessor=TextFileWAccessor&Resolver=TextWResolver" \
 	--header "X-GP-URL-HOST: localhost" \
 	--header "X-GP-URL-PORT: 51200" \
 	--header "X-GP-ATTRS: 0" \
 	--header "X-GP-DATA-DIR: data/curl/" \
 	  -d "data111" -d "data222"

 * 	result:

  	HTTP/1.1 200 OK
	Content-Type: text/plain;charset=UTF-8
	Content-Type: text/plain
	Transfer-Encoding: chunked
	Server: Jetty(7.6.10.v20130312)

	wrote 15 bytes to curlAug11_17271376231245

	file content:
	bin/hdfs dfs -cat /data/curl/*45
	data111&data222

 */

/**
 * This class handles the subpath /&lt;version&gt;/Writable/ of this REST component
 */
@RestController
@RequestMapping("/pxf/" + Version.PXF_PROTOCOL_VERSION + "/Writable/")
public class WritableResource extends BaseResource {

    private BridgeFactory bridgeFactory;

    /**
     * Creates an instance of the resource with the default singletons of RequestParser and BridgeFactory.
     */
    public WritableResource() {
        this(HttpRequestParser.getInstance(), SimpleBridgeFactory.getInstance());
    }

    /**
     * Creates an instance of the resource with provided instances of RequestParser and BridgeFactory.
     *
     * @param parser        request parser
     * @param bridgeFactory bridge factory
     */
    WritableResource(RequestParser<MultiValueMap<String, String>> parser, BridgeFactory bridgeFactory) {
        super(RequestType.WRITE_BRIDGE, parser);
        this.bridgeFactory = bridgeFactory;
    }

    /**
     * This function is called when http://nn:port/pxf/{version}/Writable/stream?path=...
     * is used.
     *
     * @param headers Holds HTTP headers from request
     * @param path    Holds URI path option used in this request
     * @param request the HttpServletRequest
     * @return ok response if the operation finished successfully
     * @throws Exception in case of wrong request parameters, failure to
     *                   initialize bridge or to write data
     */
    @PostMapping(value = "stream", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<String> stream(@RequestHeader MultiValueMap<String, String> headers,
                                         @RequestParam("path") String path,
                                         HttpServletRequest request) throws Exception {

        RequestContext context = parseRequest(headers);
        Bridge bridge = bridgeFactory.getWriteBridge(context);

        // THREAD-SAFE parameter has precedence
        boolean isThreadSafe = context.isThreadSafe() && bridge.isThreadSafe();
        LOG.debug("Request for {} will be handled {} synchronization", context.getDataSource(), (isThreadSafe ? "without" : "with"));

        return isThreadSafe ?
                writeResponse(bridge, path, request.getInputStream()) :
                synchronizedWriteResponse(bridge, path, request.getInputStream());
    }

    private ResponseEntity<String> synchronizedWriteResponse(Bridge bridge, String path, InputStream inputStream)
            throws Exception {

        // non tread-safe access will be synchronized on the class level
        synchronized (WritableResource.class) {
            return writeResponse(bridge, path, inputStream);
        }
    }

    private ResponseEntity<String> writeResponse(Bridge bridge, String path, InputStream inputStream)
            throws Exception {
        // Open the output file
        bridge.beginIteration();
        long totalWritten = 0;
        Exception ex = null;

        // dataStream will close automatically in the end of the try.
        // inputStream is closed by dataStream.close().
        try (DataInputStream dataStream = new DataInputStream(inputStream)) {
            while (bridge.setNext(dataStream)) {
                ++totalWritten;
            }
        } catch (ClientAbortException cae) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Remote connection closed by GPDB", cae);
            } else {
                LOG.error("Remote connection closed by GPDB (Enable debug for stacktrace)");
            }
        } catch (Exception e) {
            LOG.error("Exception: totalWritten so far " + totalWritten + " to " + path, e);
            ex = e;
            throw ex;
        } finally {
            try {
                bridge.endIteration();
            } catch (Exception e) {
                throw (ex == null) ? e : ex;
            }
        }

        String censuredPath = Utilities.maskNonPrintables(path);
        String returnMsg = "wrote " + totalWritten + " bulks to " + censuredPath;
        LOG.debug(returnMsg);

        return new ResponseEntity<>(returnMsg, HttpStatus.OK);
    }
}
