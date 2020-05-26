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
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
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
import java.util.concurrent.locks.ReentrantLock;

/*
 * This class handles the subpath /<version>/Bridge/ of this
 * REST component
 */
@RestController
@RequestMapping("/pxf/" + Version.PXF_PROTOCOL_VERSION)
public class BridgeResource extends BaseResource {

    /**
     * Lock is needed here in the case of a non-thread-safe plugin. Using
     * synchronized methods is not enough because the bridge work is called by
     * {@link StreamingResponseBody}, after we are getting out of this class's
     * context.
     * <p/>
     * BRIDGE_LOCK is accessed through lock() and unlock() functions, based on
     * the isThreadSafe parameter that is determined by the bridge.
     */
    private static final ReentrantLock BRIDGE_LOCK = new ReentrantLock();

    private final BridgeFactory bridgeFactory;

    public BridgeResource(BridgeFactory bridgeFactory) {
        super(RequestContext.RequestType.READ_BRIDGE);
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
        Bridge bridge = bridgeFactory.getBridge(context);

        // THREAD-SAFE parameter has precedence
        boolean isThreadSafe = context.isThreadSafe() && bridge.isThreadSafe();
        LOG.debug("Request for {} will be handled {} synchronization", context.getDataSource(), (isThreadSafe ? "without" : "with"));

        return readResponse(bridge, context, isThreadSafe);
    }

    /**
     * Produces streaming Response used by the container to read data from the bridge.
     *
     * @param bridge     bridge to use to read data
     * @param context    request context
     * @param threadSafe whether streaming can proceed in parallel
     * @return response object to be used by the container
     */
    private ResponseEntity<StreamingResponseBody> readResponse(final Bridge bridge, RequestContext context, final boolean threadSafe) {
        final int fragment = context.getDataFragment();
        final String dataDir = context.getDataSource();

        // Creating an internal streaming class which will iterate
        // the records and put them on the output stream
        StreamingResponseBody streaming = out -> {
            long recordCount = 0;

            if (!threadSafe) {
                lock(dataDir);
            }
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
                if (!threadSafe) {
                    unlock(dataDir);
                }
            }
        };

        return new ResponseEntity<>(streaming, HttpStatus.OK);
    }

    /**
     * Locks BRIDGE_LOCK
     *
     * @param path path for the request, used for logging.
     */
    private void lock(String path) {
        LOG.trace("Locking BridgeResource for {}", path);
        BRIDGE_LOCK.lock();
        LOG.trace("Locked BridgeResource for {}", path);
    }

    /**
     * Unlocks BRIDGE_LOCK
     *
     * @param path path for the request, used for logging.
     */
    private void unlock(String path) {
        LOG.trace("Unlocking BridgeResource for {}", path);
        BRIDGE_LOCK.unlock();
        LOG.trace("Unlocked BridgeResource for {}", path);
    }
}
