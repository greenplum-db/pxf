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
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.bridge.SimpleBridgeFactory;
import org.greenplum.pxf.service.HttpRequestParser;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.api.io.Writable;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.locks.ReentrantLock;

/*
 * This class handles the subpath /<version>/Bridge/ of this
 * REST component
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Bridge/")
public class BridgeResource extends BaseResource {

    private BridgeFactory bridgeFactory;

    /**
     * Lock is needed here in the case of a non-thread-safe plugin. Using
     * synchronized methods is not enough because the bridge work is called by
     * jetty ({@link StreamingOutput}), after we are getting out of this class's
     * context.
     * <p/>
     * BRIDGE_LOCK is accessed through lock() and unlock() functions, based on
     * the isThreadSafe parameter that is determined by the bridge.
     */
    private static final ReentrantLock BRIDGE_LOCK = new ReentrantLock();

    /**
     * Creates an instance of the resource with the default singletons of RequestParser and BridgeFactory.
     */
    public BridgeResource() {
        this(HttpRequestParser.getInstance(), SimpleBridgeFactory.getInstance());
    }

    /**
     * Creates an instance of the resource with provided instances of RequestParser and BridgeFactory.
     * @param parser request parser
     * @param bridgeFactory bridge factory
     */
    BridgeResource(RequestParser<HttpHeaders> parser, BridgeFactory bridgeFactory) {
        super(parser);
        this.bridgeFactory = bridgeFactory;
    }

    /**
     * Handles read data request. Parses the request, creates a bridge instance and iterates over its
     * records, printing it out to the outgoing stream. Outputs GPDBWritable or Text formats.
     *
     * Parameters come via HTTP headers.
     *
     * @param servletContext Servlet context contains attributes required by SecuredHDFS
     * @param headers Holds HTTP headers from request
     * @return response object containing stream that will output records
     * @throws Exception in case of wrong request parameters, or failure to initialize a bridge
     */
    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response read(@Context final ServletContext servletContext,
                         @Context HttpHeaders headers) throws Exception {

        RequestContext context = parseRequest(headers);
        Bridge bridge = bridgeFactory.getReadBridge(context);

        // THREAD-SAFE parameter has precedence
        boolean isThreadSafe = context.isThreadSafe() && bridge.isThreadSafe();
        LOG.debug("Request for {} will be handled {} synchronization", context.getDataSource(), (isThreadSafe ? "without" : "with"));

        return readResponse(bridge, context, isThreadSafe);
    }

    /**
     * Produces streaming Response used by the container to read data from the bridge.
     * @param bridge bridge to use to read data
     * @param context request context
     * @param threadSafe whether streaming can proceed in parallel
     * @return response object to be used by the container
     */
    private Response readResponse(final Bridge bridge, RequestContext context, final boolean threadSafe) {
        final int fragment = context.getDataFragment();
        final String dataDir = context.getDataSource();

        // Creating an internal streaming class which will iterate
        // the records and put them on the output stream
        final StreamingOutput streaming = new StreamingOutput() {
            @Override
            public void write(final OutputStream out) throws IOException,
                    WebApplicationException {
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
                    // Occurs whenever client (GPDB) decides the end the connection
                    LOG.error("Remote connection closed by GPDB", e);
                } catch (Exception e) {
                    LOG.error("Exception thrown when streaming", e);
                    throw new IOException(e.getMessage());
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
            }
        };

        return Response.ok(streaming, MediaType.APPLICATION_OCTET_STREAM).build();
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
