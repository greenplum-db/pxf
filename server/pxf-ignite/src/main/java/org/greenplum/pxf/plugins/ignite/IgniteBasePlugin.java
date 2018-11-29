package org.greenplum.pxf.plugins.ignite;

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

import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;

/**
 * PXF-Ignite base class.
 * This class manages the user-defined parameters provided in the query from PXF.
 * Implemented subclasses: {@link IgniteAccessor}, {@link IgniteResolver}.
 */
public class IgniteBasePlugin extends BasePlugin {
    // Ignite cache
    protected static final String igniteHostDefault = "127.0.0.1:8080";
    protected String igniteHost = null;
    // PXF buffer for Ignite data. '0' is allowed for INSERT queries
    protected static final int bufferSizeDefault = 128;
    protected int bufferSize = bufferSizeDefault;
    // Ignite cache name
    protected String cacheName = null;


    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        LOG.debug("Initializer started");
        igniteHost = requestContext.getOption("IGNITE_HOST");
        if (igniteHost == null) {
            igniteHost = igniteHostDefault;
        }

        cacheName = requestContext.getOption("IGNITE_CACHE");
        // If this value is null, Ignite will use the default cache

        String bufferSize_str = requestContext.getOption("BUFFER_SIZE");
        if (bufferSize_str != null) {
            try {
                bufferSize = Integer.parseInt(bufferSize_str);
                // Zero value is allowed for INSERT queries
                if (bufferSize < 0) {
                    bufferSize = bufferSizeDefault;
                    LOG.warn("Buffer size is incorrect; set to the default value (%d)", bufferSizeDefault);
                }
            }
            catch (NumberFormatException e) {
                bufferSize = bufferSizeDefault;
                LOG.warn("Buffer size is incorrect; set to the default value (%d)", bufferSizeDefault);
            }
        }
        // else: bufferSize is already set to bufferSizeDefault

        LOG.debug("Initializer successful");
    }

    public boolean isThreadSafe() {
        return true;
    }
}
