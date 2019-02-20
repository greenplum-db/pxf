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
    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        String hostParameter = requestContext.getOption("IGNITE_HOST");
        String hostsParameter = requestContext.getOption("IGNITE_HOSTS");
        if (hostsParameter != null) {
            hosts = hostsParameter;
        }
        else if (hostParameter != null) {
            hosts = hostParameter;
        }
        else {
            hosts = "127.0.0.1:10800";
        }

        // This parameter is not required. The default value is null
        igniteCache = requestContext.getOption("IGNITE_CACHE");

        // This parameter is not required. The default value is null
        user = requestContext.getOption("USER");
        if (user != null) {
            // This parameter is not required. The default value is null
            password = requestContext.getOption("PASSWORD");
        }

        // This parameter is not required. The default value is 0
        String bufferSizeParameter = requestContext.getOption("BUFFER_SIZE");
        if (bufferSizeParameter != null) {
            bufferSize = Integer.parseInt(bufferSizeParameter);
            if (bufferSize <= 0) {
                throw new NumberFormatException("BUFFER_SIZE must be a positive integer");
            }
        }

        // This parameter is not required. The default value is false
        quoteColumns = (context.getOption("QUOTE_COLUMNS") != null);

        // This parameter is not required. The default value is false
        lazy = (requestContext.getOption("IGNITE_LAZY") != null);

        // This parameter is not required. The default value is false
        tcpNodelay = (requestContext.getOption("IGNITE_TCP_NODELAY") != null);

        // This parameter is not required. The default value is false
        replicatedOnly = (requestContext.getOption("IGNITE_REPLICATED_ONLY") != null);
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    // Connection parameters
    protected String hosts = null;
    protected String igniteCache = null;
    protected String user = null;
    protected String password = null;

    // ReceiveBufferSize or SendBufferSize (depends on type of query)
    protected int bufferSize = 0;

    // Whether to quote column names
    protected boolean quoteColumns = false;
    protected static final String QUOTE = "\"";

    // Ignite perfomance options
    protected boolean lazy = false;
    protected boolean tcpNodelay = false;
    protected boolean replicatedOnly = false;
}
