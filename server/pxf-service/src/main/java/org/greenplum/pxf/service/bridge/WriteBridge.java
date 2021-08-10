package org.greenplum.pxf.service.bridge;

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


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.BadRecordException;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.BridgeInputBuilder;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

/*
 * WriteBridge class creates appropriate accessor and resolver.
 * It reads data from inputStream by the resolver,
 * and writes it to the Hadoop storage with the accessor.
 */
public class WriteBridge extends BaseBridge {

    private static final int MAX_BACKOFF = 1000;
    private final BridgeInputBuilder inputBuilder;

    /*
     * C'tor - set the implementation of the bridge
     */
    public WriteBridge(RequestContext context) {
        super(context);
        inputBuilder = new BridgeInputBuilder(context);
    }

    WriteBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory) {
        super(context, accessorFactory, resolverFactory);
        inputBuilder = new BridgeInputBuilder(context);
    }

    @Override
    public boolean beginIteration() throws Exception {
        Configuration configuration = accessor.getConfiguration();
        // max attempts to fetch fragments is 1 unless we have Kerberos-secured cluster that needs retries to
        // overcome potential SASL auth failures
        boolean securityEnabled = Utilities.isSecurityEnabled(configuration);
        int maxAttempts = securityEnabled ?
                configuration.getInt("pxf.sasl.connection.retries", 5) + 1 : 1;

        LOG.debug("Before beginning iteration, security = {}, max attempts = {}", securityEnabled, maxAttempts);

        // retry up to max allowed number
        boolean result = false;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                LOG.debug("Beginning iteration attempt #{} of {}", attempt, maxAttempts);
                result = accessor.openForWrite();
                break;
            } catch (IOException e) {
                if (StringUtils.contains(e.getMessage(), "GSS initiate failed")) {
                    LOG.warn(String.format("Attempt #%d failed to begin iteration: ", attempt), e.getMessage());
                    if (attempt < maxAttempts - 1) {
                        int backoffMs = new Random().nextInt(MAX_BACKOFF) + 1;
                        LOG.debug("Backing off for {} ms before the next attempt", backoffMs);
                        Thread.sleep(backoffMs);
                        // get a new instance of the accessor as openForWrite() might not be idempotent
                        accessor = accessorFactory.getPlugin(context);
                    } else {
                        // reached the max allowed number of retries, re-throw the exception to the caller
                        throw e;
                    }
                } else {
                    // non-GSS initiate failed IOException needs to be rethrown.
                    throw e;
                }
            }
        }
        return result;
    }

    /*
     * Read data from stream, convert it using Resolver into OneRow object, and
     * pass to WriteAccessor to write into file.
     */
    @Override
    public boolean setNext(DataInputStream inputStream) throws Exception {

        List<OneField> record = inputBuilder.makeInput(inputStream);
        if (record == null) {
            return false;
        }

        OneRow onerow = resolver.setFields(record);
        if (onerow == null) {
            return false;
        }
        if (!accessor.writeNextObject(onerow)) {
            throw new BadRecordException();
        }
        return true;
    }

    /*
     * Close the underlying resource
     */
    public void endIteration() throws Exception {
        try {
            accessor.closeForWrite();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public Writable getNext() {
        throw new UnsupportedOperationException("getNext is not implemented");
    }

}
