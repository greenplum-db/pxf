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

package org.greenplum.pxf.service.bridge;

import org.apache.commons.collections.map.LRUMap;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StatsAccessor;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;

import java.util.LinkedList;

/**
 * Bridge class optimized for aggregate queries.
 *
 */
public class AggBridge extends ReadBridge implements Bridge {

    /* Avoid resolving rows with the same key twice */
    private LRUMap outputCache;

    public AggBridge(RequestContext context) {
        this(context, AccessorFactory.getInstance(), ResolverFactory.getInstance());
    }

    AggBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory) {
        super(context, accessorFactory, resolverFactory);
    }

    @Override
    public boolean beginIteration() throws Exception {
        /* Initialize LRU cache with 100 items*/
        outputCache = new LRUMap();
        boolean openForReadStatus = accessor.openForRead();
        ((StatsAccessor) accessor).retrieveStats();
        return openForReadStatus;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Writable getNext() throws Exception {
        Writable output = null;
        LinkedList<Writable> cachedOutput = null;
        OneRow onerow = null;

        if (!outputQueue.isEmpty()) {
            return outputQueue.pop();
        }

        try {
            while (outputQueue.isEmpty()) {
                onerow = ((StatsAccessor) accessor).emitAggObject();
                if (onerow == null) {
                    break;
                }
                cachedOutput = (LinkedList<Writable>) outputCache.get(onerow.getKey());
                if (cachedOutput == null) {
                    cachedOutput = outputBuilder.makeOutput(resolver.getFields(onerow));
                    outputCache.put(onerow.getKey(), cachedOutput);
                }
                outputQueue.addAll(cachedOutput);
                if (!outputQueue.isEmpty()) {
                    output = outputQueue.pop();
                    break;
                }
            }
        } catch (Exception ex) {
            LOG.error("Error occurred when reading next object from aggregate bridge: ", ex.getMessage());
            throw ex;
        }
        return output;
    }
}
