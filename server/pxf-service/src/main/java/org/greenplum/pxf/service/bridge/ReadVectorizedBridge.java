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

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.ReadVectorizedResolver;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Deque;
import java.util.List;

@Component
@Scope("prototype")
public class ReadVectorizedBridge extends ReadBridge {

    public ReadVectorizedBridge(AccessorFactory accessorFactory, ResolverFactory resolverFactory) {
        super(accessorFactory, resolverFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Deque<Writable> makeOutput(OneRow oneRow) throws Exception {
        List<List<OneField>> resolvedBatch = ((ReadVectorizedResolver) resolver).
                getFieldsForBatch(oneRow);
        return outputBuilder.makeVectorizedOutput(resolvedBatch);
    }
}
