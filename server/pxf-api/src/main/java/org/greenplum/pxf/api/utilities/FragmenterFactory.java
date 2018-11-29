package org.greenplum.pxf.api.utilities;

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


import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;

/**
 * Factory class for creation of {@link Fragmenter} objects.
 */
public class FragmenterFactory extends BasePluginFactory<Fragmenter> {

    private static final FragmenterFactory instance = new FragmenterFactory();

    /**
     * Returns a singleton instance of the factory.
     * @return a singleton instance of the factory.
     */
    public static FragmenterFactory getInstance() {
        return instance;
    }

    @Override
    protected String getPluginClassName(RequestContext requestContext) {
        return requestContext.getFragmenter();
    }
}
