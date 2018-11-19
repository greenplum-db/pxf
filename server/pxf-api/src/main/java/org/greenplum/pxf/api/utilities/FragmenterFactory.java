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


import org.greenplum.pxf.api.BaseFragmenter;
import org.greenplum.pxf.api.model.RequestContext;

/**
 * Factory class for creation of {@link BaseFragmenter} objects. The actual {@link BaseFragmenter} object is "hidden" behind
 * an {@link BaseFragmenter} abstract class which is returned by the FragmenterFactory.
 */
public class FragmenterFactory {
    static public BaseFragmenter create(RequestContext requestContext) throws Exception {
    	String fragmenterName = requestContext.getFragmenter();

        return (BaseFragmenter) Utilities.createAnyInstance(RequestContext.class, fragmenterName, requestContext);
    }
}
