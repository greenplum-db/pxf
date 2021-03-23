package org.greenplum.pxf.service.security;

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

import org.greenplum.pxf.api.model.RequestContext;

import java.security.PrivilegedAction;

public interface SecurityService {

    /**
     * Do as the user provide in the context
     *
     * @param context the request context
     * @param action  the action to perform
     * @param <T>     the return type of the action
     * @return the action result
     * @throws Exception when the action fails
     */
    <T> T doAs(RequestContext context, PrivilegedAction<T> action) throws Exception;
}
