package org.greenplum.pxf.service;

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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.utilities.Utilities;

import java.io.IOException;

/**
 * Thin wrapper around {@link UserGroupInformation} create and destroy methods. We mock this class
 * in tests to be able to detect when a UGI is created/destroyed, and to isolate our tests from
 * creating/destroying real UGI instances.
 */
class UGIProvider {

    /**
     * Wrapper for {@link UserGroupInformation} creation
     *
     * @param effectiveUser the name of the user that we want to impersonate
     * @param loginUser the UGI of the login user (or Kerberos principal)
     * @return a {@link UserGroupInformation} for impersonation.
     * @throws IOException
     */
    UserGroupInformation createProxyUGI(String effectiveUser, UserGroupInformation loginUser) throws IOException {
        return UserGroupInformation.createProxyUser(effectiveUser, loginUser);
    }

    /**
     * Wrapper for {@link UserGroupInformation} creation of remote users
     *
     * @param user the name of the remote user
     * @param session session containing information on current configuration and login user
     * @return a remote {@link UserGroupInformation}.
     */
    UserGroupInformation createRemoteUser(String user, SessionId session) throws IOException {
        if (Utilities.isSecurityEnabled(session.getConfiguration())) {
            UserGroupInformation proxyUGI = createProxyUGI(user, session.getLoginUser());
            proxyUGI.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
            return proxyUGI;
        }
        return UserGroupInformation.createRemoteUser(user);
    }

    /**
     * Wrapper for {@link FileSystem}.closeAllForUGI method.
     *
     * @param ugi the {@link UserGroupInformation} whose filesystem resources we want to free.
     * @throws IOException
     */
    void destroy(UserGroupInformation ugi) throws IOException {
        FileSystem.closeAllForUGI(ugi);
    }
}
