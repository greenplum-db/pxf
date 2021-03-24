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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.PrivilegedAction;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BaseSecurityServiceTest {

    private static final PrivilegedAction<Boolean> EMPTY_ACTION = () -> true;

    private Configuration configuration;
    private RequestContext context;
    private SecurityService service;

    @Mock
    private SecureLogin mockSecureLogin;
    @Mock
    private UGIProvider mockUGIProvider;
    @Mock
    private UserGroupInformation mockLoginUGI;
    @Mock
    private UserGroupInformation mockProxyUGI;

    @BeforeEach
    public void setup() {
        context = new RequestContext();
        configuration = new Configuration();

        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider);

        context.setUser("gpdb-user");
        context.setTransactionId("xid");
        context.setSegmentId(7);
        context.setServerName("server");
        context.setConfig("config");
        context.setConfiguration(configuration);
    }

    /* ----------- methods that test determining remote user ----------- */

    @Test
    public void determineRemoteUser_IsLoginUser_NoKerberos_NoImpersonation_NoServiceUser() throws Exception {
        expectScenario("login-user", false, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("login-user", false, false);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_NoKerberos_NoImpersonation_ServiceUser() throws Exception {
        expectScenario("login-user", false, false, true);
        service.doAs(context, EMPTY_ACTION);
        // you would expect to find "service-user" here, and SecureLogin would set it as such
        // but our mocking logic is simple and always returns "login-user"
        // we are proving that we do not over-ride whatever SecureLogin returns in this case
        verifyScenario("login-user", false, false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_NoKerberos_Impersonation_NoServiceUser() throws Exception {
        expectScenario("gpdb-user", false, true, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user", false, true);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_NoKerberos_Impersonation_ServiceUser() throws Exception {
        expectScenario("gpdb-user", false, true, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user", false, true);
    }

    @Test
    public void determineRemoteUser_IsLoginUser_Kerberos_NoImpersonation_NoServiceUser() throws Exception {
        expectScenario("login-user", true, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("login-user", true, false);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_Kerberos_NoImpersonation_ServiceUser() throws Exception {
        expectScenario("service-user", true, false, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("service-user", true, false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_NoServiceUser() throws Exception {
        expectScenario("gpdb-user", true, true, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user", true, true);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_ServiceUser() throws Exception {
        expectScenario("gpdb-user", true, true, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user", true, true);
    }

    /* ----------- methods that test destroying UGI ----------- */

    @Test
    public void testDestroyUGI() throws Exception {
        expectScenario("login-user", false, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("login-user", false, false);
        verify(mockUGIProvider).destroy(any(UserGroupInformation.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void destroysUGIWhenTheActionExecutionThrowsRuntimeException() throws Exception {
        expectScenario("login-user", false, false, false);
        doThrow(RuntimeException.class).when(mockProxyUGI).doAs(any(PrivilegedAction.class));
        assertThrows(RuntimeException.class, () -> service.doAs(context, EMPTY_ACTION));
        verifyScenario("login-user", false, false);
        verify(mockUGIProvider).destroy(any(UserGroupInformation.class));
    }

    /* ----------- helper methods ----------- */

    private void expectScenario(String user, boolean kerberos, boolean impersonation, boolean serviceUser) throws Exception {
        if (!impersonation && kerberos) {
            configuration.set("hadoop.security.authentication", "kerberos");
        }
        when(mockSecureLogin.isUserImpersonationEnabled(configuration)).thenReturn(impersonation);
        when(mockLoginUGI.getUserName()).thenReturn("login-user");

        if (!impersonation && serviceUser && kerberos) {
            configuration.set("pxf.service.user.name", "service-user");
        }

        when(mockSecureLogin.getLoginUser("server", "config", configuration)).thenReturn(mockLoginUGI);

        if (impersonation) {
            when(mockUGIProvider.createProxyUser(user, mockLoginUGI)).thenReturn(mockProxyUGI);
        } else {
            when(mockUGIProvider.createRemoteUser(user, mockLoginUGI, kerberos)).thenReturn(mockProxyUGI);
        }
    }

    private void verifyScenario(String user, boolean kerberos, boolean impersonation) {
        if (impersonation) {
            verify(mockUGIProvider).createProxyUser(user, mockLoginUGI);
        } else {
            verify(mockUGIProvider).createRemoteUser(user, mockLoginUGI, kerberos);
        }
        verify(mockProxyUGI).doAs(ArgumentMatchers.<PrivilegedAction<Object>>any());
    }
}
