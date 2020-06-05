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
import org.greenplum.pxf.service.SessionId;
import org.greenplum.pxf.service.UGICache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.PrivilegedExceptionAction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BaseSecurityServiceTest {

    private static final PrivilegedExceptionAction<Boolean> EMPTY_ACTION = () -> true;

    private SecurityService service;

    private RequestContext context;
    @Mock
    private SecureLogin mockSecureLogin;
    @Mock
    private UGICache mockUGICache;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private UserGroupInformation mockLoginUGI;
    @Mock
    private UserGroupInformation mockProxyUGI;

    private ArgumentCaptor<SessionId> session;

    @BeforeEach
    public void setup() {
        context = new RequestContext();
        session = ArgumentCaptor.forClass(SessionId.class);

        service = new BaseSecurityService(mockSecureLogin, mockUGICache);

        context.setUser("gpdb-user");
        context.setTransactionId("xid");
        context.setSegmentId(7);
        context.setServerName("server");
        context.setConfig("config");
        context.setConfiguration(mockConfiguration);
    }

    /* ----------- methods that test determining remote user ----------- */

    @Test
    public void determineRemoteUser_IsLoginUser_NoKerberos_NoImpersonation_NoServiceUser() throws Exception {
        expectScenario(false, false, false);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        verifyScenario("login-user", false);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_NoKerberos_NoImpersonation_ServiceUser() throws Exception {
        expectScenario(false, false, true);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        // you would expect to find "service-user" here, and SecureLogin would set it as such
        // but our mocking logic is simple and always returns "login-user"
        // we are proving that we do not over-ride whatever SecureLogin returns in this case
        verifyScenario("login-user", false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_NoKerberos_Impersonation_NoServiceUser() throws Exception {
        expectScenario(false, true, false);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        verifyScenario("gpdb-user", true);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_NoKerberos_Impersonation_ServiceUser() throws Exception {
        expectScenario(false, true, true);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        verifyScenario("gpdb-user", true);
    }

    @Test
    public void determineRemoteUser_IsLoginUser_Kerberos_NoImpersonation_NoServiceUser() throws Exception {
        expectScenario(true, false, false);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        verifyScenario("login-user", false);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_Kerberos_NoImpersonation_ServiceUser() throws Exception {
        expectScenario(true, false, true);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        verifyScenario("service-user", false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_NoServiceUser() throws Exception {
        expectScenario(true, true, false);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        verifyScenario("gpdb-user", true);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_ServiceUser() throws Exception {
        expectScenario(true, true, true);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        verifyScenario("gpdb-user", true);
    }

    /* ----------- methods that test cleaning UGI cache ----------- */

    @Test
    public void doesNotCleanTheUGICacheOnNonLastCalls() throws Exception {
        expectScenario(false, false, false);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        verifyScenario("login-user", false);
        verify(mockUGICache).release(any(SessionId.class), eq(false));
    }

    @Test
    public void tellsTheUGICacheToCleanItselfOnTheLastCallForASegment() throws Exception {
        context.setLastFragment(true);
        expectScenario(false, false, false);
        service.doAs(context, context.isLastFragment(), EMPTY_ACTION);
        verifyScenario("login-user", false);
        verify(mockUGICache).release(any(SessionId.class), eq(true));
    }

    /* ----------- helper methods ----------- */

    private void expectScenario(boolean kerberos, boolean impersonation, boolean serviceUser) throws Exception {
        if (!impersonation) {
            when(mockConfiguration.get("hadoop.security.authentication", "simple"))
                    .thenReturn(kerberos ? "kerberos" : "simple");
        }
        when(mockSecureLogin.isUserImpersonationEnabled(mockConfiguration)).thenReturn(impersonation);
        when(mockLoginUGI.getUserName()).thenReturn("login-user");

        if (!impersonation && serviceUser && kerberos) {
            when(mockConfiguration.get("pxf.service.user.name")).thenReturn("service-user");
        }

        when(mockSecureLogin.getLoginUser("server", "config", mockConfiguration)).thenReturn(mockLoginUGI);
        when(mockUGICache.getUserGroupInformation(any(SessionId.class), eq(impersonation))).thenReturn(mockProxyUGI);
    }

    private void verifyScenario(String user, boolean impersonation) throws Exception {
        verify(mockUGICache).getUserGroupInformation(session.capture(), eq(impersonation));
        verify(mockProxyUGI).doAs(ArgumentMatchers.<PrivilegedExceptionAction<Object>>any());
        assertEquals(user, session.getValue().getUser());
        assertEquals(7, session.getValue().getSegmentId().intValue());
        assertSame(mockConfiguration, session.getValue().getConfiguration());
        assertSame(mockLoginUGI, session.getValue().getLoginUser());
    }
}
