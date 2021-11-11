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
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.security.PrivilegedAction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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

        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider, true);

        context.setUser("gpdb-user");
        context.setTransactionId("xid");
        context.setSegmentId(7);
        context.setServerName("server");
        context.setConfig("config");
        context.setConfiguration(configuration);
    }

    /* ----------- methods that test determining remote user ----------- */

    /* --- NO KERBEROS / NO IMPERSONATION -- */

    @Test
    public void determineRemoteUser_IsLoginUser_NoKerberos_NoImpersonation_NoServiceUser() throws Exception {
        expectScenario("login-user", false, false, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("login-user", false, false, false);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_NoKerberos_NoImpersonation_ServiceUser() throws Exception {
        expectScenario("login-user", false, false, true, false);
        service.doAs(context, EMPTY_ACTION);
        // you would expect to find "service-user" here, and SecureLogin would set it as such
        // but our mocking logic is simple and always returns "login-user"
        // we are proving that we do not over-ride whatever SecureLogin returns in this case
        verifyScenario("login-user", false, false, false);
    }

    @Test
    public void determineRemoteUser_IsLoginUser_NoKerberos_NoImpersonation_ConstrainedDelegation() throws Exception {
        // setting constrained delegation in non-secure environment will cause in a validation error
        expectErrorScenario(false, false,
                "Kerberos constrained delegation should not be enabled for non-secure clusters.",
                "Set the value of pxf.service.kerberos.constrained-delegation property to false in foo-dir/pxf-site.xml file.");
    }

    /* --- NO KERBEROS / IMPERSONATION -- */

    @Test
    public void determineRemoteUser_IsGpdbUser_NoKerberos_Impersonation_NoServiceUser() throws Exception {
        expectScenario("gpdb-user", false, true, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user", false, true, false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_NoKerberos_Impersonation_ServiceUser() throws Exception {
        expectScenario("gpdb-user", false, true, true, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user", false, true, false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_NoKerberos_Impersonation_ServiceUser_NoExpansion() throws Exception {
        // no kerberos should cause no expansion anyways
        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider, false);
        expectScenario("gpdb-user", false, true, true, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user", false, true, false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_NoKerberos_Impersonation_ConstrainedDelegation() throws Exception {
        // setting constrained delegation in non-secure environment will cause in a validation error
        expectErrorScenario(false, true,
                "Kerberos constrained delegation should not be enabled for non-secure clusters.",
                "Set the value of pxf.service.kerberos.constrained-delegation property to false in foo-dir/pxf-site.xml file.");
    }

    /* --- KERBEROS / NO IMPERSONATION -- */

    @Test
    public void determineRemoteUser_IsLoginUser_Kerberos_NoImpersonation_NoServiceUser() throws Exception {
        expectScenario("login-user@REALM", true, false, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("login-user@REALM", true, false, false);
    }

    @Test
    public void determineRemoteUser_IsLoginUser_Kerberos_NoImpersonation_NoServiceUser_ConstrainedDelegation() throws Exception {
        // this is a useless case as constrained delegation is enabled for no reason, but it is a possible config combo
        expectScenario("login-user@REALM", true, false, false, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("login-user@REALM", true, false, true, false);
    }

    @Test
    public void determineRemoteUser_IsLoginUser_Kerberos_NoImpersonation_NoServiceUser_NoExpansion() throws Exception {
        // no impersonation should not attempt expansion and just take the login name which is already expanded
        // since this is kerberos use case and the login user (unlike gpdb user) should always have realm part
        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider, false);
        expectScenario("login-user@REALM", true, false, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("login-user@REALM", true, false, false);
    }

    @Test
    public void determineRemoteUser_IsLoginUser_Kerberos_NoImpersonation_NoServiceUser_NoExpansion_ConstrainedDelegation() throws Exception {
        // this is a useless case as constrained delegation is enabled for no reason, but it is a possible config combo
        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider, false);
        expectScenario("login-user@REALM", true, false, false, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("login-user@REALM", true, false, true, false);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_Kerberos_NoImpersonation_ServiceUser() throws Exception {
        expectScenario("service-user@REALM", true, false, true, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("service-user@REALM", true, false, false);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_Kerberos_NoImpersonation_ServiceUser_ConstrainedDelegation() throws Exception {
        expectScenario("service-user@REALM", true, false, true, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("service-user@REALM", true, false, true);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_Kerberos_NoImpersonation_ServiceUser_NoExpansion() throws Exception {
        // no impersonation should not attempt expansion and just take the service name which will is not expanded
        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider, false);
        expectScenario("service-user", true, false, true, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("service-user", true, false, false);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_Kerberos_NoImpersonation_ServiceUser_NoExpansion_ConstrainedDelegation() throws Exception {
        // constrained delegation will overrule and perform expansion
        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider, false);
        expectScenario("service-user@REALM", true, false, true, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("service-user@REALM", true, false, true);
    }

    @Test
    public void determineRemoteUser_IsServiceUser_Kerberos_NoImpersonation_ServiceUser_ConstrainedDelegation_RealmMismatch() throws Exception {
        // having remote user with @ in the name but not ending in realm will cause an error
        expectErrorScenario(true, false,
                "Remote principal name serice@user contains @ symbol but does not end with REALM",
                "Check the value of pxf.service.user.name property in foo-dir/pxf-site.xml file.",
                "serice@user");
    }


    /* --- KERBEROS / IMPERSONATION -- */

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_NoServiceUser() throws Exception {
        expectScenario("gpdb-user@REALM", true, true, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user@REALM", true, true, false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_NoServiceUser_NoExpansion() throws Exception {
        // no expansion will still use short name
        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider, false);
        expectScenario("gpdb-user", true, true, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user", true, true, false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_ServiceUser() throws Exception {
        expectScenario("gpdb-user@REALM", true, true, true, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user@REALM", true, true, false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_ServiceUser_NoExpansion() throws Exception {
        // no expansion will still use short name
        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider, false);
        expectScenario("gpdb-user", true, true, true, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user", true, true, false);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_ConstrainedDelegation() throws Exception {
        expectScenario("gpdb-user@REALM", true, true, false, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user@REALM", true, true, true);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_ServiceUser_ConstrainedDelegation() throws Exception {
        // service user is irrelevant for kerberos with impersonation
        expectScenario("gpdb-user@REALM", true, true, true, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user@REALM", true, true, true);
    }

    @Test
    public void determineRemoteUser_IsGpdbUser_Kerberos_Impersonation_ServiceUser_NoExpansion_ConstrainedDelegation() throws Exception {
        service = new BaseSecurityService(mockSecureLogin, mockUGIProvider, false);
        // service user is irrelevant for kerberos with impersonation
        expectScenario("gpdb-user@REALM", true, true, true, true);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("gpdb-user@REALM", true, true, true);
    }

    /* ----------- methods that test destroying UGI ----------- */

    @Test
    public void testDestroyUGI() throws Exception {
        expectScenario("login-user", false, false, false, false);
        service.doAs(context, EMPTY_ACTION);
        verifyScenario("login-user", false, false, false);
        verify(mockUGIProvider).destroy(any(UserGroupInformation.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void destroysUGIWhenTheActionExecutionThrowsRuntimeException() throws Exception {
        expectScenario("login-user", false, false, false, false);
        doThrow(RuntimeException.class).when(mockProxyUGI).doAs(any(PrivilegedAction.class));
        assertThrows(RuntimeException.class, () -> service.doAs(context, EMPTY_ACTION));
        verifyScenario("login-user", false, false, false);
        verify(mockUGIProvider).destroy(any(UserGroupInformation.class));
    }

    /* ----------- helper methods ----------- */

    private void expectScenario(String remoteUser, boolean kerberos, boolean impersonation, boolean serviceUser, boolean constrainedDelegation) throws Exception {
        if (kerberos) {
            configuration.set("hadoop.security.authentication", "kerberos");
        }
        if (constrainedDelegation) {
            configuration.set("pxf.service.kerberos.constrained-delegation", "true");
        }

        when(mockSecureLogin.isUserImpersonationEnabled(configuration)).thenReturn(impersonation);
        when(mockSecureLogin.isConstrainedDelegationEnabled(configuration)).thenReturn(constrainedDelegation);

        if (kerberos) {
            when(mockLoginUGI.getUserName()).thenReturn("login-user@REALM");
        } else {
            when(mockLoginUGI.getUserName()).thenReturn("login-user");
        }

        if (serviceUser) {
            configuration.set("pxf.service.user.name", "service-user");
        }

        when(mockSecureLogin.getLoginUser("server", "config", configuration)).thenReturn(mockLoginUGI);

        if (impersonation || constrainedDelegation) {
            when(mockUGIProvider.createProxyUser(remoteUser, mockLoginUGI)).thenReturn(mockProxyUGI);
        } else {
            when(mockUGIProvider.createRemoteUser(remoteUser, mockLoginUGI, kerberos)).thenReturn(mockProxyUGI);
        }
    }

    private void expectErrorScenario(boolean kerberos, boolean impersonation, String errorMessage, String errorHint) throws IOException {
        expectErrorScenario(kerberos, impersonation, errorMessage, errorHint, null);
    }

    private void expectErrorScenario(boolean kerberos, boolean impersonation, String errorMessage, String errorHint, String serviceUser) throws IOException {
        if (kerberos) {
            configuration.set("hadoop.security.authentication", "kerberos");
        }
        configuration.set("pxf.service.kerberos.constrained-delegation","true");
        configuration.set("pxf.config.server.directory","foo-dir"); // for checking error hint message

        when(mockSecureLogin.isUserImpersonationEnabled(configuration)).thenReturn(impersonation);
        when(mockSecureLogin.isConstrainedDelegationEnabled(configuration)).thenReturn(true);
        when(mockLoginUGI.getUserName()).thenReturn("login-user@REALM");
        when(mockSecureLogin.getLoginUser("server", "config", configuration)).thenReturn(mockLoginUGI);
        if (serviceUser != null) {
            configuration.set("pxf.service.user.name", serviceUser);
        }

        PxfRuntimeException e = assertThrows(PxfRuntimeException.class, () -> service.doAs(context, EMPTY_ACTION));
        assertEquals(errorMessage, e.getMessage());
        assertEquals(errorHint, e.getHint());

        // verify that no actual work has been done
        verifyNoMoreInteractions(mockUGIProvider, mockLoginUGI);
    }

    private void verifyScenario(String user, boolean kerberos, boolean impersonation, boolean constrainedDelegation) {
        verifyScenario(user, kerberos, impersonation, constrainedDelegation, true);
    }

    private void verifyScenario(String user, boolean kerberos, boolean impersonation,
                                boolean constrainedDelegation, boolean expectPropertiesResolver) {
        if (impersonation || constrainedDelegation) {
            verify(mockUGIProvider).createProxyUser(user, mockLoginUGI);
        } else {
            verify(mockUGIProvider).createRemoteUser(user, mockLoginUGI, kerberos);
        }
        verify(mockProxyUGI).doAs(ArgumentMatchers.<PrivilegedAction<Object>>any());

        String saslProviderName = configuration.get("hadoop.security.saslproperties.resolver.class");
        if (constrainedDelegation && expectPropertiesResolver) {
            assertEquals(PxfSaslPropertiesResolver.class.getName(), saslProviderName);
        } else {
            assertNull(saslProviderName);
        }
    }
}
