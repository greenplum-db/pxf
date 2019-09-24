package org.greenplum.pxf.service.servlet;

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
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.model.BaseConfigurationFactory;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.KerberosLoginSession;
import org.greenplum.pxf.service.SessionId;
import org.greenplum.pxf.service.UGICache;
import org.greenplum.pxf.service.utilities.SecuredHDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Reference;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/**
 * Listener on lifecycle events of our webapp
 */
public class SecurityServletFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityServletFilter.class);
    private static final String CONFIG_HEADER = "X-GP-OPTIONS-CONFIG";
    private static final String CONFIG_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";
    private static final String CONFIG_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";
    private static final String USER_HEADER = "X-GP-USER";
    private static final String SEGMENT_ID_HEADER = "X-GP-SEGMENT-ID";
    private static final String SERVER_HEADER = "X-GP-OPTIONS-SERVER";
    private static final String TRANSACTION_ID_HEADER = "X-GP-XID";
    private static final String LAST_FRAGMENT_HEADER = "X-GP-LAST-FRAGMENT";
    private static final String DELEGATION_TOKEN_HEADER = "X-GP-TOKEN";
    private static final String MISSING_HEADER_ERROR = "Header %s is missing in the request";
    private static final String EMPTY_HEADER_ERROR = "Header %s is empty in the request";
    private static final Object KERBEROS_LOGIN_LOCK = new Object();
    private static final Map<String, KerberosLoginSession> loginMap = new HashMap<>();
    UGICache ugiCache;
    private FilterConfig config;
    private final ConfigurationFactory configurationFactory;

    public SecurityServletFilter() {
        this(BaseConfigurationFactory.getInstance());
    }

    SecurityServletFilter(ConfigurationFactory configurationFactory) {
        this.configurationFactory = configurationFactory;
    }

    /**
     * Initializes the filter.
     *
     * @param filterConfig filter configuration
     */
    @Override
    public void init(FilterConfig filterConfig) {
        config = filterConfig;
        ugiCache = new UGICache();
    }

    /**
     * If user impersonation is configured, examines the request for the presense of the expected security headers
     * and create a proxy user to execute further request chain. Responds with an HTTP error if the header is missing
     * or the chain processing throws an exception.
     *
     * @param request  http request
     * @param response http response
     * @param chain    filter chain
     */
    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException {

        String impersonationHeaderValue = getHeaderValue(request, "X-GP-OPTIONS-IMPERSONATION", false);
        boolean isUserImpersonation = StringUtils.isNotBlank(impersonationHeaderValue) ?
                "true".equals(impersonationHeaderValue) :
                Utilities.isUserImpersonationEnabled();

        if (isUserImpersonation) {
            LOG.debug("User impersonation is enabled");
        }

        // retrieve user header and make sure header is present and is not empty
        final String gpdbUser = getHeaderValue(request, USER_HEADER, true);
        final String transactionId = getHeaderValue(request, TRANSACTION_ID_HEADER, true);
        final Integer segmentId = getHeaderValueInt(request, SEGMENT_ID_HEADER, true);
        final boolean lastCallForSegment = getHeaderValueBoolean(request, LAST_FRAGMENT_HEADER, false);

        final String serverName = StringUtils.defaultIfBlank(getHeaderValue(request, SERVER_HEADER, false), "default");
        final String configDirectory = StringUtils.defaultIfBlank(getHeaderValue(request, CONFIG_HEADER, false), serverName);

        // Secure login if kerberos is enabled
        Configuration configuration = configurationFactory.initConfiguration(configDirectory, serverName, null, null);
        UserGroupInformation loginUser = null;
        if (Utilities.isSecurityEnabled(configuration)) {
            KerberosLoginSession loginSession = getServerLoginSession(serverName, configDirectory, configuration);
            if (loginSession == null) {
                synchronized (this.getClass()) {
                    loginSession = getServerLoginSession(serverName, configDirectory, configuration);
                    if (loginSession == null) {
                        loginSession = login(serverName, configDirectory, isUserImpersonation, configuration);
                        loginMap.put(serverName, loginSession);
                    }
                }
            }
            loginUser = loginSession.getUgi();
        } else {
            loginUser = UserGroupInformation.getLoginUser();
        }

        SessionId session = new SessionId(
                segmentId,
                transactionId,
                (isUserImpersonation ? gpdbUser : loginUser.getUserName()),
                configuration,
                loginUser);

        // Prepare privileged action to run on behalf of proxy user
        PrivilegedExceptionAction<Boolean> action = () -> {
            LOG.debug("Performing request chain call for proxy user = {}", gpdbUser);
            chain.doFilter(request, response);
            return true;
        };

        // Refresh Kerberos token when security is enabled
        String tokenString = getHeaderValue(request, DELEGATION_TOKEN_HEADER, false);
        SecuredHDFS.verifyToken(tokenString, config.getServletContext());

        try {
            LOG.debug("Retrieving proxy user for session: {}", session);
            // Retrieve proxy user UGI from the UGI of the logged in user
            UserGroupInformation userGroupInformation = ugiCache
                    .getUserGroupInformation(session, isUserImpersonation);

            // Execute the servlet chain as that user
            userGroupInformation.doAs(action);
        } catch (UndeclaredThrowableException ute) {
            // unwrap the real exception thrown by the action
            throw new ServletException(ute.getCause());
        } catch (InterruptedException ie) {
            throw new ServletException(ie);
        } finally {
            // Optimization to cleanup the cache if it is the last fragment
            LOG.debug("Releasing proxy user for session: {}. {}",
                    session, lastCallForSegment ? " Last fragment call" : "");
            try {
                ugiCache.release(session, lastCallForSegment);
            } catch (Throwable t) {
                LOG.error("Error releasing UGICache for session: {}", session, t);
            }
            if (lastCallForSegment) {
                LOG.info("Finished processing {}", session);
            }
        }
    }

    private KerberosLoginSession getServerLoginSession(final String serverName, final String configDirectory, Configuration configuration) {


        // if (server is not in map)
        //    login
        // else
        //     if config directory changed or principal name changed or keytab path file changed or file md5 changed
        //         clear old entry in map
        //         login

        KerberosLoginSession currentSession = loginMap.get(serverName);

        if (currentSession == null)
            return null;
        // Check for changes in:
        // - config directory
        // - principal name
        // - keytab path
        // - keytab md5


        final String principalName = configuration.get(CONFIG_KEY_SERVICE_PRINCIPAL);
        final String keytabPath = configuration.get(CONFIG_KEY_SERVICE_KEYTAB);
        String keytabMd5 = null;

//        try (InputStream is = Files.newInputStream(Paths.get(keytabPath))) {
//            keytabMd5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
//        } catch (IOException e) {
//            throw new IllegalArgumentException(String.format("Unable to read keytab at path %s", keytabPath), e);
//        }

        KerberosLoginSession kerberosLoginSession = new KerberosLoginSession(configDirectory, principalName, keytabPath, keytabMd5);

        if (!currentSession.equals(kerberosLoginSession)) {
            return null;
        }

        return currentSession;
    }

    /**
     * Destroys the filter.
     */
    @Override
    public void destroy() {
    }

    /**
     * Establishes Login Context for the PXF service principal using Kerberos keytab.
     */
    private KerberosLoginSession login(String serverName, String configDirectory, boolean isUserImpersonationEnabled, Configuration configuration) {
        try {

            LOG.info("User impersonation is {} for server {}", (isUserImpersonationEnabled ? "enabled" : "disabled"), serverName);

            UserGroupInformation.setConfiguration(configuration);

            LOG.info("Kerberos Security is enabled");

            String defaultPrincipal = null, defaultKeytab = null;

            if (StringUtils.equals(serverName, "default")) {

                // use system property as default for backward compatibility when only 1 Kerberized cluster was supported

                defaultPrincipal = System.getProperty(CONFIG_KEY_SERVICE_PRINCIPAL);
                defaultKeytab = System.getProperty(CONFIG_KEY_SERVICE_KEYTAB);
            }

            String principal = configuration.get(CONFIG_KEY_SERVICE_PRINCIPAL, defaultPrincipal);
            String keytabFilename = configuration.get(CONFIG_KEY_SERVICE_KEYTAB, defaultKeytab);

            if (StringUtils.isEmpty(principal)) {
                throw new RuntimeException("Kerberos Security requires a valid principal.");
            }

            if (StringUtils.isEmpty(keytabFilename)) {
                throw new RuntimeException("Kerberos Security requires a valid keytab file name.");
            }

            configuration.set(CONFIG_KEY_SERVICE_PRINCIPAL, principal);
            configuration.set(CONFIG_KEY_SERVICE_KEYTAB, keytabFilename);

            LOG.info("Kerberos principal: {}", configuration.get(CONFIG_KEY_SERVICE_PRINCIPAL));
            LOG.info("Kerberos keytab: {}", configuration.get(CONFIG_KEY_SERVICE_KEYTAB));

            SecurityUtil.login(configuration, CONFIG_KEY_SERVICE_KEYTAB, CONFIG_KEY_SERVICE_PRINCIPAL);

            return new KerberosLoginSession(configDirectory, principal, keytabFilename, null, UserGroupInformation.getLoginUser());
        } catch (Exception e) {
            LOG.error("PXF service login failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private Integer getHeaderValueInt(ServletRequest request, String headerKey, boolean required)
            throws IllegalArgumentException {
        String value = getHeaderValue(request, headerKey, required);
        return value != null ? Integer.valueOf(value) : null;
    }

    private String getHeaderValue(ServletRequest request, String headerKey, boolean required)
            throws IllegalArgumentException {
        String value = ((HttpServletRequest) request).getHeader(headerKey);
        if (required && value == null) {
            throw new IllegalArgumentException(String.format(MISSING_HEADER_ERROR, headerKey));
        } else if (required && value.trim().isEmpty()) {
            throw new IllegalArgumentException(String.format(EMPTY_HEADER_ERROR, headerKey));
        }
        return value;
    }

    private boolean getHeaderValueBoolean(ServletRequest request, String headerKey, boolean required) {
        return StringUtils.equals("true", getHeaderValue(request, headerKey, required));
    }

}
