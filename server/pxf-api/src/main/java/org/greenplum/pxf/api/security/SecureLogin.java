package org.greenplum.pxf.api.security;

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
import org.apache.hadoop.security.LoginSession;
import org.apache.hadoop.security.PxfUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * This class relies heavily on Hadoop API to
 * <ul>
 * <li>Check need for secure login in Hadoop</li>
 * <li>Parse and load .xml configuration file</li>
 * <li>Do a Kerberos login with a kaytab file</li>
 * <li>convert _HOST in Kerberos principal to current hostname</li>
 * </ul>
 * <p>
 * It uses Hadoop Configuration to parse XML configuration files.<br>
 * It uses Hadoop Security to modify principal and perform the login.
 * <p>
 * The major limitation in this class is its dependency on Hadoop. If Hadoop
 * security is off, no login will be performed regardless of connector being
 * used.
 */
public class SecureLogin {
    private static final Logger LOG = LoggerFactory.getLogger(SecureLogin.class);

    public static final String CONFIG_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";
    public static final String CONFIG_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";
    public static final String CONFIG_KEY_SERVICE_USER_IMPERSONATION = "pxf.service.user.impersonation";
    public static final String CONFIG_KEY_SERVICE_USER_NAME = "pxf.service.user.name";
    private static final String PROPERTY_KEY_USER_IMPERSONATION = "pxf.service.user.impersonation.enabled";

    private static final Map<String, LoginSession> loginMap = new HashMap<>();

    private static final SecureLogin instance = new SecureLogin();

    /**
     * Prevent instantiation of this class by e
     */
    private SecureLogin() {
    }

    /**
     * Returns the static instance for this factory
     *
     * @return the static instance for this factory
     */
    public static SecureLogin getInstance() {
        return instance;
    }

    public UserGroupInformation getLoginUser(RequestContext context, Configuration configuration) throws IOException {
        return getLoginUser(context.getServerName(), context.getConfig(), configuration);
    }

    public UserGroupInformation getLoginUser(String serverName, String configDirectory, Configuration configuration) throws IOException {
        // Kerberos security is enabled for the server, use identity of the Kerberos principal for the server
        LoginSession loginSession = getServerLoginSession(serverName, configDirectory, configuration);
        if (loginSession == null) {
            synchronized (SecureLogin.class) {
                loginSession = getServerLoginSession(serverName, configDirectory, configuration);
                if (loginSession == null) {

                    if (Utilities.isSecurityEnabled(configuration)) {
                        LOG.info("Kerberos Security is enabled for server {}", serverName);
                        loginSession = login(serverName, configDirectory, configuration);
                    } else {
                        // Remote user specified in config file, or defaults to user running pxf service
                        String remoteUser = configuration.get(CONFIG_KEY_SERVICE_USER_NAME, System.getProperty("user.name"));

                        loginSession = new LoginSession(configDirectory, null, null, UserGroupInformation.createRemoteUser(remoteUser), null, 0L);
                    }
                    loginMap.put(serverName, loginSession);
                }
            }
        }

        if (Utilities.isSecurityEnabled(configuration)) {
            PxfUserGroupInformation.reloginFromKeytab(loginSession);
        }

        return loginSession.getLoginUser();
    }

    /**
     * Establishes login context (LoginSession) for the PXF service principal using Kerberos keytab.
     *
     * @param serverName      name of the configuration server
     * @param configDirectory path to the configuration directory
     * @param configuration   request configuration
     * @return login session for the server
     */
    private LoginSession login(String serverName, String configDirectory, Configuration configuration) {
        try {
            boolean isUserImpersonationEnabled = isUserImpersonationEnabled(configuration);

            LOG.info("User impersonation is {} for server {}", (isUserImpersonationEnabled ? "enabled" : "disabled"), serverName);

//            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.reset();
            Configuration config = new Configuration();
            config.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(config);

            String principal = getServicePrincipal(serverName, configuration);
            String keytabFilename = getServiceKeytab(serverName, configuration);

            if (StringUtils.isEmpty(principal)) {
                throw new RuntimeException(String.format("Kerberos Security for server %s requires a valid principal.", serverName));
            }

            if (StringUtils.isEmpty(keytabFilename)) {
                throw new RuntimeException(String.format("Kerberos Security for server %s requires a valid keytab file name.", serverName));
            }

            configuration.set(CONFIG_KEY_SERVICE_PRINCIPAL, principal);
            configuration.set(CONFIG_KEY_SERVICE_KEYTAB, keytabFilename);

            LOG.info("Kerberos principal for server {}: {}", serverName, principal);
            LOG.info("Kerberos keytab for server {}: {}", serverName, keytabFilename);

            LoginSession loginSession = PxfUserGroupInformation
                    .loginUserFromKeytab(configuration, serverName, configDirectory, principal, keytabFilename);

            LOG.info("Logged in as principal {} for server {}", loginSession.getLoginUser(), serverName);

            return loginSession;
        } catch (Exception e) {
            throw new RuntimeException(String.format("PXF service login failed for server %s", serverName), e);
        }
    }

    /**
     * Returns whether user impersonation has been configured as enabled.
     *
     * @return true if user impersonation is enabled, false otherwise
     */
    public boolean isUserImpersonationEnabled(Configuration configuration) {
        String valueFromUserImpersonationOnServer = configuration.get(SecureLogin.CONFIG_KEY_SERVICE_USER_IMPERSONATION, System.getProperty(PROPERTY_KEY_USER_IMPERSONATION, "false"));
        return StringUtils.equalsIgnoreCase(valueFromUserImpersonationOnServer, "true");
    }

    /**
     * Returns an existing login session for the server if it has already been established before and configuration has not changed.
     *
     * @param serverName      name of the configuration server
     * @param configDirectory path to the configuration directory
     * @param configuration   configuration for the request
     * @return login session or null if it does not exist or does not match current configuration
     */
    private LoginSession getServerLoginSession(final String serverName, final String configDirectory, Configuration configuration) {

        LoginSession currentSession = loginMap.get(serverName);
        if (currentSession == null)
            return null;

        LoginSession expectedLoginSession = new LoginSession(configDirectory, SecureLogin.getServicePrincipal(serverName, configuration), SecureLogin.getServiceKeytab(serverName, configuration));
        if (!currentSession.equals(expectedLoginSession)) {
            LOG.warn("LoginSession has changed for server {} : existing {} expected {}", serverName, currentSession, expectedLoginSession);
            return null;
        }

        return currentSession;
    }

    /**
     * Returns the service principal name from the configuration if available,
     * or defaults to the system property for the default server for backwards
     * compatibility.
     *
     * @param serverName    the name of the server
     * @param configuration the hadoop configuration
     * @return the service principal for the given server and configuration
     */
    public static String getServicePrincipal(String serverName, Configuration configuration) {
        // use system property as default for backward compatibility when only 1 Kerberized cluster was supported
        String defaultPrincipal = StringUtils.equalsIgnoreCase(serverName, "default") ?
                System.getProperty(CONFIG_KEY_SERVICE_PRINCIPAL) :
                null;
        return configuration.get(CONFIG_KEY_SERVICE_PRINCIPAL, defaultPrincipal);
    }

    /**
     * Returns the service keytab path from the configuration if available,
     * or defaults to the system property for the default server for backwards
     * compatibility.
     *
     * @param serverName    the name of the server
     * @param configuration the hadoop configuration
     * @return the path of the service keytab for the given server and configuration
     */
    public static String getServiceKeytab(String serverName, Configuration configuration) {
        // use system property as default for backward compatibility when only 1 Kerberized cluster was supported
        String defaultKeytab = StringUtils.equalsIgnoreCase(serverName, "default") ?
                System.getProperty(CONFIG_KEY_SERVICE_KEYTAB) :
                null;
        return configuration.get(CONFIG_KEY_SERVICE_KEYTAB, defaultKeytab);
    }
}
