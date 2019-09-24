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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.model.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


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
    private static final String PROPERTY_KEY_USER_IMPERSONATION = "pxf.service.user.impersonation.enabled";
    public static final String CONFIG_KEY_SERVICE_USER_NAME = "pxf.service.user.name";

    private static final Map<String, LoginSession> loginMap = new HashMap<>();

    private static final SecureLogin instance = new SecureLogin();

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
                        loginSession = new SecureLogin().login(serverName, configDirectory, configuration);
                    } else {
                        // Remote user specified in config file, or defaults to user running pxf service
                        String remoteUser = configuration.get(CONFIG_KEY_SERVICE_USER_NAME, UserGroupInformation.getLoginUser().getUserName());
                        loginSession = new LoginSession(configDirectory, remoteUser, null, null, UserGroupInformation.createRemoteUser(remoteUser));
                    }
                    loginMap.put(serverName, loginSession);
                }
            }
        }

        UserGroupInformation loginUser = loginSession.getUgi();
        if (Utilities.isSecurityEnabled(configuration)) {
            // TODO: we need to relogin from keytab ourselves because the method below relies on a static keytab file
            loginUser.reloginFromKeytab();
        }

        return loginUser;
    }

    /**
     * Establishes Login Context for the PXF service principal using Kerberos keytab.
     */
    public LoginSession login(String serverName, String configDirectory, Configuration configuration) {
        try {
            boolean isUserImpersonationEnabled = isUserImpersonationEnabled(configuration);

            LOG.info("User impersonation is {} for server {}", (isUserImpersonationEnabled ? "enabled" : "disabled"), serverName);

            UserGroupInformation.setConfiguration(configuration);

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

            SecurityUtil.login(configuration, CONFIG_KEY_SERVICE_KEYTAB, CONFIG_KEY_SERVICE_PRINCIPAL);

            return new LoginSession(configDirectory, principal, keytabFilename, null, UserGroupInformation.getLoginUser());
        } catch (Exception e) {
            LOG.error("PXF service login failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public boolean isUserImpersonationEnabled(Configuration configuration) {
        String valueFromUserImpersonationOnServer = configuration.get(SecureLogin.CONFIG_KEY_SERVICE_USER_IMPERSONATION, System.getProperty(PROPERTY_KEY_USER_IMPERSONATION, "true"));
        return StringUtils.equalsIgnoreCase(valueFromUserImpersonationOnServer, "true");
    }

    private LoginSession getServerLoginSession(final String serverName, final String configDirectory, Configuration configuration) {


        // if (server is not in map)
        //    login
        // else
        //     if config directory changed or principal name changed or keytab path file changed or file md5 changed
        //         clear old entry in map
        //         login

        LoginSession currentSession = loginMap.get(serverName);

        if (currentSession == null)
            return null;
        // Check for changes in:
        // - config directory
        // - principal name
        // - keytab path
        // - keytab md5

        final String principalName = SecureLogin.getServicePrincipal(serverName, configuration);
        final String keytabPath = SecureLogin.getServiceKeytab(serverName, configuration);
        String keytabMd5 = null;
        // TODO calculate MD5

//        try (InputStream is = Files.newInputStream(Paths.get(keytabPath))) {
//            keytabMd5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
//        } catch (IOException e) {
//            throw new IllegalArgumentException(String.format("Unable to read keytab at path %s", keytabPath), e);
//        }

        LoginSession kerberosLoginSession = new LoginSession(configDirectory, principalName, keytabPath, keytabMd5);

        if (!currentSession.equals(kerberosLoginSession)) {
            LOG.error("Kerberos principal : changes detected in the kerberos login session");
            try {
                FileSystem.closeAllForUGI(currentSession.getUgi());
            } catch (IOException e) {
                LOG.error(String.format("Error releasing UGI for server: %s", serverName), e);
            }
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

    /**
     * Stores information about Kerberos login details for a given configuration server.
     */
    private class LoginSession {

        private String configDirectory;
        private String principalName;
        private String keytabPath;
        private String keytabMd5;
        private UserGroupInformation ugi;

        /**
         * Creates a new session object.
         *
         * @param configDirectory server configuration directory
         * @param principalName   Kerberos principal name to use to obtain tokens
         * @param keytabPath      full path to a keytab file for the principal
         * @param keytabMd5       MD5 hash of the keytab file
         */
        public LoginSession(String configDirectory, String principalName, String keytabPath, String keytabMd5) {
            this(configDirectory, principalName, keytabPath, keytabMd5, null);
        }

        /**
         * Creates a new session object.
         *
         * @param configDirectory server configuration directory
         * @param principalName   Kerberos principal name to use to obtain tokens
         * @param keytabPath      full path to a keytab file for the principal
         * @param keytabMd5       MD5 hash of the keytab file
         * @param ugi             UserGroupInformation for the given principal after login to Kerberos was performed
         */
        public LoginSession(String configDirectory, String principalName, String keytabPath, String keytabMd5, UserGroupInformation ugi) {
            this.configDirectory = configDirectory;
            this.principalName = principalName;
            this.keytabPath = keytabPath;
            this.keytabMd5 = keytabMd5;
            this.ugi = ugi;
        }

        /**
         * Get the login UGI for this session
         *
         * @return the UGI for this session
         */
        public UserGroupInformation getUgi() {
            return ugi;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LoginSession that = (LoginSession) o;
            // ugi is not included into expression below as it is a transient derived value
            return Objects.equals(configDirectory, that.configDirectory) &&
                    Objects.equals(principalName, that.principalName) &&
                    Objects.equals(keytabPath, that.keytabPath) &&
                    Objects.equals(keytabMd5, that.keytabMd5);
        }

        @Override
        public int hashCode() {
            return Objects.hash(configDirectory, principalName, keytabPath, keytabMd5);
        }
    }
}
