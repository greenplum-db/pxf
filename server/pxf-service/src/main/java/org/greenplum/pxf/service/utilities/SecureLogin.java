package org.greenplum.pxf.service.utilities;

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
import org.greenplum.pxf.service.KerberosLoginSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /**
     * Establishes Login Context for the PXF service principal using Kerberos keytab.
     */
    public KerberosLoginSession login(String serverName, String configDirectory, boolean isUserImpersonationEnabled, Configuration configuration) {
        try {
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

            return new KerberosLoginSession(configDirectory, principal, keytabFilename, null, UserGroupInformation.getLoginUser());
        } catch (Exception e) {
            LOG.error("PXF service login failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
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
