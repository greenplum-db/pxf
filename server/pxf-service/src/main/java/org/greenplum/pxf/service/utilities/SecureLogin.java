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
import org.greenplum.pxf.api.model.BaseConfigurationFactory;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

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

    private static final String CONFIG_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";
    private static final String CONFIG_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";
    private static final String PXF_CONF_PROPERTY = "pxf.conf";
    private static final String DEFAULT_SERVER_CONFIG_DIR =
            System.getProperty(PXF_CONF_PROPERTY) + File.separator +
                    "servers" + File.separator +
                    "default";
    private final ConfigurationFactory configurationFactory;

    public SecureLogin() {
        this(BaseConfigurationFactory.getInstance());
    }

    SecureLogin(ConfigurationFactory configurationFactory) {
        this.configurationFactory = configurationFactory;
    }

    /**
     * Establishes Login Context for the PXF service principal using Kerberos keytab.
     */
    public void login() {
        try {
            boolean isUserImpersonationEnabled = Utilities.isUserImpersonationEnabled();
            LOG.info("User impersonation is " + (isUserImpersonationEnabled ? "enabled" : "disabled"));

            Configuration configuration = configurationFactory.initConfiguration("default", null);
            UserGroupInformation.setConfiguration(configuration);

            if (!UserGroupInformation.isSecurityEnabled()) {
                LOG.info("Kerberos Security is not enabled");
                return;
            }

            File serverDirectory = new File(DEFAULT_SERVER_CONFIG_DIR);
            if (!serverDirectory.exists() || !serverDirectory.isDirectory() || !serverDirectory.canRead()) {
                throw new RuntimeException(String.format(
                        "Directory %s does not exist, unable to create configuration for default server.",
                        DEFAULT_SERVER_CONFIG_DIR));
            }

            LOG.info("Kerberos Security is enabled");

            String principal = System.getProperty(CONFIG_KEY_SERVICE_PRINCIPAL);
            String keytabFilename = System.getProperty(CONFIG_KEY_SERVICE_KEYTAB);

            if (StringUtils.isEmpty(principal)) {
                throw new RuntimeException("Kerberos Security requires a valid principal.");
            }

            if (StringUtils.isEmpty(keytabFilename)) {
                throw new RuntimeException("Kerberos Security requires a valid keytab file name.");
            }

            configuration.set(CONFIG_KEY_SERVICE_PRINCIPAL, principal);
            configuration.set(CONFIG_KEY_SERVICE_KEYTAB, keytabFilename);

            LOG.debug("Kerberos principal: {}", configuration.get(CONFIG_KEY_SERVICE_PRINCIPAL));
            LOG.debug("Kerberos keytab: {}", configuration.get(CONFIG_KEY_SERVICE_KEYTAB));

            SecurityUtil.login(configuration, CONFIG_KEY_SERVICE_KEYTAB, CONFIG_KEY_SERVICE_PRINCIPAL);

        } catch (Exception e) {
            LOG.error("PXF service login failed: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
