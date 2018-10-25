package org.greenplum.pxf.plugins.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConfigurationCache {

    private static final Log LOG = LogFactory.getLog(ConfigurationCache.class);

    private static final String CONFIG_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";
    private static final String CONFIG_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";

    private static ConfigurationCache INSTANCE;
    private final Map<String, Configuration> cache;

    private ConfigurationCache() {
        cache = new HashMap<>();
    }

    public static ConfigurationCache getInstance() {
        if (INSTANCE == null) {
            synchronized (ConfigurationCache.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ConfigurationCache();
                }
            }
        }
        return INSTANCE;
    }

    public Configuration getConfiguration(String serverName) {

        if (!cache.containsKey(serverName)) {
            synchronized (cache) {
                if (!cache.containsKey(serverName)) {
                    Configuration config = new Configuration();

                    // For multiple host support, we can add different
                    // resources given the name of the server (i.e.)
                    // config.addResource("server1/core-site.xml");
                    // We can create a configuration without loading
                    // defaults = new Configuration(loadDefaults=false);

                    if (StringUtils.equals("default", serverName)) {
                        try {
                            secureConfigurationIfAvailable(config);
                        } catch (Exception e) {
                            LOG.error("PXF Configuration Failed: " + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    }

                    cache.put(serverName, config);
                }
            }
        }

        return cache.get(serverName);
    }

    private void secureConfigurationIfAvailable(Configuration config) throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            LOG.info("Kerberos Security is enabled");

            String principal = System.getProperty(CONFIG_KEY_SERVICE_PRINCIPAL);
            String keytabFilename = System.getProperty(CONFIG_KEY_SERVICE_KEYTAB);

            if (StringUtils.isEmpty(principal)) {
                throw new RuntimeException("Kerberos Security requires a valid principal.");
            }

            if (StringUtils.isEmpty(keytabFilename)) {
                throw new RuntimeException("Kerberos Security requires a valid keytab file name.");
            }

            config.set(CONFIG_KEY_SERVICE_PRINCIPAL, principal);
            config.set(CONFIG_KEY_SERVICE_KEYTAB, keytabFilename);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Kerberos principal: " + config.get(CONFIG_KEY_SERVICE_PRINCIPAL));
                LOG.debug("Kerberos keytab: " + config.get(CONFIG_KEY_SERVICE_KEYTAB));
            }

            SecurityUtil.login(config, CONFIG_KEY_SERVICE_KEYTAB, CONFIG_KEY_SERVICE_PRINCIPAL);
        } else {
            LOG.info("Kerberos Security is not enabled");
        }
    }
}
