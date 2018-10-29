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

/**
 * Caches Hadoop Configuration. We create a configuration per
 * server once and maintain it in an internal cache.
 * <p>
 * The cache is implemented as a Singleton.
 */
public class ConfigurationCache {

    private static final Log LOG = LogFactory.getLog(ConfigurationCache.class);

    private static final String CONFIG_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";
    private static final String CONFIG_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";

    private static ConfigurationCache INSTANCE;
    private final Map<String, Configuration> cache;

    /**
     * Prevent class instantiation
     */
    private ConfigurationCache() {
        cache = new HashMap<>();
    }

    /**
     * Returns the instance of the singleton ConfigurationCache
     */
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

    /**
     * Returns the configuration for the corresponding serverName.
     * If the configuration does not exist, it will create a new one
     * and cache it by serverName.
     *
     * @param serverName An alias name for the server
     * @return the configuration for the given serverName
     */
    public Configuration getConfiguration(String serverName) {
        boolean configCreated = false;
        Configuration config = cache.get(serverName);

        if (config == null) {
            synchronized (cache) {
                if ((config = cache.get(serverName)) == null) {
                    config = new Configuration();
                    // For multiple host support, we can add different
                    // resources given the name of the server (i.e.)
                    // config.addResource("server1/core-site.xml");
                    // We can create a configuration without loading
                    // defaults = new Configuration(loadDefaults=false);

                    cache.put(serverName, config);
                    configCreated = true;
                }
            }

            if (configCreated && StringUtils.equals("default", serverName)) {
                try {
                    secureConfigurationIfAvailable(config);
                } catch (Exception e) {
                    LOG.error("PXF Configuration Failed: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        return cache.get(serverName);
    }

    /**
     * Adds the principal and keytab to the configuration if Kerberos is enabled.
     *
     * @param config the configuration
     */
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
