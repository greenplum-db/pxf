package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Map;

public interface ConfigurationFactory {

    String PXF_CONF_PROPERTY = "pxf.conf";
    String SERVER_CONFIG_DIR_PREFIX =
            System.getProperty(PXF_CONF_PROPERTY) + File.separator + "servers" + File.separator;
    String DEFAULT_SERVER_CONFIG_DIR = SERVER_CONFIG_DIR_PREFIX + "default";

    /**
     * Initializes a configuration object that applies server-specific configurations
     */
    Configuration initConfiguration(String serverName, Map<String, String> options);
}
