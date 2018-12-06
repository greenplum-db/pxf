package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Map;

public interface ConfigurationFactory {

    String PXF_CONF_PROPERTY = "pxf.conf";
    File SERVERS_CONFIG_DIR = new File(
            System.getProperty(PXF_CONF_PROPERTY) + File.separator + "servers");
    String DEFAULT_SERVER_CONFIG_DIR = SERVERS_CONFIG_DIR + "default";

    /**
     * Initializes a configuration object that applies server-specific configurations
     */
    Configuration initConfiguration(String serverName, Map<String, String> options);
}
