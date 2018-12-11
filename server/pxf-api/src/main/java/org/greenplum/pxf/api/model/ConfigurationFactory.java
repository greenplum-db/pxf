package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Map;

public interface ConfigurationFactory {

    String PXF_CONF_PROPERTY = "pxf.conf";
    File SERVERS_CONFIG_DIR = new File(
            System.getProperty(PXF_CONF_PROPERTY) + File.separator + "servers");
    String DEFAULT_SERVER_CONFIG_DIR = SERVERS_CONFIG_DIR + File.separator + "default";

    /**
     * Initializes a configuration object that applies server-specific configurations and
     * adds additional properties on top of it, if specified.
     *
     * @param serverName name of the server
     * @param additionalProperties additional properties to be added to the configuration
     * @return configuration object
     */
    Configuration initConfiguration(String serverName, Map<String, String> additionalProperties);
}
