package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public interface ConfigurationFactory {

    /**
     * Initializes a configuration object that applies server-specific configurations
     */
    Configuration initConfiguration(String serverName, Map<String, String> options);
}
