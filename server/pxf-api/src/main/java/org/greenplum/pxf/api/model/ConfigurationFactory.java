package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Map;

public interface ConfigurationFactory {

    String PXF_CONFIG_RESOURCE_PATH_PROPERTY = "pxf.config.resource.path";

    /**
     * Configuration property that stores the server directory
     */
    String PXF_CONFIG_SERVER_DIRECTORY_PROPERTY = "pxf.config.server.directory";

    /**
     * Synthetic configuration property that stores the user so that is can be
     * used in config files for interpolation in other properties, for example
     * in JDBC when setting session authorization from a proxy user to the
     * end-user
     */
    String PXF_SESSION_USER_PROPERTY = "pxf.session.user";

    /**
     * Initializes a configuration object that applies server-specific configurations and
     * adds additional properties on top of it, if specified.
     *
     * @param configDirectory name of the configuration directory
     * @param serverName name of the server
     * @param userName name of the user
     * @param additionalProperties additional properties to be added to the configuration
     * @return configuration object
     */
    Configuration initConfiguration(String configDirectory, String serverName, String userName, Map<String, String> additionalProperties);
}
