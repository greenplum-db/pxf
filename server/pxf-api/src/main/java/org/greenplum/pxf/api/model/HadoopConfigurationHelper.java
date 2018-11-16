package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;


//TODO move to utilities
public class HadoopConfigurationHelper {

    private static final String DEFAULT_SERVER_NAME = "default";
    private static final String PXF_CONF_PROPERTY = "pxf.conf";
    private static final String SERVER_CONFIG_DIR_PREFIX =
            System.getProperty(PXF_CONF_PROPERTY) +
                    File.separator +
                    "servers" +
                    File.separator;

    private Logger LOG = LoggerFactory.getLogger(HadoopConfigurationHelper.class);

    /**
     * Returns a configuration object that applies server-specific configurations
     */
    public Configuration getConfiguration(InputData inputData) {

        // start with built-in Hadoop configuration that loads core-site.xml
        Configuration configuration = new Configuration();

        // add all other *-site.xml files from default cluster will be added as "default" resources
        // by JobConf class or other Hadoop libraries on as-needed basis

        String serverName = inputData.getServerName();
        if (!DEFAULT_SERVER_NAME.equals(serverName)) {
            // determine full path for server configuration directory
            String directoryName = SERVER_CONFIG_DIR_PREFIX + serverName;
            File serverDirectory = new File(directoryName);
            if (!serverDirectory.exists()) {
                LOG.debug("Directory %s does not exist", serverDirectory);
                return configuration;
            }
            LOG.debug("Using directory %s for server %s configuration", serverDirectory, serverName);
            addSiteFilesAsResources(configuration, serverDirectory, serverName);
        }

        inputData.getUserPropertiesStream()
                .forEach(entry -> configuration.set(entry.getKey()
                        .substring(InputData.USER_PROP_PREFIX.length()), entry.getValue()));

        return configuration;
    }

    private void addSiteFilesAsResources(Configuration configuration, File directory, String serverName) {
        // add all *-site.xml files inside the server config directory as configuration resources
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory.toPath(), "*-site.xml")) {
            for (Path path : stream) {
                LOG.debug("adding configuration resource for server %s from %s", serverName, path);
                configuration.addResource(path.toUri().toURL());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to read configuration for server %s from %s",
                    serverName, directory.getAbsolutePath()), e);
        }
    }
}
