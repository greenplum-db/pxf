package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class BaseConfigurationFactory implements ConfigurationFactory {

    private static final BaseConfigurationFactory instance = new BaseConfigurationFactory();
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    /**
     * Returns the static instance for this factory
     *
     * @return the static instance for this factory
     */
    public static BaseConfigurationFactory getInstance() {
        return instance;
    }

    /**
     * Initializes a configuration object that applies server-specific configurations
     */
    @Override
    public Configuration initConfiguration(String serverName, Map<String, String> options) {
        // start with built-in Hadoop configuration that loads core-site.xml
        Configuration configuration = new Configuration();

        // add all site files as URL resources to the configuration, no resources will be added from the classpath
        String serverDirectoryName = SERVER_CONFIG_DIR_PREFIX + serverName;
        File serverDirectory = new File(serverDirectoryName);
        if (!serverDirectory.exists() || !serverDirectory.isDirectory() || !serverDirectory.canRead()) {
            LOG.warn("Directory {} does not exist, no configuration resources are added for server {}", serverDirectoryName, serverName);
        } else {
            LOG.debug("Using directory {} for server {} configuration", serverDirectoryName, serverName);
            addSiteFilesAsResources(configuration, serverName, serverDirectory);
        }

        //TODO: do we need whitelisting of properties
        if (options != null) {
            options.forEach(configuration::set);
        }

        return configuration;
    }

    private void addSiteFilesAsResources(Configuration configuration, String serverName, File directory) {
        // add all *-site.xml files inside the server config directory as configuration resources
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory.toPath(), "*-site.xml")) {
            for (Path path : stream) {
                URL resourceURL = path.toUri().toURL();
                LOG.debug("adding configuration resource from {}", resourceURL);
                configuration.addResource(resourceURL);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to read configuration for server %s from %s",
                    serverName, directory.getAbsolutePath()), e);
        }
    }
}
