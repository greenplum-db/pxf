package org.greenplum.pxf.api.model;

import org.apache.commons.lang.StringUtils;
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
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final File serversConfigDirectory;

    public BaseConfigurationFactory() {
        this(SERVERS_CONFIG_DIR);
    }

    BaseConfigurationFactory(File serversConfigDirectory) {
        this.serversConfigDirectory = serversConfigDirectory;
    }

    /**
     * Returns the static instance for this factory
     *
     * @return the static instance for this factory
     */
    public static BaseConfigurationFactory getInstance() {
        return instance;
    }

    @Override
    public Configuration initConfiguration(String serverName, Map<String, String> additionalProperties) {
        // start with built-in Hadoop configuration that loads core-site.xml
        Configuration configuration = new Configuration();

        File[] serverDirectories = serversConfigDirectory
                .listFiles(f -> f.isDirectory() &&
                        f.canRead() &&
                        StringUtils.equalsIgnoreCase(serverName, f.getName()));

        String serverDirectoryName = serversConfigDirectory + serverName;

        if (serverDirectories == null || serverDirectories.length == 0) {
            LOG.warn("Directory {} does not exist, no configuration resources are added for server {}", serverDirectoryName, serverName);
        } else if (serverDirectories.length > 1) {
            throw new IllegalStateException(String.format(
                    "Multiple directories found for server %s. Server directories are expected to be case-insensitive.", serverName));
        } else {
            // add all site files as URL resources to the configuration, no resources will be added from the classpath
            LOG.debug("Using directory {} for server {} configuration", serverDirectoryName, serverName);
            addSiteFilesAsResources(configuration, serverName, serverDirectories[0]);
        }

        // add additional properties, if provided
        if (additionalProperties != null) {
            additionalProperties.forEach(configuration::set);
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
