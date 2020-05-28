package org.greenplum.pxf.api.model;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Component
public class BaseConfigurationFactory implements ConfigurationFactory {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final File serversConfigDirectory;

    @Autowired
    public BaseConfigurationFactory(PxfServerProperties pxfServerProperties) {
        this(new File(String.format("%s%sservers", pxfServerProperties.getConf(), File.separator)));
    }

    BaseConfigurationFactory(File serversConfigDirectory) {
        this.serversConfigDirectory = serversConfigDirectory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration initConfiguration(String configDirectory, String serverName, String userName, Map<String, String> additionalProperties) {
        // start with built-in Hadoop configuration that loads core-site.xml
        LOG.debug("Initializing configuration for server {}", serverName);
        Configuration configuration = new Configuration();
        // while implementing multiple kerberized support we noticed that non-kerberized hadoop
        // access was trying to use SASL-client authentication. Setting the fallback to simple auth
        // allows us to still access non-kerberized hadoop clusters when there exists at least one
        // kerberized hadoop cluster. The root cause is that UGI has static fields and many hadoop
        // libraries depend on the state of the UGI
        // allow using SIMPLE auth for non-Kerberized HCFS access by SASL-enabled IPC client
        // that is created due to the fact that it uses UGI.isSecurityEnabled
        // and will try to use SASL if there is at least one Kerberized Hadoop cluster
        configuration.set(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, "true");

        // set synthetic property pxf.session.user so that is can be used in config files for interpolation in other properties
        // for example in JDBC when setting session authorization from a proxy user to the end-user
        configuration.set(PXF_SESSION_USER_PROPERTY, userName);

        File[] serverDirectories = null;
        Path p = Paths.get(configDirectory);

        if (p.isAbsolute()) {
            File f = p.toFile();
            if (f.exists() && f.isDirectory() && f.canRead()) {
                serverDirectories = new File[]{f};
            }
        } else {
            serverDirectories = serversConfigDirectory
                    .listFiles(f ->
                            f.isDirectory() &&
                                    f.canRead() &&
                                    StringUtils.equalsIgnoreCase(configDirectory, f.getName()));
        }

        if (ArrayUtils.isEmpty(serverDirectories)) {
            LOG.debug("Directory {}{}{} does not exist or cannot be read by PXF, no configuration resources are added for server {}",
                    serversConfigDirectory, File.separator, configDirectory, serverName);
        } else if (serverDirectories.length > 1) {
            throw new IllegalStateException(String.format(
                    "Multiple directories found for server %s. Server directories are expected to be case-insensitive.", serverName
            ));
        } else {
            // add all site files as URL resources to the configuration, no resources will be added from the classpath
            LOG.debug("Using directory {} for server {} configuration", serverDirectories[0], serverName);
            processServerResources(configuration, serverName, serverDirectories[0]);
        }

        // add additional properties, if provided
        if (additionalProperties != null) {
            LOG.debug("Adding {} additional properties to configuration for server {}", additionalProperties.size(), serverName);
            additionalProperties.forEach(configuration::set);
        }

        // add user configuration
        if (!ArrayUtils.isEmpty(serverDirectories)) {
            processUserResource(configuration, serverName, userName, serverDirectories[0]);
        }

        try {
            // We need to set the restrict system properties to false so
            // variables in the configuration get replaced by system property
            // values
            configuration.setRestrictSystemProps(false);
        } catch (NoSuchMethodError e) {
            // Expected exception for MapR
        }

        return configuration;
    }

    private void processServerResources(Configuration configuration, String serverName, File directory) {
        // add all *-site.xml files inside the server config directory as configuration resources
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory.toPath(), "*-site.xml")) {
            for (Path path : stream) {
                URL resourceURL = path.toUri().toURL();
                LOG.debug("Adding configuration resource for server {} from {}", serverName, resourceURL);
                configuration.addResource(resourceURL);
                // store the path to the resource in the configuration in case plugins need to access the files again
                configuration.set(String.format("%s.%s", PXF_CONFIG_RESOURCE_PATH_PROPERTY, path.getFileName().toString()), resourceURL.toString());
            }
            // add the server directory itself as configuration property in case plugins need to access non-site-xml files
            configuration.set(PXF_CONFIG_SERVER_DIRECTORY_PROPERTY, directory.getCanonicalPath());

        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to read configuration for server %s from %s",
                    serverName, directory.getAbsolutePath()), e);
        }
    }

    private void processUserResource(Configuration configuration, String serverName, String userName, File directory) {
        // add user config file as configuration resource
        try {
            Path path = Paths.get(String.format("%s/%s-user.xml", directory.toPath(), userName));
            if (Files.exists(path)) {
                Configuration userConfiguration = new Configuration(false);
                URL resourceURL = path.toUri().toURL();
                userConfiguration.addResource(resourceURL);
                LOG.debug("Adding user properties for server {} from {}", serverName, resourceURL);
                userConfiguration.forEach(entry -> configuration.set(entry.getKey(), entry.getValue()));
                configuration.set(String.format("%s.%s", PXF_CONFIG_RESOURCE_PATH_PROPERTY, path.getFileName().toString()), resourceURL.toString());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to read user configuration for user %s using server %s from %s",
                    userName, serverName, directory.getAbsolutePath()), e);
        }
    }
}
