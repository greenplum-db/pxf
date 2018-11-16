package org.greenplum.pxf.api.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class HDFSPlugin extends BasePlugin {
    private static final Log LOG = LogFactory.getLog(HDFSPlugin.class);

    public static final String DEFAULT_SERVER_NAME = "default";
    public static final String PXF_CONF_PROPERTY = "pxf.conf";
    private static final String SERVER_CONFIG_DIR_PREFIX =
            System.getProperty(PXF_CONF_PROPERTY) +
                    File.separator +
                    "servers" +
                    File.separator;

    protected Configuration configuration;

    /**
     * Initialize the plugin for the incoming request
     *
     * @param inputData data provided in the request
     */
    @Override
    public void initialize(InputData inputData) {
        super.initialize(inputData);
        initConfiguration();
    }

    /**
     * Initializes a configuration object that applies server-specific configurations
     */
    private void initConfiguration() {

        // start with built-in Hadoop configuration that loads core-site.xml
        configuration = new Configuration();

        // add all other *-site.xml files from default cluster will be added as "default" resources
        // by JobConf class or other Hadoop libraries on as-needed basis


        if (!DEFAULT_SERVER_NAME.equals(inputData.getServerName())) {
            // determine full path for server configuration directory
            String directoryName = SERVER_CONFIG_DIR_PREFIX + inputData.getServerName();
            File serverDirectory = new File(directoryName);
            if (!serverDirectory.exists()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Directory " + directoryName + " does not exist");
                }
                return;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Using directory " + directoryName + " for server " + inputData.getServerName() + " configuration");
            }
            addSiteFilesAsResources(configuration, serverDirectory);
        }

        // force properties to load by doing a get
        inputData.getUserPropertiesStream()
                .forEach(entry -> configuration.set(entry.getKey()
                        .substring(InputData.USER_PROP_PREFIX.length()), entry.getValue()));
    }

    private void addSiteFilesAsResources(Configuration configuration, File directory) {
        // add all *-site.xml files inside the server config directory as configuration resources
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory.toPath(), "*-site.xml")) {
            for (Path path : stream) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("adding configuration resource from " + path.toUri().toURL());
                }
                configuration.addResource(path.toUri().toURL());
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to read configuration for server " + inputData.getServerName() + " from " + directory.getAbsolutePath(), e);
        }
    }
}
