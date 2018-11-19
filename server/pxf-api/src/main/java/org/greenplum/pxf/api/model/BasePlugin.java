package org.greenplum.pxf.api.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Base class for all plugin types (Accessor, Resolver, BaseFragmenter, ...).
 * Manages the meta data.
 */
public class BasePlugin implements Plugin {

    private static final String DEFAULT_SERVER_NAME = "default";
    private static final String PXF_CONF_PROPERTY = "pxf.conf";
    private static final String SERVER_CONFIG_DIR_PREFIX =
            System.getProperty(PXF_CONF_PROPERTY) + File.separator + "servers" + File.separator;

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected Configuration configuration;
    protected RequestContext requestContext;

    private boolean initialized = false;

    /**
     * Initialize the plugin for the incoming request
     *
     * @param requestContext data provided in the request
     */
    @Override
    public void initialize(RequestContext requestContext) {
        this.requestContext = requestContext;
        initConfiguration();

        this.initialized = true;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    /**
     * Determines if the plugin is initialized
     *
     * @return true if initialized, false otherwise
     */
    public final boolean isInitialized() {
        return initialized;
    }



    /**
     * Initializes a configuration object that applies server-specific configurations
     */
    private void initConfiguration() {

        // start with built-in Hadoop configuration that loads core-site.xml
        configuration = new Configuration();

        // add all other *-site.xml files from default cluster will be added as "default" resources
        // by JobConf class or other Hadoop libraries on as-needed basis


        if (!DEFAULT_SERVER_NAME.equals(requestContext.getServerName())) {
            // determine full path for server configuration directory
            String directoryName = SERVER_CONFIG_DIR_PREFIX + requestContext.getServerName();
            File serverDirectory = new File(directoryName);
            if (!serverDirectory.exists()) {
                LOG.debug("Directory %s does not exist", directoryName);
                return;
            }

            LOG.debug("Using directory " + directoryName + " for server " + requestContext.getServerName() + " configuration");

            addSiteFilesAsResources(configuration, serverDirectory);
        }

        requestContext.getUserPropertiesStream()
                .forEach(entry -> configuration.set(entry.getKey()
                        .substring(RequestContext.USER_PROP_PREFIX.length()), entry.getValue()));
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
            throw new RuntimeException("Unable to read configuration for server " + requestContext.getServerName() + " from " + directory.getAbsolutePath(), e);
        }
    }
}
