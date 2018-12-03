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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Base class for all plugin types (Accessor, Resolver, Fragmenter, ...).
 * Manages the meta data.
 */
public class BasePlugin implements Plugin {

    private static final String PXF_CONF_PROPERTY = "pxf.conf";
    private static final String SERVER_CONFIG_DIR_PREFIX =
            System.getProperty(PXF_CONF_PROPERTY) + File.separator + "servers" + File.separator;

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected Configuration configuration;
    protected RequestContext context;

    private boolean initialized = false;

    /**
     * Initialize the plugin for the incoming request
     *
     * @param requestContext data provided in the request
     */
    @Override
    public void initialize(RequestContext requestContext) {
        this.context = requestContext;
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

        // add all site files as URL resources to the configuration, no resources will be added from the classpath
        String serverName = context.getServerName();
        String serverDirectoryName = SERVER_CONFIG_DIR_PREFIX + serverName;
        File serverDirectory = new File(serverDirectoryName);
        if (!serverDirectory.exists() || !serverDirectory.isDirectory() || !serverDirectory.canRead()) {
            LOG.warn("Directory {} does not exist, no configuration resources are added for server {}", serverDirectoryName, serverName);
        } else {
            LOG.debug("Using directory {} for server {} configuration", serverDirectoryName, serverName);
            addSiteFilesAsResources(configuration, serverName, serverDirectory);
        }

        //TODO: do we need whitelisting of properties
        context.getOptions().forEach(configuration::set);
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
