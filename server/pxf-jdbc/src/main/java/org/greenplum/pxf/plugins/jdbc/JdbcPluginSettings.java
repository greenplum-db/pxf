package org.greenplum.pxf.plugins.jdbc;

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

import org.greenplum.pxf.api.model.RequestContext;

import org.apache.hadoop.conf.Configuration;

/**
 * PXF-JDBC Plugin settings' definitions.
 *
 * Note settings' values are stored separately.
 */
public enum JdbcPluginSettings {
    jdbcDriver("JDBC_DRIVER", "jdbc.driver"),
    jdbcUrl("DB_URL", "jdbc.url"),
    jdbcUser("USER", "jdbc.user"),
    jdbcPassword("PASS", "jdbc.pass"),
    preQuerySql(null, "jdbc.pre_query.sql"),
    stopIfPreQueryFails("STOP_IF_PRE_FAILS", "jdbc.pre_query.stop_if_fails"),
    batchSize("BATCH_SIZE", "jdbc.batch_size"),
    poolSize("POOL_SIZE", "jdbc.pool_size"),
    partitionBy("PARTITION_BY", "jdbc.partition.by"),
    partitionRange("RANGE", "jdbc.partition.range"),
    partitionInterval("INTERVAL", "jdbc.partition.interval");

    private final String contextName;
    private final String configurationName;

    /**
     * Construct a JdbcPluginSetting
     *
     * 'null' argument denotes a setting that must not be loaded either from context or configuration
     */
    JdbcPluginSettings(String contextName, String configurationName) {
        this.contextName = contextName;
        this.configurationName = configurationName;
    }

    /**
     * Load this setting from {@link RequestContext}.
     *
     * Use 'loadFromContextOrConfugiration'! This function is made public for special cases only,
     * e.g. when it is necessary to load two different settings from one source.
     */
    public String loadFromContext(RequestContext context) {
        if (context == null) {
            return null;
        }
        return contextName != null ? context.getOption(contextName) : null;
    }

    /**
     * Load this setting from {@link Configuration}.
     *
     * Use 'loadFromContextOrConfugiration'! This function is made public for special cases only,
     * e.g. when it is necessary to load two different settings from one source.
     */
    public String loadFromConfiguration(Configuration configuration) {
        if (configuration == null) {
            return null;
        }
        return configurationName != null ? configuration.get(configurationName) : null;
    }

    /**
     * Load this setting from {@link RequestContext}.
     * If setting is not present there, load it from {@link Configuration}.
     */
    public String loadFromContextOrConfiguration(RequestContext context, Configuration configuration) {
        String result = loadFromContext(context);
        if (result == null) {
            result = loadFromConfiguration(configuration);
        }
        return result;
    }

    @Override
    public String toString() {
        return "{" +
            (contextName == null ? "" : (contextName + " (option)")) +
            (contextName != null && configurationName != null ? " | " : "") +
            (configurationName == null ? "" : (configurationName + " (configuration parameter)"))
            + "}";
    }
};
