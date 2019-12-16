package org.greenplum.pxf.plugins.jdbc.utils;

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

/**
 * A tool class to change PXF-JDBC plugin behaviour for certain external databases
 */
public enum DbProduct {
    MICROSOFT {
        @Override
        public String wrapDate(Object val){
            return "'" + val + "'";
        }

        @Override
        public String buildSessionQuery(String key, String value) {
            return String.format("SET %s %s", key, value);
        }
    },

    MYSQL {
        @Override
        public String wrapDate(Object val){
            return "DATE('" + val + "')";
        }
    },

    ORACLE {
        @Override
        public String wrapDate(Object val) {
            return "to_date('" + val + "', 'YYYY-MM-DD')";
        }

        @Override
        public String wrapTimestamp(Object val) {
            return "to_timestamp('" + val + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
        }

        @Override
        public String buildSessionQuery(String key, String value) {
            return String.format("ALTER SESSION SET %s = %s", key, value);
        }
    },

    POSTGRES {
        @Override
        public String wrapDate(Object val) {
            return "date'" + val + "'";
        }
    },

    S3_SELECT {
        @Override
        public String wrapDate(Object val) {
            return "TO_TIMESTAMP('" + val + "')";
        }

        @Override
        public String wrapTimestamp(Object val) {
            return "TO_TIMESTAMP('" + val + "')";
        }
    };

    /**
     * Wraps a given date value the way required by target database
     *
     * @param val {@link java.sql.Date} object to wrap
     * @return a string with a properly wrapped date object
     */
    public abstract String wrapDate(Object val);

    /**
     * Wraps a given timestamp value the way required by target database
     *
     * @param val {@link java.sql.Timestamp} object to wrap
     * @return a string with a properly wrapped timestamp object
     */
    public String wrapTimestamp(Object val) {
        return "'" + val + "'";
    }

    /**
     * Build a query to set session-level variables for target database
     *
     * @param key variable name (key)
     * @param value variable value
     *
     * @return a string with template SET query
     */
    public String buildSessionQuery(String key, String value) {
        return String.format("SET %s = %s", key, value);
    }

    /**
     * Get DbProduct for database by database name
     *
     * @param dbName database name
     * @return a DbProduct of the required class
     */
    public static DbProduct getDbProduct(String dbName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Database product name is '" + dbName + "'");
        }

        dbName = dbName.toUpperCase();
        DbProduct result;
        if (dbName.contains("MICROSOFT"))
            result = DbProduct.MICROSOFT;
        else if (dbName.contains("MYSQL"))
            result = DbProduct.MYSQL;
        else if (dbName.contains("ORACLE"))
            result = DbProduct.ORACLE;
        else
            result = DbProduct.POSTGRES;

        if (LOG.isDebugEnabled()) {
            LOG.debug("DbProduct '" + result + "' is used");
        }
        return result;
    }

    private static final Logger LOG = LoggerFactory.getLogger(DbProduct.class);
}
