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
 * A tool class to change PXF behaviour for some external databases.
 *
 * To implement a new DbProduct:
 * 1. Create a class inheriting from this one and implement logic there;
 * 2. Add new class constructor to getDbProduct() function.
 */
public abstract class DbProduct {
    private static final Logger LOG = LoggerFactory.getLogger(DbProduct.class);

    /**
     * Get an instance of some class - the database product
     *
     * @param dbName A full name of the database
     * @return a DbProduct of the required class
     */
    public static DbProduct getDbProduct(String dbName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Database product name is '" + dbName + "'");
        }
        if (dbName.toUpperCase().contains("POSTGRES"))
            return new PostgresProduct();
        else if (dbName.toUpperCase().contains("MYSQL"))
            return new MysqlProduct();
        else if (dbName.toUpperCase().contains("ORACLE"))
            return new OracleProduct();
        else if (dbName.toUpperCase().contains("MICROSOFT"))
            return new MicrosoftProduct();
        else
            return new PostgresProduct();
    }

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
    public abstract String wrapTimestamp(Object val);
}
