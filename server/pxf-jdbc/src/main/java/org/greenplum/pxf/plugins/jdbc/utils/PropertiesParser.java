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

import java.util.Properties;

/**
 * Class to parse properties from other objects
 */
public class PropertiesParser {
    private static final String EXCEPTION_HELP = "\n" + "Property name and value must be separated by '='; Properties must be separated by ','. Use '\\=' or '\\,' to pass '=' or ',' as part of property name or value";

    /**
     * Parse properties from string.
     *
     * ',' divides properties; '=' divides key-value pairs.
     * To escape divisors, '\,' and '\=' can be used.
     *
     * @param properties
     * @return new Properties object
     *
     * @throws IllegalArgumentException if a parsing error occurs
     */
    public static Properties parse(String properties) {
        Properties result = new Properties();

        // Split properties string into distinct properties
        String[] propertiesSplit = properties.split("(?<!\\\\),");
        for (String property : propertiesSplit) {
            // Split property into key-value pair
            String[] propertyPair = property.split("(?<!\\\\)=", 2);

            if (propertyPair.length != 2) {
                throw new IllegalArgumentException(String.format(
                    "Property '%s' has incorrect format (property key or value is missing)." + EXCEPTION_HELP,
                    property
                ));
            }

            // Replace escaped characters with actual ones
            for (int i = 0; i < 2; i++) {
                propertyPair[i] = propertyPair[i].replaceAll("\\\\,", ",");
                propertyPair[i] = propertyPair[i].replaceAll("\\\\=", "=");
            }

            if (propertyPair[0].length() == 0) {
                throw new IllegalArgumentException(String.format(
                    "Property '%s' has incorrect format (property key has length 0)." + EXCEPTION_HELP,
                    property
                ));
            }
            if (propertyPair[1].length() == 0) {
                throw new IllegalArgumentException(String.format(
                    "Property '%s' has incorrect format (property value has length 0)." + EXCEPTION_HELP,
                    property
                ));
            }

            result.setProperty(propertyPair[0], propertyPair[1]);
        }

        return result;
    }
}
