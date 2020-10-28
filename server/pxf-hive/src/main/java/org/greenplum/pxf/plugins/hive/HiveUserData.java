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

package org.greenplum.pxf.plugins.hive;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Class which is a carrier for user data in Hive fragment.
 */
public class HiveUserData {

    public static final String HIVE_UD_DELIM = "!HUDD!";
    private static final int EXPECTED_NUM_OF_TOKS = 4;

    private final String propertiesString;
    private final String partitionKeys;
    private final String delimiter;
    private final List<Integer> hiveIndexes;

    public HiveUserData(String propertiesString,
                        String partitionKeys,
                        String delimiter,
                        List<Integer> hiveIndexes) {

        this.propertiesString = propertiesString;
        this.partitionKeys = partitionKeys;
        this.delimiter = (delimiter == null ? "0" : delimiter);
        this.hiveIndexes = hiveIndexes;
    }

    /**
     * Returns properties string needed for SerDe initialization
     *
     * @return properties string needed for SerDe initialization
     */
    public String getPropertiesString() {
        return propertiesString;
    }

    /**
     * Returns partition keys
     *
     * @return partition keys
     */
    public String getPartitionKeys() {
        return partitionKeys;
    }

    /**
     * Returns field delimiter
     *
     * @return field delimiter
     */
    public String getDelimiter() {
        return delimiter;
    }

    /**
     * The method returns expected number of tokens in raw user data
     *
     * @return number of tokens in raw user data
     */
    public static int getNumOfTokens() {
        return EXPECTED_NUM_OF_TOKS;
    }

    /**
     * Returns a list of indexes corresponding to columns on the Hive table
     * that will be retrieved during the query
     *
     * @return the list of indexes
     */
    public List<Integer> getHiveIndexes() {
        return hiveIndexes;
    }

    @Override
    public String toString() {
        return propertiesString + HiveUserData.HIVE_UD_DELIM
                + partitionKeys + HiveUserData.HIVE_UD_DELIM
                + delimiter + HiveUserData.HIVE_UD_DELIM
                + (hiveIndexes != null ? hiveIndexes.stream().map(String::valueOf).collect(Collectors.joining(",")) : "null");
    }

}
