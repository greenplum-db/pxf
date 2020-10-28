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
    private static final int EXPECTED_NUM_OF_TOKS = 8;

    private final String inputFormatName;
    private final String propertiesString;
    private final String partitionKeys;
    private final String delimiter;
    private final String colTypes;
    private final int skipHeader;
    private final List<Integer> hiveIndexes;
    private final String allColumnNames;

    public HiveUserData(String inputFormatName,
                        String propertiesString,
                        String partitionKeys,
                        String delimiter,
                        String colTypes,
                        int skipHeader,
                        List<Integer> hiveIndexes,
                        String allColumnNames) {

        this.inputFormatName = inputFormatName;
        this.propertiesString = propertiesString;
        this.partitionKeys = partitionKeys;
        this.delimiter = (delimiter == null ? "0" : delimiter);
        this.colTypes = colTypes;
        this.skipHeader = skipHeader;
        this.hiveIndexes = hiveIndexes;
        this.allColumnNames = allColumnNames;
    }

    /**
     * Returns input format of a fragment
     *
     * @return input format of a fragment
     */
    public String getInputFormatName() {
        return inputFormatName;
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
     * The method returns all the column types
     *
     * @return colTypes
     */
    public String getColTypes() {
        return colTypes;
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
     * Returns number of header rows
     *
     * @return skipHeader
     */
    public int getSkipHeader() {
        return skipHeader;
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

    /**
     * Returns a comma-separated list of column names defined in the Hive
     * table definition
     *
     * @return the comma-separated list of column names
     */
    public String getAllColumnNames() {
        return allColumnNames;
    }

    @Override
    public String toString() {
        return inputFormatName + HiveUserData.HIVE_UD_DELIM
                + propertiesString + HiveUserData.HIVE_UD_DELIM
                + partitionKeys + HiveUserData.HIVE_UD_DELIM
                + delimiter + HiveUserData.HIVE_UD_DELIM
                + colTypes + HiveUserData.HIVE_UD_DELIM
                + skipHeader + HiveUserData.HIVE_UD_DELIM
                + (hiveIndexes != null ? hiveIndexes.stream().map(String::valueOf).collect(Collectors.joining(",")) : "null") + HiveUserData.HIVE_UD_DELIM
                + allColumnNames;
    }

}
