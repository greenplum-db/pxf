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

/**
 * Class which is a carrier for user data in Hive fragment.
 *
 */
public class HiveUserData {

    public static final String HIVE_UD_DELIM = "!HUDD!";
    private static final int EXPECTED_NUM_OF_TOKS = 8;

    private String inputFormatName;
    private String serdeClassName;
    private String propertiesString;
    private String partitionKeys;
    private boolean filterInFragmenter;
    private String delimiter;
    private String colTypes;
    private int skipHeader;

    public HiveUserData(String inputFormatName, String serdeClassName,
            String propertiesString, String partitionKeys,
            boolean filterInFragmenter,
            String delimiter,
            String colTypes,
            int skipHeader) {

        this.inputFormatName = inputFormatName;
        this.serdeClassName = serdeClassName;
        this.propertiesString = propertiesString;
        this.partitionKeys = partitionKeys;
        this.filterInFragmenter = filterInFragmenter;
        this.delimiter = (delimiter == null ? "0" : delimiter);
        this.colTypes = colTypes;
        this.skipHeader = skipHeader;
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
     * Returns SerDe class name
     *
     * @return SerDe class name
     */
    public String getSerdeClassName() {
        return serdeClassName;
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
     * Returns whether filtering was done in fragmenter
     *
     * @return true if filtering was done in fragmenter
     */
    public boolean isFilterInFragmenter() {
        return filterInFragmenter;
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

    @Override
    public String toString() {
        return inputFormatName + HiveUserData.HIVE_UD_DELIM
                + serdeClassName + HiveUserData.HIVE_UD_DELIM
                + propertiesString + HiveUserData.HIVE_UD_DELIM
                + partitionKeys + HiveUserData.HIVE_UD_DELIM
                + filterInFragmenter + HiveUserData.HIVE_UD_DELIM
                + delimiter + HiveUserData.HIVE_UD_DELIM
                + colTypes +  HiveUserData.HIVE_UD_DELIM
                + skipHeader;
    }

    public static class Builder {
        private String serdeClassName;
        private String partitionKeys;
        private int skipHeader;

        public HiveUserData build() {
            return new HiveUserData(
                    null,
                    serdeClassName,
                    null,
                    partitionKeys,
                    false,
                    null,
                    null,
                    skipHeader);
        }

        public Builder withSerdeClassName(String s) {
            serdeClassName = s;
            return this;
        }

        public Builder withPartitionKeys(String s) {
            partitionKeys = s;
            return this;
        }

        public Builder withSkipHeader(int n) {
            skipHeader = n;
            return this;
        }
    }
}
