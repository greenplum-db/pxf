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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.EnumAggregationType;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.api.utilities.Utilities;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Common configuration available to all PXF plugins. Represents input data
 * coming from client applications, such as GPDB.
 */
@Getter
@Setter
public class RequestContext {

    /**
     * The request type can be used to later determine whether we
     * are in a read, write or fragmenter call.
     */
    private RequestType requestType;

    public RequestType getRequestType() {
        return requestType;
    }

    /**
     * The request type can be set when parsing http parameters, etc.
     * {@link org.greenplum.pxf.service.HttpRequestParser#parseRequest()}
     */
    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    public enum RequestType {
        READ_BRIDGE,
        WRITE_BRIDGE,
    }

    // ----- NAMED PROPERTIES -----

    /**
     * The fully-qualified class name for the java class that was defined as
     * Accessor.
     */
    private String accessor;

    /**
     * The aggregate type, i.e - count, min, max, etc
     */
    private EnumAggregationType aggType;

    /**
     * The name of the server configuration for this request.
     */
    @Setter(AccessLevel.NONE)
    private String config;

    /**
     * The server configuration associated to the server that this request is
     * accessing
     */
    private Configuration configuration;

    /**
     * The data source of the required resource (i.e a file path or a table
     * name).
     */
    private String dataSource;

    /**
     * The fully-qualified class name for the java class that was defined as
     * Fragmenter or null if no fragmenter was defined.
     */
    private String fragmenter;

    /**
     * The index of a fragment in a file
     */
    private int fragmentIndex;

    /**
     * Metadata for a fragment
     */
    @Getter(AccessLevel.NONE)
    private FragmentMetadata fragmentMetadata;

    /**
     * The filter string, <tt>null</tt> if #hasFilter is <tt>false</tt>.
     */
    private String filterString;

    /**
     * Profile-centric metadata
     */
    private Object metadata;

    /**
     * The current output format, either {@link OutputFormat#TEXT} or
     * {@link OutputFormat#GPDBWritable}.
     */
    private OutputFormat outputFormat;

    /**
     * The Greenplum command count
     */
    private int gpCommandCount;

    /**
     * The Greenplum session ID
     */
    private int gpSessionId;

    /**
     * The server name providing the service.
     */
    private String host;

    /**
     * The Kerberos token information.
     */
    private String token;

    /**
     * Statistics parameter. Returns the max number of fragments to return for
     * ANALYZE sampling. The value is set in Greenplum side using the GUC
     * pxf_stats_max_fragments.
     */
    @Setter(AccessLevel.NONE)
    private int statsMaxFragments = 0;

    /**
     * Statistics parameter. Returns a number between 0.0001 and 1.0,
     * representing the sampling ratio on each fragment for ANALYZE sampling.
     * The value is set in Greenplum side based on ANALYZE computations and the
     * number of sampled fragments.
     */
    @Setter(AccessLevel.NONE)
    private float statsSampleRatio = 0;

    /**
     * Number of attributes projected in query.
     * <p>
     * Example:
     * SELECT col1, col2, col3... : number of attributes projected - 3
     * SELECT col1, col2, col3... WHERE col4=a : number of attributes projected - 4
     * SELECT *... : number of attributes projected - 0
     */
    private int numAttrsProjected;

    /**
     * The plugin configuration
     */
    private PluginConf pluginConf;

    /**
     * The server port providing the service.
     */
    private int port;

    /**
     * The name of the profile associated to this request.
     */
    private String profile;

    /**
     * The scheme defined at the profile level
     */
    private String profileScheme;

    /**
     * The protocol defined at the foreign data wrapper (FDW) level
     */
    @Getter(AccessLevel.NONE)
    private String protocol;

    /**
     * The fully-qualified class name for the java class that was defined as
     * Resolver.
     */
    private String resolver;

    /**
     * The format defined at the FDW foreign table level
     */
    private String format;

    /**
     * Encapsulates CSV parsing information
     */
    private GreenplumCSV greenplumCSV = new GreenplumCSV();

    /**
     * The name of the recordkey column. It can appear in any location in the
     * columns list. By specifying the recordkey column, the user declares that
     * he is interested to receive for every record retrieved also the the
     * recordkey in the database. The recordkey is present in HBase table (it is
     * called rowkey), and in sequence files. When the HDFS storage element
     * queried will not have a recordkey and the user will still specify it in
     * the "create external table" statement, then the values for this field
     * will be null. This field will always be the first field in the tuple
     * returned.
     */
    private ColumnDescriptor recordkeyColumn;

    /**
     * The contents of pxf_remote_service_login set in Greenplum. Should the
     * user set it to an empty string this function will return null.
     */
    private String remoteLogin;

    /**
     * The contents of pxf_remote_service_secret set in Greenplum. Should the
     * user set it to an empty string this function will return null.
     */
    private String remoteSecret;

    /**
     * The current segment ID in Greenplum.
     */
    private int segmentId;

    /**
     * The name of the origin Greenplum schema name.
     */
    private String schemaName;

    /**
     * The name of the origin Greenplum table name.
     */
    private String tableName;

    /**
     * The transaction ID for the current Greenplum query.
     */
    private String transactionId;

    /**
     * The name of the server to access. The name will be used to build
     * a path for the config files (i.e. $PXF_BASE/servers/$serverName/*.xml)
     */
    @Setter(AccessLevel.NONE)
    private String serverName = "default";

    /**
     * The number of segments in Greenplum.
     */
    private int totalSegments;

    /**
     * The list of column descriptors
     */
    private List<ColumnDescriptor> tupleDescription = new ArrayList<>();

    /**
     * The identity of the end-user making the request.
     */
    private String user;

    /**
     * The encoding of the source data
     */
    private Charset dataEncoding;

    /**
     * The encoding of the database
     */
    private Charset databaseEncoding;

    /**
     * The version of the PXF API the client implements
     */
    private String clientApiVersion;

    /**
     * Additional Configuration Properties to be added to configuration for
     * the request
     */
    private Map<String, String> additionalConfigProps;

    /**
     * USER-DEFINED OPTIONS other than NAMED PROPERTIES
     */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * Returns a String value of the given option or a default value if the option was not provided
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @return string value of the option or default value if the option was not provided
     */
    public String getOption(String option, String defaultValue) {
        return options.getOrDefault(option, defaultValue);
    }

    /**
     * Returns an integer value of the given option or a default value if the option was not provided.
     * Will throw an IllegalArgumentException if the option value can not be represented as an integer
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @return integer value of the option or default value if the option was not provided
     */
    public int getOption(String option, int defaultValue) {
        return getOption(option, defaultValue, false);
    }

    /**
     * Returns a long value of the given option or a default value if the option was not provided.
     * Will throw an IllegalArgumentException if the option value can not be represented as a long
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @return long value of the option or default value if the option was not provided
     */
    public long getOption(String option, long defaultValue) {
        return getOption(option, defaultValue, false);
    }

    /**
     * Returns an integer value of the given option or a default value if the option was not provided.
     * Will throw an IllegalArgumentException if the option value can not be represented as an integer or
     * if the integer is negative but only natural integer was expected.
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @param naturalOnly  true if the integer is expected to be non-negative (natural), false otherwise
     * @return integer value of the option or default value if the option was not provided
     */
    public int getOption(String option, int defaultValue, boolean naturalOnly) {
        int result = defaultValue;
        String value = options.get(option);
        if (value != null) {
            try {
                result = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                        "Property %s has incorrect value %s : must be a%s integer", option, value, naturalOnly ? " non-negative" : "n"), e);
            }
            if (naturalOnly && result < 0) {
                throw new IllegalArgumentException(String.format(
                        "Property %s has incorrect value %s : must be a non-negative integer", option, value));
            }
        }
        return result;
    }

    /**
     * Returns a long value of the given option or a default value if the option was not provided.
     * Will throw an IllegalArgumentException if the option value can not be represented as a long or
     * if the long is negative but only natural long was expected.
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @param naturalOnly  true if the long is expected to be non-negative (natural), false otherwise
     * @return long value of the option or default value if the option was not provided
     */
    public long getOption(String option, long defaultValue, boolean naturalOnly) {
        long result = defaultValue;
        String value = options.get(option);
        if (value != null) {
            try {
                result = Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                        "Property %s has incorrect value %s : must be a%s long", option, value, naturalOnly ? " non-negative" : "n"), e);
            }
            if (naturalOnly && result < 0) {
                throw new IllegalArgumentException(String.format(
                        "Property %s has incorrect value %s : must be a non-negative long", option, value));
            }
        }
        return result;
    }

    /**
     * Returns a string value of the given option or null if the option was not provided.
     *
     * @param option name of the option
     * @return string value of the given option or null if the option was not provided.
     */
    public String getOption(String option) {
        return options.get(option);
    }

    /**
     * Returns a boolean value of the given option or a default value if the
     * option was not provided. Will throw an IllegalArgumentException if the
     * option value can not be represented as a boolean
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @return boolean value of the option or default value if the option was not provided
     */
    public boolean getOption(String option, boolean defaultValue) {
        boolean result;
        String value = options.get(option);
        if (value == null) {
            result = defaultValue;
        } else if (StringUtils.equalsIgnoreCase(value, "true")) {
            result = true;
        } else if (StringUtils.equalsIgnoreCase(value, "false")) {
            result = false;
        } else {
            throw new IllegalArgumentException(String.format(
                    "Property %s has incorrect value %s : must be either true or false", option, value));
        }
        return result;
    }

    /**
     * Adds an option with the given name and value to the set of options.
     *
     * @param name  name of the option
     * @param value value of the option
     */
    public void addOption(String name, String value) {
        options.put(name, value);
    }

    /**
     * Returns unmodifiable map of options.
     *
     * @return map of options, with keys as option names and values as option values
     */
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    /**
     * The data fragment.
     *
     * @return fragment metadata
     */
    @SuppressWarnings("unchecked")
    public <T extends FragmentMetadata> T getFragmentMetadata() {
        return (T) fragmentMetadata;
    }

    /**
     * Returns true if there is a filter string to parse.
     *
     * @return whether there is a filter string
     */
    public boolean hasFilter() {
        return filterString != null;
    }

    /**
     * Returns true if there is column projection.
     *
     * @return true if there is column projection, false otherwise
     */
    public boolean hasColumnProjection() {
        return numAttrsProjected > 0 && numAttrsProjected < tupleDescription.size();
    }

    /**
     * Returns the number of columns in tuple description.
     *
     * @return number of columns
     */
    public int getColumns() {
        return tupleDescription.size();
    }

    /**
     * Returns column index from tuple description.
     *
     * @param index index of column
     * @return column by index
     */
    public ColumnDescriptor getColumn(int index) {
        return tupleDescription.get(index);
    }

    /**
     * Sets the name of the server configuration for this request.
     *
     * @param config the directory name for the configuration
     */
    public void setConfig(String config) {
        if (StringUtils.isNotBlank(config) && !Utilities.isValidDirectoryName(config)) {
            fail("invalid CONFIG directory name '%s'", config);
        }
        this.config = config;
    }

    /**
     * Returns the contents of pxf_remote_service_login set in GPDB. Should the
     * user set it to an empty string this function will return null.
     *
     * @return remote login details if set, null otherwise
     */
    public String getLogin() {
        return remoteLogin;
    }

    /**
     * Returns the contents of pxf_remote_service_secret set in GPDB. Should the
     * user set it to an empty string this function will return null.
     *
     * @return remote password if set, null otherwise
     */
    public String getSecret() {
        return remoteSecret;
    }

    /**
     * Sets the name of the server in a multi-server setup.
     * If the name is blank, it is defaulted to "default"
     *
     * @param serverName the name of the server
     */
    public void setServerName(String serverName) {
        if (StringUtils.isNotBlank(serverName)) {

            if (!Utilities.isValidRestrictedDirectoryName(serverName)) {
                throw new IllegalArgumentException(String.format("Invalid server name '%s'", serverName));
            }

            this.serverName = serverName.toLowerCase();
        }
    }

    public void setStatsMaxFragments(int statsMaxFragments) {
        this.statsMaxFragments = statsMaxFragments;
        if (statsMaxFragments <= 0) {
            throw new IllegalArgumentException(String
                    .format("Wrong value '%d'. STATS_MAX_FRAGMENTS must be a positive integer",
                            statsMaxFragments));
        }
    }

    public void setStatsSampleRatio(float statsSampleRatio) {
        this.statsSampleRatio = statsSampleRatio;
        if (statsSampleRatio < 0.0001 || statsSampleRatio > 1.0) {
            throw new IllegalArgumentException(
                    "Wrong value '"
                            + statsSampleRatio
                            + "'. "
                            + "STATS_SAMPLE_RATIO must be a value between 0.0001 and 1.0");
        }
    }

    public void validate() {
        if ((statsSampleRatio > 0) != (statsMaxFragments > 0)) {
            fail("Missing parameter: STATS_SAMPLE_RATIO and STATS_MAX_FRAGMENTS must be set together");
        }

        if (requestType == RequestType.READ_BRIDGE) {
            // fragmenter is required for PXF read call only (PXF write
            // does not require a fragmenter)
            ensureNotNull("FRAGMENTER", fragmenter);
        }

        // accessor and resolver are user properties, might be missing if profile is not set
        ensureNotNull("ACCESSOR", accessor);
        ensureNotNull("RESOLVER", resolver);
    }

    private void ensureNotNull(String property, Object value) {
        if (value == null) {
            fail("Property %s has no value in the current request", property);
        }
    }

    private void fail(String message, Object... args) {
        String errorMessage = String.format(message, args);
        throw new IllegalArgumentException(errorMessage);
    }

    public String getProtocol() {
        return StringUtils.isNotBlank(protocol) ? protocol : inferProtocolName();
    }

    /**
     * Infers the protocol name from the profileScheme or profile
     * Introduced for backwards compatibility. Can be removed after
     * the external framework is no longer supported
     *
     * @return the inferred protocol name
     */
    private String inferProtocolName() {
        if (StringUtils.isBlank(profileScheme) && !StringUtils.isBlank(profile)) {
            return profile.contains(":") ? profile.split(":")[0] : profile;
            // When the profileScheme is not available, extract the profileScheme from the profile
            // for example hdfs:text will return hdfs profileScheme
        }
        return profileScheme;
    }

    /**
     * Infers the format name from the profile.
     * Introduced for backwards compatibility. Can be removed after
     * the external framework is no longer supported
     *
     * @return the inferred format name
     */
    public String inferFormatName() {
        if (!StringUtils.isBlank(profile) && profile.contains(":")) {
            return profile.split(":")[1];
        }
        // if the format name cannot be inferred from the profile
        // we return null
        return null;
    }
}
