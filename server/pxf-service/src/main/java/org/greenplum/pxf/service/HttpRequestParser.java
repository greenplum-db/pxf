package org.greenplum.pxf.service;

import org.apache.commons.codec.CharEncoding;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.EnumAggregationType;
import org.greenplum.pxf.api.utilities.ProfilesConf;
import org.greenplum.pxf.api.utilities.ProtocolData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Parser for HTTP requests that contain data in HTTP headers.
 */
public class HttpRequestParser implements RequestParser<HttpHeaders> {

    private static final String TRUE_LCASE = "true";
    private static final String FALSE_LCASE = "false";
    private static final String PROP_PREFIX = "X-GP-";
    public static final int INVALID_SPLIT_IDX = -1;

    private final Logger LOG = LoggerFactory.getLogger(HttpRequestParser.class);

    //TODO pa
    protected OutputFormat outputFormat;
    protected int port;
    protected String host;
    protected String token;
    // statistics parameters
    protected int statsMaxFragments;
    protected float statsSampleRatio;

    @Override
    public RequestContext parseRequest(String path, HttpHeaders request) {

        Map<String, String> params = convertToCaseInsensitiveMap(request.getRequestHeaders());

        // TODO what if some data is sensitive (credentials) and should not be logged ?
        LOG.debug("Parsed request parameters: " + params);

        // build new instance of RequestContext and fill it with parsed values
        RequestContext result = new RequestContext();

        result.setAccessor();
        result.setAggType();
        result.setDataFragment();
        result.setDataSource();


        boolean isFilterPresent = getBoolProperty("HAS-FILTER");
        if (isFilterPresent) {
            result.setFilterString(getProperty("FILTER"));
        }
        result.setFilterStringValid(isFilterPresent);

        result.setFragmenter();
        result.setFragmentIndex();
        result.setFragmentMetadata();
        result.setFragmentUserData();
        result.setMetadata();
        result.setNumAttrsProjected();

        /*
         * accessor - will throw exception if outputFormat is
         * BINARY and the user did not supply accessor=... or profile=...
         * resolver - will throw exception if outputFormat is
         * BINARY and the user did not supply resolver=... or profile=...
         */
        String profile = getUserProperty("PROFILE");
        if (profile != null) {
            setProfilePlugins();
        }
        result.setProfile();

        result.setRecordkeyColumn();
        result.setRemoteLogin();
        result.setRemoteSecret();
        result.setResolver();
        result.setSegmentId(getIntProperty("SEGMENT-ID"));
        result.setServerName();
        result.setThreadSafe();
        result.setTotalSegments(getIntProperty("SEGMENT-COUNT"));
        result.setTupleDescription();
        result.setUser();
        result.setUserData();

        result.s

        /**
         * Constructs a ProtocolData.
         * Parses X-GP-* system configuration variables and
         * X-GP-OPTIONS-* user configuration variables
         * @param paramsMap contains all query-specific parameters from Gpdb
         */


        outputFormat = OutputFormat.valueOf(getProperty("FORMAT"));
        host = getProperty("URL-HOST");
        port = getIntProperty("URL-PORT");

        tupleDescription = new ArrayList<ColumnDescriptor>();
        recordkeyColumn = null;
        parseTupleDescription();


        accessor = getUserProperty("ACCESSOR");
        if (accessor == null) {
            protocolViolation("ACCESSOR");
        }
        resolver = getUserProperty("RESOLVER");
        if (resolver == null) {
            protocolViolation("RESOLVER");
        }

        fragmenter = getUserProperty("FRAGMENTER");
        metadata = getUserProperty("METADATA");

        dataSource = getProperty("DATA-DIR");

        user = getProperty("USER");

        parseFragmentMetadata();
        parseUserData();
        parseThreadSafe();
        parseRemoteCredentials();

        setServerName(getUserProperty("SERVER"));

        dataFragment = INVALID_SPLIT_IDX;
        parseDataFragment(getOptionalProperty("DATA-FRAGMENT"));

        statsMaxFragments = 0;
        statsSampleRatio = 0;
        parseStatsParameters();

        // Store alignment for global use as a system property
        System.setProperty("greenplum.alignment", getProperty("ALIGNMENT"));

        //Get aggregation operation
        String aggTypeOperationName = getOptionalProperty("AGG-TYPE");

        this.setAggType(EnumAggregationType.getAggregationType(aggTypeOperationName));

        //Get fragment index
        String fragmentIndexStr = getOptionalProperty("FRAGMENT-INDEX");

        if (fragmentIndexStr != null) {
            this.setFragmentIndex(Integer.parseInt(fragmentIndexStr));
        }
    }

    /**
     * Constructs a ProtocolData. Parses X-GP-* configuration variables.
     *
     * @param paramsMap     contains all query-specific parameters from Gpdb
     * @param profileString contains the profile name
     */
    public ProtocolData(Map<String, String> paramsMap, String profileString) {
        requestParametersMap = paramsMap;
        profile = profileString;
        setProfilePlugins();
        metadata = getUserProperty("METADATA");

        user = getProperty("USER");
    }

    /**
     * Sets the requested profile plugins from profile file into
     * {@link #requestParametersMap}.
     */
    private void setProfilePlugins() {
        Map<String, String> pluginsMap = ProfilesConf.getProfilePluginsMap(profile);
        checkForDuplicates(pluginsMap, requestParametersMap);
        requestParametersMap.putAll(pluginsMap);
    }

    /**
     * Verifies there are no duplicates between parameters declared in the table
     * definition and parameters defined in a profile.
     * <p>
     * The parameters' names are case insensitive.
     */
    private void checkForDuplicates(Map<String, String> plugins,
                                    Map<String, String> params) {
        List<String> duplicates = new ArrayList<>();
        for (String key : plugins.keySet()) {
            if (params.containsKey(key)) {
                duplicates.add(key);
            }
        }

        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException("Profile '" + profile
                    + "' already defines: "
                    + String.valueOf(duplicates).replace("X-GP-OPTIONS-", ""));
        }
    }

    /**
     * Returns the request parameters.
     *
     * @return map of request parameters
     */
    public Map<String, String> getParametersMap() {
        return requestParametersMap;
    }

    /**
     * Throws an exception when the given property value is missing in request.
     *
     * @param property missing property name
     * @throws IllegalArgumentException throws an exception with the property
     *                                  name in the error message
     */
    public void protocolViolation(String property) {
        String error = "Internal server error. Property \"" + property
                + "\" has no value in current request";

        LOG.error(error);
        throw new IllegalArgumentException(error);
    }

    /**
     * Returns the value to which the specified property is mapped in
     * {@link #requestParametersMap}.
     *
     * @param property the lookup property key
     * @throws IllegalArgumentException if property key is missing
     */
    private String getProperty(String property) {
        String result = requestParametersMap.get(PROP_PREFIX + property);

        if (result == null) {
            protocolViolation(property);
        }

        return result;
    }

    /**
     * Returns the optional property value. Unlike {@link #getProperty}, it will
     * not fail if the property is not found. It will just return null instead.
     *
     * @param property the lookup optional property
     * @return property value as a String
     */
    private String getOptionalProperty(String property) {
        return params.get(PROP_PREFIX + property);
    }

    /**
     * Returns a property value as an int type.
     *
     * @param property the lookup property
     * @return property value as an int type
     * @throws NumberFormatException if the value is missing or can't be
     *                               represented by an Integer
     */
    private int getIntProperty(String property) {
        return Integer.parseInt(getProperty(property));
    }

    /**
     * Returns a property value as boolean type. A boolean property is defined
     * as an int where 0 means false, and anything else true (like C).
     *
     * @param property the lookup property
     * @return property value as boolean
     * @throws NumberFormatException if the value is missing or can't be
     *                               represented by an Integer
     */
    private boolean getBoolProperty(String property) {
        return getIntProperty(property) != 0;
    }

    /**
     * Returns the current output format, either {@link OutputFormat#TEXT} or
     * {@link OutputFormat#GPDBWritable}.
     *
     * @return output format
     */
    public OutputFormat outputFormat() {
        return outputFormat;
    }

    /**
     * Returns the server name providing the service.
     *
     * @return server name
     */
    public String serverName() {
        return host;
    }

    /**
     * Returns the server port providing the service.
     *
     * @return server port
     */
    public int serverPort() {
        return port;
    }

    /**
     * Returns Kerberos token information.
     *
     * @return token
     */
    public String getToken() {
        return token;
    }

    /**
     * Statistics parameter. Returns the max number of fragments to return for
     * ANALYZE sampling. The value is set in GPDB side using the GUC
     * pxf_stats_max_fragments.
     *
     * @return max number of fragments to be processed by analyze
     */
    public int getStatsMaxFragments() {
        return statsMaxFragments;
    }

    /**
     * Statistics parameter. Returns a number between 0.0001 and 1.0,
     * representing the sampling ratio on each fragment for ANALYZE sampling.
     * The value is set in GPDB side based on ANALYZE computations and the
     * number of sampled fragments.
     *
     * @return sampling ratio
     */
    public float getStatsSampleRatio() {
        return statsSampleRatio;
    }

    /**
     * Returns a user defined property.
     *
     * @param userProp the lookup user property
     * @return property value as a String
     */
    private String getUserProperty(String userProp) {
        return requestParametersMap.get(USER_PROP_PREFIX + userProp.toUpperCase());
    }

    /**
     * Sets the thread safe parameter. Default value - true.
     */
    private void parseThreadSafe() {

        threadSafe = true;
        String threadSafeStr = getUserProperty("THREAD-SAFE");
        if (threadSafeStr != null) {
            threadSafe = parseBooleanValue(threadSafeStr);
        }
    }

    private boolean parseBooleanValue(String threadSafeStr) {

        if (threadSafeStr.equalsIgnoreCase(TRUE_LCASE)) {
            return true;
        }
        if (threadSafeStr.equalsIgnoreCase(FALSE_LCASE)) {
            return false;
        }
        throw new IllegalArgumentException("Illegal boolean value '"
                + threadSafeStr + "'." + " Usage: [TRUE|FALSE]");
    }

    /*
     * Sets the tuple description for the record
     * Attribute Projection information is optional
     */
    void parseTupleDescription() {

        /* Process column projection info */
        String columnProjStr = getOptionalProperty("ATTRS-PROJ");
        List<Integer> columnProjList = new ArrayList<Integer>();
        if (columnProjStr != null) {
            int columnProj = Integer.parseInt(columnProjStr);
            numAttrsProjected = columnProj;
            if (columnProj > 0) {
                String columnProjIndexStr = getProperty("ATTRS-PROJ-IDX");
                String columnProjIdx[] = columnProjIndexStr.split(",");
                for (int i = 0; i < columnProj; i++) {
                    columnProjList.add(Integer.valueOf(columnProjIdx[i]));
                }
            } else {
                /* This is a special case to handle aggregate queries not related to any specific column
                 * eg: count(*) queries. */
                columnProjList.add(0);
            }
        }

        int columns = getIntProperty("ATTRS");
        for (int i = 0; i < columns; ++i) {
            String columnName = getProperty("ATTR-NAME" + i);
            int columnTypeCode = getIntProperty("ATTR-TYPECODE" + i);
            String columnTypeName = getProperty("ATTR-TYPENAME" + i);
            Integer[] columnTypeMods = parseTypeMods(i);
            ColumnDescriptor column;
            if (columnProjStr != null) {
                column = new ColumnDescriptor(columnName, columnTypeCode, i, columnTypeName, columnTypeMods, columnProjList.contains(Integer.valueOf(i)));
            } else {
                /* For data formats that don't support column projection */
                column = new ColumnDescriptor(columnName, columnTypeCode, i, columnTypeName, columnTypeMods);
            }
            tupleDescription.add(column);

            if (columnName.equalsIgnoreCase(ColumnDescriptor.RECORD_KEY_NAME)) {
                recordkeyColumn = column;
            }
        }
    }

    private Integer[] parseTypeMods(int columnIndex) {
        String typeModeCountStr = getOptionalProperty("ATTR-TYPEMOD" + columnIndex + "-COUNT");
        Integer[] result = null;
        Integer typeModeCount = null;
        if (typeModeCountStr != null) {
            try {
                typeModeCount = Integer.parseInt(typeModeCountStr);
                if (typeModeCount < 0)
                    throw new IllegalArgumentException("ATTR-TYPEMOD" + columnIndex + "-COUNT cann't be negative");
                result = new Integer[typeModeCount];
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("ATTR-TYPEMOD" + columnIndex + "-COUNT must be a positive integer");
            }
            for (int i = 0; i < typeModeCount; i++) {
                try {
                    result[i] = Integer.parseInt(getProperty("ATTR-TYPEMOD" + columnIndex + "-" + i));
                    if (result[i] < 0)
                        throw new NumberFormatException();
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("ATTR-TYPEMOD" + columnIndex + "-" + i + " must be a positive integer");
                }
            }
        }
        return result;
    }

    /**
     * Sets the index of the allocated data fragment
     *
     * @param fragment the allocated data fragment
     */
    protected void parseDataFragment(String fragment) {

        /*
         * Some resources don't require a fragment, hence the list can be empty.
         */
        if (StringUtils.isEmpty(fragment)) {
            return;
        }
        dataFragment = Integer.parseInt(fragment);
    }

    private void parseFragmentMetadata() {
        fragmentMetadata = parseBase64("FRAGMENT-METADATA",
                "Fragment metadata information");
    }

    private void parseUserData() {
        userData = parseBase64("FRAGMENT-USER-DATA", "Fragment user data");
    }

    private byte[] parseBase64(String key, String errName) {
        String encoded = getOptionalProperty(key);
        if (encoded == null) {
            return null;
        }
        if (!Base64.isArrayByteBase64(encoded.getBytes())) {
            throw new IllegalArgumentException(errName
                    + " must be Base64 encoded." + "(Bad value: " + encoded
                    + ")");
        }
        byte[] parsed = Base64.decodeBase64(encoded);
        LOG.debug("decoded " + key + ": " + new String(parsed));
        return parsed;
    }

    private void parseRemoteCredentials() {
        remoteLogin = getOptionalProperty("REMOTE-USER");
        remoteSecret = getOptionalProperty("REMOTE-PASS");
    }

    private void parseStatsParameters() {

        String maxFrags = getUserProperty("STATS-MAX-FRAGMENTS");
        if (!StringUtils.isEmpty(maxFrags)) {
            statsMaxFragments = Integer.parseInt(maxFrags);
            if (statsMaxFragments <= 0) {
                throw new IllegalArgumentException("Wrong value '"
                        + statsMaxFragments + "'. "
                        + "STATS-MAX-FRAGMENTS must be a positive integer");
            }
        }

        String sampleRatioStr = getUserProperty("STATS-SAMPLE-RATIO");
        if (!StringUtils.isEmpty(sampleRatioStr)) {
            statsSampleRatio = Float.parseFloat(sampleRatioStr);
            if (statsSampleRatio < 0.0001 || statsSampleRatio > 1.0) {
                throw new IllegalArgumentException(
                        "Wrong value '"
                                + statsSampleRatio
                                + "'. "
                                + "STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
            }
        }

        if ((statsSampleRatio > 0) != (statsMaxFragments > 0)) {
            throw new IllegalArgumentException(
                    "Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together");
        }
    }

    /**
     * Converts the request headers multivalued map to a case-insensitive
     * regular map by taking only first values and storing them in a
     * CASE_INSENSITIVE_ORDER TreeMap. All values are converted from ISO_8859_1
     * (ISO-LATIN-1) to UTF_8.
     *
     * @param requestHeaders request headers multi map.
     * @return a regular case-insensitive map.
     */
    public Map<String, String> convertToCaseInsensitiveMap(MultivaluedMap<String, String> requestHeaders) {
        Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            if (values != null) {
                String value;
                if(values.size() > 1) {
                    value = StringUtils.join(values, ",");
                } else {
                    value = values.get(0);
                }
                if (value != null) {
                    // converting to value UTF-8 encoding
                    try {
                        value = new String(value.getBytes(CharEncoding.ISO_8859_1), CharEncoding.UTF_8);
                    } catch (UnsupportedEncodingException e) {
                        // should not happen as both ISO and UTF-8 encodings should be supported
                        throw new RuntimeException(e);
                    }
                    LOG.trace("key: %s value: %s", key, value);
                    result.put(key, value.replace("\\\"", "\""));
                }
            }
        }
        return result;
    }
}
}
