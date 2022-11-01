package org.greenplum.pxf.service;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.PluginConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.CharsetUtils;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.EnumAggregationType;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Parser for HTTP requests that contain data in HTTP headers.
 */
@Component
public class HttpRequestParser implements RequestParser<MultiValueMap<String, String>> {

    private static final String ERROR_MESSAGE_HINT =
            "upgrade PXF extension (run 'pxf [cluster] register' and then 'ALTER EXTENSION pxf UPDATE')";

    private static final String ERROR_MESSAGE_TEMPLATE = "API version mismatch; server implements v%s and client implements v%s";

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestParser.class);

    private static final String TRUE_LCASE = "true";
    private static final String PROFILE_SCHEME = "PROFILE-SCHEME";
    private static final String PXF_API_VERSION = "pxfApiVersion";

    private final CharsetUtils charsetUtils;
    private final PluginConf pluginConf;
    private final HttpHeaderDecoder headerDecoder;
    private final BuildProperties buildProperties;
    private final PxfApiVersionChecker apiVersionChecker;

    /**
     * Create a new instance of the HttpRequestParser with the given PluginConf
     *
     * @param pluginConf    the plugin conf
     * @param charsetUtils  utilities for Charset
     * @param headerDecoder decoder of http headers
     * @param buildProperties   Spring Boot build info
     * @param apiVersionChecker component for comparing versions
     */
    public HttpRequestParser(PluginConf pluginConf, CharsetUtils charsetUtils, HttpHeaderDecoder headerDecoder, BuildProperties buildProperties, PxfApiVersionChecker apiVersionChecker) {
        this.pluginConf = pluginConf;
        this.charsetUtils = charsetUtils;
        this.headerDecoder = headerDecoder;
        this.buildProperties = buildProperties;
        this.apiVersionChecker = apiVersionChecker;
    }

    /**
     * Throws an exception when the given property value is missing in request.
     *
     * @param property missing property name
     * @throws IllegalArgumentException throws an exception with the property
     *                                  name in the error message
     */
    private static void protocolViolation(String property) {
        String error = String.format("Property %s has no value in the current request", property);
        throw new IllegalArgumentException(error);
    }

    @Override
    public RequestContext parseRequest(MultiValueMap<String, String> requestHeaders, RequestContext.RequestType requestType) {

        RequestMap params = new RequestMap(requestHeaders, headerDecoder);

        if (LOG.isDebugEnabled()) {
            // Logging only keys to prevent sensitive data to be logged
            LOG.debug("Parsing request parameters: " + params.keySet());
        }

        RequestContext context = new RequestContext();

        // fill the Request-scoped RequestContext with parsed values

        context.setClientApiVersion(
                params.removeProperty("PXF-API-VERSION", ERROR_MESSAGE_HINT));

        if (!apiVersionChecker.isCompatible(getServerApiVersion(), context.getClientApiVersion())) {
            throw new PxfRuntimeException(
                    String.format(ERROR_MESSAGE_TEMPLATE, getServerApiVersion(), context.getClientApiVersion()), ERROR_MESSAGE_HINT);
        }

        // whether we are in a fragmenter, read_bridge, or write_bridge scenario
        context.setRequestType(requestType);

        // first of all, set profile and enrich parameters with information from specified profile
        String profileUserValue = params.removeUserProperty("PROFILE");
        String profile = profileUserValue == null ? null : profileUserValue.toLowerCase();
        context.setProfile(profile);
        addProfilePlugins(profile, params);

        // Ext table uses system property FORMAT for wire serialization format
        String wireFormat = params.removeProperty("FORMAT");
        context.setOutputFormat(OutputFormat.valueOf(wireFormat));

        // FDW uses user property FORMAT to indicate format of data
        String format = params.removeUserProperty("FORMAT");
        format = StringUtils.isNotBlank(format) ? format : context.inferFormatName();
        context.setFormat(format);

        context.setAccessor(params.removeUserProperty("ACCESSOR"));
        context.setAggType(EnumAggregationType.getAggregationType(params.removeOptionalProperty("AGG-TYPE")));

        context.setDataSource(params.removeProperty("DATA-DIR"));

        String filterString = params.removeOptionalProperty("FILTER");
        String hasFilter = params.removeProperty("HAS-FILTER");
        if (filterString != null) {
            context.setFilterString(filterString);
        } else if ("1".equals(hasFilter)) {
            LOG.info("Original query has filter, but it was not propagated to PXF");
        }

        context.setDataEncoding(charsetUtils.forName(params.removeProperty("DATA-ENCODING")));
        context.setDatabaseEncoding(charsetUtils.forName(params.removeProperty("DATABASE-ENCODING")));

        context.setFragmenter(params.removeUserProperty("FRAGMENTER"));

        context.setHost(params.removeProperty("URL-HOST"));
        context.setMetadata(params.removeUserProperty("METADATA"));
        context.setPort(params.removeIntProperty("URL-PORT"));
        context.setProfileScheme(params.removeUserProperty(PROFILE_SCHEME));
        context.setProtocol(params.removeUserProperty("PROTOCOL"));
        context.setRemoteLogin(params.removeOptionalProperty("REMOTE-USER"));
        context.setRemoteSecret(params.removeOptionalProperty("REMOTE-PASS"));
        context.setResolver(params.removeUserProperty("RESOLVER"));
        context.setSegmentId(params.removeIntProperty("SEGMENT-ID"));
        context.setServerName(params.removeUserProperty("SERVER"));
        context.setSchemaName(params.removeProperty("SCHEMA-NAME"));
        context.setTableName(params.removeProperty("TABLE-NAME"));

        // An optional CONFIG value specifies the name of the server
        // configuration directory, if not provided the config is the server name
        String config = params.removeUserProperty("CONFIG");
        context.setConfig(StringUtils.isNotBlank(config) ? config : context.getServerName());

        String maxFrags = params.removeUserProperty("STATS-MAX-FRAGMENTS");
        if (!StringUtils.isBlank(maxFrags)) {
            context.setStatsMaxFragments(Integer.parseInt(maxFrags));
        }

        String sampleRatioStr = params.removeUserProperty("STATS-SAMPLE-RATIO");
        if (!StringUtils.isBlank(sampleRatioStr)) {
            context.setStatsSampleRatio(Float.parseFloat(sampleRatioStr));
        }

        context.setTotalSegments(params.removeIntProperty("SEGMENT-COUNT"));
        context.setTransactionId(params.removeProperty("XID"));

        // parse tuple description
        parseTupleDescription(params, context);

        if (context.getOutputFormat() == OutputFormat.TEXT) {
            // parse CSV format information
            parseGreenplumCSV(params, context);

            // Only single column tables support 'OFF' delimiter
            if (context.getTupleDescription().size() != 1 && context.getGreenplumCSV().getDelimiter() == null) {
                throw new IllegalArgumentException(String.format("using no delimiter is only possible for a single column table. %d columns found", context.getTupleDescription().size()));
            }
        }

        context.setGpSessionId(params.removeIntProperty("SESSION-ID"));
        context.setGpCommandCount(params.removeIntProperty("COMMAND-COUNT"));
        context.setUser(params.removeProperty("USER"));

        // Store alignment for global use as a system property
        System.setProperty("greenplum.alignment", params.removeProperty("ALIGNMENT"));

        Map<String, String> optionMappings = null;

        // prepare addition configuration properties if profile was specified
        if (StringUtils.isNotBlank(profile)) {
            optionMappings = pluginConf.getOptionMappings(profile);
        }

        // use empty map for convenience instead of null
        if (optionMappings == null) {
            optionMappings = Collections.emptyMap();
        }

        Map<String, String> additionalConfigProps = new HashMap<>();

        // Iterate over the remaining properties
        // we clone the keyset to prevent concurrent modification exceptions
        List<String> paramNames = new ArrayList<>(params.keySet());
        for (String param : paramNames) {
            if (StringUtils.startsWithIgnoreCase(param, RequestMap.USER_PROP_PREFIX)) {
                // Add all left-over user properties as options
                String optionName = param.toLowerCase().replace(RequestMap.USER_PROP_PREFIX_LOWERCASE, "");
                String optionValue = params.removeUserProperty(optionName);
                context.addOption(optionName, optionValue);
                LOG.debug("Added option {} to request context", optionName);

                // lookup if the option should also be applied as a config property
                String propertyName = optionMappings.get(optionName);
                if (StringUtils.isNotBlank(propertyName)) {
                    // if option has been provided by the user in the request, set the value
                    // of the corresponding configuration property
                    if (optionValue != null) {
                        additionalConfigProps.put(propertyName, optionValue);
                        // do not log property value as it might contain sensitive information
                        LOG.debug("Added extra config property {} from option {}", propertyName, optionName);
                    }
                }
            } else if (StringUtils.startsWithIgnoreCase(param, RequestMap.PROP_PREFIX)) {
                // log debug for all left-over system properties
                LOG.debug("Unused property {}", param);
            }
        }

        context.setAdditionalConfigProps(additionalConfigProps);
        context.setPluginConf(pluginConf);

        // Call the protocol handler for any protocol-specific logic handling
        if (StringUtils.isNotBlank(profile)) {
            String handlerClassName = pluginConf.getHandler(profile);
            if (handlerClassName != null) {
                Utilities.updatePlugins(context, handlerClassName);
            }
        }

        // validate that the result has all required fields, and values are in valid ranges
        context.validate();

        return context;
    }

    String getServerApiVersion() {
        return buildProperties.get(PXF_API_VERSION);
    }

    private void parseGreenplumCSV(RequestMap params, RequestContext context) {
        context.getGreenplumCSV()
                .withDelimiter(params.removeUserProperty("DELIMITER"))
                .withEscapeChar(params.removeUserProperty("ESCAPE"))
                .withNewline(params.removeUserProperty("NEWLINE"))
                .withQuoteChar(params.removeUserProperty("QUOTE"))
                .withValueOfNull(params.removeUserProperty("NULL"));
    }

    /**
     * Sets the requested profile plugins from the profile file into the request map.
     *
     * @param profile profile specified in the user request
     * @param params  parameters provided in the user request
     */
    private void addProfilePlugins(String profile, RequestMap params) {
        // if profile was not specified in the request, do nothing
        if (StringUtils.isBlank(profile)) {
            LOG.debug("Profile was not specified in the request");
            return;
        }

        LOG.debug("Adding plugins for profile {}", profile);

        // get Profile's plugins from the configuration file
        Map<String, String> pluginsMap = pluginConf.getPlugins(profile);
        if (pluginsMap == null) {
            // a dynamic profile will have no plugins predefined
            return;
        }

        // create sets of keys to find out duplicates between what user has specified in the request
        // and what is configured in the configuration file -- DO NOT ALLOW DUPLICATES
        List<String> duplicates = pluginsMap.keySet().stream()
                .filter(n -> params.containsKey(RequestMap.USER_PROP_PREFIX + n))
                .collect(Collectors.toList());

        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(String.format("Profile '%s' already defines: %s",
                    profile, duplicates));
        }

        // since there are guaranteed to be no duplications at this point,
        // add properties defined by profiles to the request map as if they were specified by the user
        pluginsMap.forEach((k, v) -> params.put(RequestMap.USER_PROP_PREFIX + k, v));

        String profileProtocol = pluginConf.getProtocol(profile);
        if (profileProtocol != null) {
            params.put(RequestMap.USER_PROP_PREFIX + PROFILE_SCHEME, profileProtocol);
        }
    }

    /*
     * Sets the tuple description for the record
     * Attribute Projection information is optional
     */
    private void parseTupleDescription(RequestMap params, RequestContext context) {
        int columns = params.removeIntProperty("ATTRS");
        BitSet attrsProjected = new BitSet(columns + 1);

        /* Process column projection info */
        String columnProjStr = params.removeOptionalProperty("ATTRS-PROJ");
        if (columnProjStr != null) {
            int numberOfProjectedColumns = Integer.parseInt(columnProjStr);
            context.setNumAttrsProjected(numberOfProjectedColumns);
            if (numberOfProjectedColumns > 0) {
                String[] projectionIndices = params.removeProperty("ATTRS-PROJ-IDX").split(",");
                for (String s : projectionIndices) {
                    attrsProjected.set(Integer.parseInt(s));
                }
            } else {
                /* This is a special case to handle aggregate queries not related to any specific column
                 * eg: count(*) queries. */
                attrsProjected.set(0);
            }
        }


        for (int attrNumber = 0; attrNumber < columns; attrNumber++) {
            String columnName = params.removeProperty("ATTR-NAME" + attrNumber);
            int columnOID = params.removeIntProperty("ATTR-TYPECODE" + attrNumber);
            String columnTypeName = params.removeProperty("ATTR-TYPENAME" + attrNumber);
            Integer[] columnTypeMods = parseTypeMods(params, attrNumber);
            // Project the column if columnProjStr is null
            boolean isProjected = columnProjStr == null || attrsProjected.get(attrNumber);
            ColumnDescriptor column = new ColumnDescriptor(
                    columnName,
                    columnOID,
                    attrNumber,
                    columnTypeName,
                    columnTypeMods,
                    isProjected);
            context.getTupleDescription().add(column);

            if (columnName.equalsIgnoreCase(ColumnDescriptor.RECORD_KEY_NAME)) {
                context.setRecordkeyColumn(column);
            }
        }
    }

    private Integer[] parseTypeMods(RequestMap params, int columnIndex) {
        String typeModCountPropName = "ATTR-TYPEMOD" + columnIndex + "-COUNT";
        String typeModeCountStr = params.removeOptionalProperty(typeModCountPropName);
        if (typeModeCountStr == null)
            return null;

        int typeModeCount = parsePositiveIntOrError(typeModeCountStr, typeModCountPropName);

        Integer[] result = new Integer[typeModeCount];
        for (int i = 0; i < typeModeCount; i++) {
            String typeModItemPropName = "ATTR-TYPEMOD" + columnIndex + "-" + i;
            result[i] = parsePositiveIntOrError(params.removeProperty(typeModItemPropName), typeModItemPropName);
        }
        return result;
    }

    private int parsePositiveIntOrError(String s, String propName) {
        int n;
        try {
            n = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("%s must be an integer", propName), e);
        }
        if (n < 0) {
            throw new IllegalArgumentException(String.format("%s must be a positive integer", propName));
        }
        return n;
    }

    /**
     * Converts the request headers multivalued map to a case-insensitive
     * regular map by taking only first values and storing them in a
     * CASE_INSENSITIVE_ORDER TreeMap. All values are converted from ISO_8859_1
     * (ISO-LATIN-1) to UTF_8.
     */
    static class RequestMap extends TreeMap<String, String> {
        private static final long serialVersionUID = 4745394510220213936L;
        private static final String PROP_PREFIX = "X-GP-";
        private static final String USER_PROP_PREFIX = "X-GP-OPTIONS-";
        private static final String USER_PROP_PREFIX_LOWERCASE = "x-gp-options-";


        RequestMap(MultiValueMap<String, String> requestHeaders, HttpHeaderDecoder headerDecoder) {
            super(String.CASE_INSENSITIVE_ORDER);

            boolean headersEncoded = headerDecoder.areHeadersEncoded(requestHeaders);
            for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
                String key = entry.getKey();
                String value = headerDecoder.getHeaderValue(key, entry.getValue(), headersEncoded);
                if (value != null) {
                    LOG.trace("Key: {} Value: {}", key, value);
                    put(key, value);
                }
            }
        }

        /**
         * Returns a user defined property.
         *
         * @param userProp the lookup user property
         * @return property value as a String
         */
        private String removeUserProperty(String userProp) {
            return remove(USER_PROP_PREFIX + userProp);
        }

        /**
         * Returns the optional property value. Unlike {@link #removeProperty}, it will
         * not fail if the property is not found. It will just return null instead.
         *
         * @param property the lookup optional property
         * @return property value as a String
         */
        private String removeOptionalProperty(String property) {
            return remove(PROP_PREFIX + property);
        }

        /**
         * Returns a property value as an int type and removes it from the map
         *
         * @param property the lookup property
         * @return property value as an int type
         * @throws NumberFormatException if the value is missing or can't be
         *                               represented by an Integer
         */
        private int removeIntProperty(String property) {
            return Integer.parseInt(removeProperty(property));
        }

        /**
         * Returns the value to which the specified property is mapped and
         * removes it from the map
         *
         * @param property the lookup property key
         * @throws IllegalArgumentException if property key is missing
         */
        private String removeProperty(String property) {
            String result = remove(PROP_PREFIX + property);

            if (result == null) {
                protocolViolation(property);
            }

            return result;
        }

        /**
         * Returns the value to which the specified property is mapped and
         * removes it from the map
         *
         * @param property the lookup property key
         * @param errMsgHint hint for user to include in error message
         * @throws PxfRuntimeException if property key is missing
         */
        private String removeProperty(String property, String errMsgHint) {
            try {
                return removeProperty(property);
            } catch (IllegalArgumentException e) {
                throw new PxfRuntimeException(e.getMessage(), errMsgHint, e);
            }
        }

        /**
         * Returns an optional property value as boolean type. If the property
         * is missing, the default false is returned.
         *
         * @param property the lookup property key
         * @return true when the property is true, false otherwise
         */
        private boolean removeOptionalBoolProperty(String property) {
            return StringUtils.equals(TRUE_LCASE, removeOptionalProperty(property));
        }
    }

}
