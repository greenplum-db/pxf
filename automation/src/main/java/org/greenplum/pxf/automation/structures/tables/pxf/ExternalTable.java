package org.greenplum.pxf.automation.structures.tables.pxf;

import java.util.Arrays;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;

/**
 * Represent GPDB -> PXF external table.
 */
public abstract class ExternalTable extends Table {

    private String host = "127.0.0.1";

    private String port = "5888";

    private String path = "somepath/gpdb_regression_data";

    private String fragmenter;

    private String accessor;

    private String resolver;

    private String dataSchema;

    private String format;

    private String formatter;

    private String delimiter;

    private String[] userParameters;

    private String profile;

    private String errorTable;

    private int segmentRejectLimit = 0;

    private String segmentRejectLimitType = "ROWS";

    private String encoding;

    public ExternalTable(String name, String[] fields, String path,
                         String format) {
        super(name, fields);
        this.path = path;
        this.format = format;
        if(ProtocolUtils.getProtocol() != ProtocolEnum.HDFS) {
            this.setUserParameters(new String[]{"server=" + ProtocolUtils.getProtocol().value()});
        }
    }

    @Override
    public String constructDropStmt(boolean cascade) {

        StringBuilder sb = new StringBuilder();

        sb.append("DROP EXTERNAL TABLE IF EXISTS " + getFullName());
        if (cascade) {
            sb.append(" CASCADE");
        }

        return sb.toString();
    }

    @Override
    protected String createHeader() {
        return "CREATE EXTERNAL TABLE " + getFullName();
    }

    @Override
    protected String createLocation() {

        StringBuilder sb = new StringBuilder();

        sb.append(" LOCATION (E'");
        sb.append(getLocation());
        sb.append("')");

        return sb.toString();
    }

    public String getLocation() {

        StringBuilder sb = new StringBuilder("pxf://");
        // GPDB mode does not use host:port in location URL

        sb.append(getPath());
        sb.append("?");
        sb.append(getLocationParameters());

        return sb.toString();
    }

    /**
     * Generates location for create query
     *
     * @return location parameters
     */
    protected String getLocationParameters() {

        StringBuilder sb = new StringBuilder();

        if (getProfile() != null) {
            appendParamter(sb, "PROFILE=" + getProfile());
        }

        if (getFragmenter() != null) {
            appendParamter(sb, "FRAGMENTER=" + getFragmenter());
        }

        if (getAccessor() != null) {
            appendParamter(sb, "ACCESSOR=" + getAccessor());
        }

        if (getResolver() != null) {
            appendParamter(sb, "RESOLVER=" + getResolver());
        }

        if (getDataSchema() != null) {
            appendParamter(sb, "DATA-SCHEMA=" + getDataSchema());
        }

        if (getUserParameters() != null) {

            for (int i = 0; i < getUserParameters().length; i++) {
                appendParamter(sb, getUserParameters()[i]);
            }
        }

        return sb.toString();
    }

    /**
     * Appends location parameters to {@link StringBuilder}, append '&' between
     * parameters
     *
     * @param sBuilder {@link StringBuilder} to collect parameters
     * @param parameter to add to {@link StringBuilder}
     */
    protected void appendParamter(StringBuilder sBuilder, String parameter) {

        // if not the first parameter, add '&'
        if (!sBuilder.toString().equals("")) {
            sBuilder.append("&");
        }

        sBuilder.append(parameter);
    }

    @Override
    public String constructCreateStmt() {
        String createStatment = "";

        createStatment += createHeader();
        createStatment += createFields();
        createStatment += createLocation();

        if (getFormat() != null) {
            createStatment += " FORMAT '" + getFormat() + "'";

        }

        if (getFormatter() != null) {
            createStatment += " (formatter='" + getFormatter() + "')";
        }

        if (getDelimiter() != null) {

            // if Escape character, no need for "'"
            String parsedDelimiter = getDelimiter();
            if (!parsedDelimiter.startsWith("E")) {
                parsedDelimiter = "'" + parsedDelimiter + "'";
            }
            createStatment += " (DELIMITER " + parsedDelimiter + ")";
        }

        if (getEncoding() != null) {
            createStatment += " ENCODING '" + getEncoding() + "'";
        }

        if (getErrorTable() != null) {
            createStatment += " LOG ERRORS";
        }

        if (getSegmentRejectLimit() > 0) {
            createStatment += " SEGMENT REJECT LIMIT "
                    + getSegmentRejectLimit() + " "
                    + getSegmentRejectLimitType();
        }

        return createStatment;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public String getErrorTable() {
        return errorTable;
    }

    public void setErrorTable(String errorTable) {
        this.errorTable = errorTable;
    }

    public int getSegmentRejectLimit() {
        return segmentRejectLimit;
    }

    public void setSegmentRejectLimit(int segmentRejectLimit) {
        this.segmentRejectLimit = segmentRejectLimit;
    }

    public String getSegmentRejectLimitType() {
        return segmentRejectLimitType;
    }

    public void setSegmentRejectLimitType(String segmentRejectLimitType) {
        this.segmentRejectLimitType = segmentRejectLimitType;
    }

    public void setSegmentRejectLimitAndType(int segmentRejectLimit,
                                             String segmentRejectLimitType) {
        this.segmentRejectLimit = segmentRejectLimit;
        this.segmentRejectLimitType = segmentRejectLimitType;
    }

    public String getFragmenter() {
        return fragmenter;
    }

    public void setFragmenter(String fragmenter) {
        this.fragmenter = fragmenter;
    }

    public String getAccessor() {
        return accessor;
    }

    public void setAccessor(String accessor) {
        this.accessor = accessor;
    }

    public String getResolver() {
        return resolver;
    }

    public void setResolver(String resolver) {
        this.resolver = resolver;
    }

    public String getDataSchema() {
        return dataSchema;
    }

    public void setDataSchema(String dataSchema) {
        this.dataSchema = dataSchema;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getFormatter() {
        return formatter;
    }

    public void setFormatter(String formatter) {
        this.formatter = formatter;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    /**
     * Array of user parameters, each param in the format "KEY=VALUE"
     *
     * @param userParameters
     */
    public void setUserParameters(String[] userParameters) {

        this.userParameters = null;

        if (userParameters != null) {
            this.userParameters = Arrays.copyOf(userParameters,
                    userParameters.length);
        }
    }

    public String[] getUserParameters() {
        return userParameters;
    }
}
