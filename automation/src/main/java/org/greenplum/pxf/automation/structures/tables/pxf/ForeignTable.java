package org.greenplum.pxf.automation.structures.tables.pxf;

import org.apache.commons.lang.StringUtils;

public class ForeignTable extends WritableExternalTable {

    public ForeignTable(String name, String[] fields, String path, String format) {
        super(name, fields, path, format);
    }

    @Override
    public String constructCreateStmt() {
        StringBuilder builder = new StringBuilder()
                .append(createHeader())
                .append(createFields())
                .append(createServer())
                .append(createOptions());
        return builder.toString();
    }

    @Override
    public String constructDropStmt(boolean cascade) {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP FOREIGN TABLE IF EXISTS " + getFullName());
        if (cascade) {
            sb.append(" CASCADE");
        }
        return sb.toString();
    }

    @Override
    protected String createHeader() {
        return "CREATE FOREIGN TABLE " + getFullName();
    }

    protected String createServer() {
        String[] serverParameters = StringUtils.defaultIfBlank(getServer(), "default").split("=");
        // getServer() might return a string "server=<..>", strip the prefix
        int index = serverParameters.length > 1 ? 1 : 0;
        return String.format(" SERVER %s_%s", serverParameters[index], getProtocol());
    }

    protected String createOptions() {
        // foreign tables do not have locations, parameters go into options
        // path (resource option for FDW) should always be present
        StringBuilder builder = new StringBuilder(" OPTIONS (");
        appendOption(builder,"resource", getPath(), true, true);

        String formatOption = getFormatOption();
        if (formatOption != null) {
            appendOption(builder, "format", formatOption);
        }

        // process F/A/R as options, they are used in tests to test column projection / predicate pushdown
        if (getFragmenter() != null) {
            appendOption(builder, "fragmenter", getFragmenter());
        }
        if (getAccessor() != null) {
            appendOption(builder, "accessor", getAccessor());
        }
        if (getResolver() != null) {
            appendOption(builder, "resolver", getResolver());
        }

        // process copy options
        if (getDelimiter() != null) {
            // if Escape character, no need for "'"
            appendOption(builder,"delimiter", getDelimiter(), false, !getDelimiter().startsWith("E"));
        }

        if (getEscape() != null) {
            // if Escape character, no need for "'"
            appendOption(builder,"delimiter", getEscape(), false, !getEscape().startsWith("E"));
        }

        if (getNewLine() != null) {
            appendOption(builder, "newline", getNewLine());
        }

        // TODO: encoding might only be properly supported in refactor branch
        // https://github.com/greenplum-db/pxf/commit/6be3ca67e1a2748205fcaf9ac96e124925593e11#diff-495fb626f562922c4333130eb7334b9766a18f1968e577549bff0384890e0d05
        if (getEncoding() != null) {
            appendOption(builder, "encoding", getEncoding());
        }

        // TODO: are these really "copy options" ? copy.c does not have to seem to have them
        /*
        if (getErrorTable() != null) {
            appendOption(builder, "log errors", getEncoding());
            createStatment += " LOG ERRORS";
        }

        if (getSegmentRejectLimit() > 0) {
            createStatment += " SEGMENT REJECT LIMIT "
                    + getSegmentRejectLimit() + " "
                    + getSegmentRejectLimitType();
        }
        */

        // process user options, some might actually belong to Foreign Server, but eventually they all will be
        // combined in a single set, so the net result is the same, other than testing option precedence rules

        if (getDataSchema() != null) {
            appendOption(builder, "DATA-SCHEMA", getDataSchema());
        }

        if (getExternalDataSchema() != null) {
            appendOption(builder, "SCHEMA", getExternalDataSchema());
        }

        String[] params = getUserParameters();
        if (params != null) {
            for (String param : params) {
                // parse parameter, each one is KEY=VALUE
                String[] paramPair = param.split("=");
                appendOption(builder, paramPair[0], paramPair[1]);
            }
        }

        builder.append(")");
        return builder.toString();
    }

    private void appendOption(StringBuilder builder, String optionName, String optionValue) {
        appendOption(builder, optionName, optionValue, false, true);
    }

    private void appendQuotedOption(StringBuilder builder, String optionName, String optionValue) {
        appendOption(builder, optionName, optionValue, false, false);
    }

    private void appendOption(StringBuilder builder, String optionName, String optionValue, boolean first, boolean needQuoting) {
        if (!first) {
            builder.append(", ");
        }
        builder.append(optionName)
               .append(" ")
               .append(needQuoting ? "'" : "")
               .append(optionValue)
               .append(needQuoting ? "'" : "");
    }

    private String getFormatOption() {
        // FDW format option is a second part of profile
        String[] profileParts = getProfileParts();
        String format;
        if (profileParts.length == 1) {
            format = profileParts[0].toLowerCase();
            if (format.equals("hive") || format.equals("hbase") || format.equals("jdbc") ) {
                return null;
            } else if (format.startsWith("hdfs") || format.startsWith("hive")) {
                format = format.substring(4);
            } else {
                // special case of old 1 word profiles that are basically formats (Parquet, Json, etc)
                if (format.equals("textsimple")) {
                    format = "text";
                } else if (format.equals("textmulti")) {
                    format = "text:multi";
                } else if (format.equals("hivevectorizedorc")) {
                    //TODO: vectorized becomes a separate option, how to handle this ?
                    format = "orc";
                } else if (format.equals("sequencewritable")) {
                    format = "sequencefile";
                }
                // just leave it as parsed for json / avro / parquet that are left
            }
        } else {
            format = profileParts[1];
            if (profileParts.length == 3) {
                // edge case for kinds of hdfs:text:multi
                format += ":" + profileParts[2];
            }
        }
        return format;
    }

    private String getProtocol() {
        // FDW protocol is a second part of profile
        String[] profileParts = getProfileParts();
        String protocol = profileParts[0].toLowerCase();

        // special case: one-word old-style compound profiles of HiveOrc and similar Hive* or Hdfs*
        if (profileParts.length == 1) {
            if (protocol.startsWith("hdfs")) {
                protocol = "hdfs";
            } else if (protocol.startsWith("hive")) {
                protocol = "hive";
            } else if (protocol.equals("hbase")) {
                protocol = "hbase";
            } else if (protocol.equals("jdbc")) {
                protocol = "jdbc";
            } else {
                // single word profiles that are left are basically for format (Parquet or Json)
                // so assume they will work against hdfs, at least for now
                protocol = "hdfs";
            }
        }
        return protocol;
    }

    private String[] getProfileParts() {
        if (getProfile() == null) {
            // TODO: what will we do with tests that set F/A/R directly without a profile ?
            throw new IllegalStateException("Cannot create foreign table when profile is not specified");
        }
        return getProfile().split(":");
    }
}
