package org.greenplum.pxf.automation.structures.tables.pxf;

import org.apache.commons.lang.StringUtils;

public class ForeignTable extends ReadableExternalTable {

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
        String serverBase = StringUtils.defaultIfBlank(getServer(), "default");
        return String.format(" SERVER %s_%s", serverBase, getProtocol());
    }

    protected String createOptions() {
        // foreign tables do not have locations, parameters go into options
        // path (resource option for FDW) should always be present
        StringBuilder builder = new StringBuilder(" OPTIONS (");
        appendOption(builder,"resource", getPath(), true);

        String formatOption = getFormatOption();
        if (formatOption != null) {
            appendOption(builder, "format", formatOption);
        }

        // process copy options
        if (getDelimiter() != null) {
            // if Escape character, no need for "'"
            String parsedDelimiter = getDelimiter();
            /*
            if (!parsedDelimiter.startsWith("E")) {
                parsedDelimiter = "'" + parsedDelimiter + "'";
            }
            */
            appendOption(builder, "delimiter", parsedDelimiter);
        }

        if (getEscape() != null) {
            // if Escape character, no need for "'"
            String parsedEscapeCharacter = getEscape();
            /*
            if (!parsedEscapeCharacter.startsWith("E")) {
                parsedEscapeCharacter = "'" + parsedEscapeCharacter + "'";
            }
            */
            appendOption(builder, "escape", parsedEscapeCharacter);
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
        appendOption(builder, optionName, optionValue, false);
    }

    private void appendOption(StringBuilder builder, String optionName, String optionValue, boolean first) {
        if (!first) {
            builder.append(", ");
        }
        builder.append(optionName)
                .append(" '")
                .append(optionValue)
                .append("'");
    }

    private String getFormatOption() {
        // FDW format option is a second part of profile
        if (getProfile() == null) {
            // TODO: what will we do with tests that set F/A/R directly without a profile ?
            throw new IllegalStateException("Cannot create foreign table when profile is not specified");
        }
        String[] profileParts = getProfile().split(":");
        return (profileParts.length < 2) ? null : profileParts[1];
    }

    private String getProtocol() {
        // FDW protocol is a second part of profile
        if (getProfile() == null) {
            // TODO: what will we do with tests that set F/A/R directly without a profile ?
            throw new IllegalStateException("Cannot create foreign table when profile is not specified");
        }
        return getProfile().split(":")[0];
    }

}
