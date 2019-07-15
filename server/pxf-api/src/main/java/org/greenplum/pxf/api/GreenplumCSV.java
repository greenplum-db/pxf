package org.greenplum.pxf.api;

/**
 * Greenplum CSV Default
 */
public class GreenplumCSV {


    // TODO: FDW: We want to read the values for delimiter, newline, value of null,
    // TODO: FDW: etc from the request and serialize the CSV using those values

    /*
     * Greenplum CSV Defaults
     */
    public static final char QUOTE = '"';
    public static final char ESCAPE = '"';
    public static final char DELIMITER = ',';
    public static final String NEWLINE = "\n";
    public static final String VALUE_OF_NULL = "";
}
