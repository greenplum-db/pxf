package org.greenplum.pxf.plugins.jdbc.utils.oracle;

/**
 * Utilities class to build Oracle session query
 */
public class OracleJdbcUtils {
    private static final OracleSessionQueryFactory sessionQueryFactory = new OracleSessionQueryFactory();

    /**
     * Build a query to set session-level variables for Oracle database
     *
     * @param property variable name
     * @param value    variable value
     * @return a query to set session-level variables
     */
    public static String buildSessionQuery(String property, String value) {
        return sessionQueryFactory.create(property, value);
    }
}
