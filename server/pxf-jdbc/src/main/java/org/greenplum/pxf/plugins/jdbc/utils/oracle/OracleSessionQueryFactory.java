package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import org.apache.commons.lang.StringUtils;

/**
 * Factory class to build Oracle session statement.
 */
public class OracleSessionQueryFactory {
    private static final String ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_PREFIX = "alter_session_parallel";
    private static final String ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER = "\\.";
    private static final OracleParallelSessionParamFactory oracleSessionParamFactory = new OracleParallelSessionParamFactory();

    /**
     * Build a query to set common session-level variables or parallel session variables for Oracle database
     *
     * @param property variable name
     * @param value    variable value
     * @return a query to set session-level variables
     */
    public String create(String property, String value) {
        if (property.contains(ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_PREFIX)) {
            return getParallelSessionCommand(property, value);
        }
        return String.format("ALTER SESSION SET %s = %s", property, value);
    }

    /**
     * Build a query to set parallel session variables for Oracle database
     *
     * @param property variable name
     * @param value    variable value
     * @return a string with ALTER SESSION { ... } PARALLEL { ... } [ PARALLEL integer ] statement
     */
    private String getParallelSessionCommand(String property, String value) {
        OracleParallelSessionParam param = oracleSessionParamFactory.create(property,
                value, ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER);
        return createParallelSessionCommand(param);
    }

    /**
     * Build a query to set parallel session variables with provided parallel session parameters.
     *
     * @param param {@link OracleParallelSessionParam}
     * @return a string with ALTER SESSION { ... } PARALLEL { ... } [ PARALLEL integer ] statement
     */
    private String createParallelSessionCommand(OracleParallelSessionParam param) {
        return String.format("ALTER SESSION %s PARALLEL %s%s%s",
                param.getClause(),
                param.getStatementType(),
                StringUtils.isBlank(param.getDegreeOfParallelism()) ? "" : " PARALLEL ",
                param.getDegreeOfParallelism());
    }
}
