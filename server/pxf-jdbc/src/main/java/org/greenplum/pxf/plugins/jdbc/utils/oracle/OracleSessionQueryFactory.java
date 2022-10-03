package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import org.apache.logging.log4j.util.Strings;

public class OracleSessionQueryFactory {
    private static final String ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_PREFIX = "alter_session_parallel";
    private static final String ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER = "\\.";
    private final OracleParallelSessionParamFactory oracleSessionParamFactory = new OracleParallelSessionParamFactory();

    public String create(String property, String value) {
        if (property.contains(ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_PREFIX)) {
            return getParallelSessionCommand(property, value);
        }
        return String.format("ALTER SESSION SET %s = %s", property, value);
    }

    private String getParallelSessionCommand(String property, String value) {
        OracleParallelSessionParam param = oracleSessionParamFactory.create(property,
                value, ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER);
        return createParallelSessionCommand(param);
    }

    private String createParallelSessionCommand(OracleParallelSessionParam param) {
        if (Strings.isNotEmpty(param.getDegreeOfParallelism())
                && param.getClause() == OracleParallelSessionParam.Clause.FORCE) {
            return String.format("ALTER SESSION %s PARALLEL %s PARALLEL %s",
                    param.getClause(), param.getStatementType(), param.getDegreeOfParallelism());
        } else {
            return String.format("ALTER SESSION %s PARALLEL %s", param.getClause(), param.getStatementType());
        }
    }
}
