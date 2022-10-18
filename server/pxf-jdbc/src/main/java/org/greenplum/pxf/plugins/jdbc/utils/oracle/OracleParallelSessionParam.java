package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import lombok.Data;

/**
 * Possible values for parallel session variables for Oracle database
 */
@Data
public class OracleParallelSessionParam {
    private Clause clause;
    private StatementType statementType;
    private String degreeOfParallelism;

    public enum Clause {
        ENABLE,
        DISABLE,
        FORCE
    }

    public enum StatementType {
        DML,
        DDL,
        QUERY
    }
}
