package org.greenplum.pxf.plugins.jdbc.utils.oracle;

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

    public void setClause(Clause clause) {
        this.clause = clause;
    }

    public void setStatementType(StatementType statementType) {
        this.statementType = statementType;
    }

    public void setDegreeOfParallelism(String degreeOfParallelism) {
        this.degreeOfParallelism = degreeOfParallelism;
    }

    public Clause getClause() {
        return clause;
    }

    public StatementType getStatementType() {
        return statementType;
    }

    public String getDegreeOfParallelism() {
        return degreeOfParallelism;
    }
}
