package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OracleParallelSessionParamFactoryTest {

    private final OracleParallelSessionParamFactory oracleParallelSessionParamFactory = new OracleParallelSessionParamFactory();
    private final String property = "jdbc.session.property.alter_session_parallel.1";
    private final String delimiter = "\\.";

    @Test
    void createWithClauseAndStatementAndDegreeOfParallelismSuccess() {
        String value = "force.query.5";
        OracleParallelSessionParam param = oracleParallelSessionParamFactory.create(property, value, delimiter);
        assertEquals(param.getClause(), OracleParallelSessionParam.Clause.FORCE);
        assertEquals(param.getStatementType(), OracleParallelSessionParam.StatementType.QUERY);
        assertEquals(param.getDegreeOfParallelism(), "5");
    }

    @Test
    void createWithClauseAndStatementSuccess() {
        String value = "enable.ddl";
        OracleParallelSessionParam param = oracleParallelSessionParamFactory.create(property, value, delimiter);
        assertEquals(param.getClause(), OracleParallelSessionParam.Clause.ENABLE);
        assertEquals(param.getStatementType(), OracleParallelSessionParam.StatementType.DDL);
        assertEquals(param.getDegreeOfParallelism(), "");
    }

    @Test
    void createWithClauseAndStatementAndBlankDegreeOfParallelismSuccess() {
        String value = "disable.dml.  ";
        String property = "jdbc.session.property.alter_session_parallel.1";
        OracleParallelSessionParam param = oracleParallelSessionParamFactory.create(property, value, delimiter);
        assertEquals(param.getClause(), OracleParallelSessionParam.Clause.DISABLE);
        assertEquals(param.getStatementType(), OracleParallelSessionParam.StatementType.DML);
        assertEquals(param.getDegreeOfParallelism(), "");
    }

    @Test
    void createWithEmptyValue() {
        String value = "enable.dml.";
        String property = "jdbc.session.property.alter_session_parallel.1";
        OracleParallelSessionParam param = oracleParallelSessionParamFactory.create(property, value, delimiter);
        assertEquals(param.getClause(), OracleParallelSessionParam.Clause.ENABLE);
        assertEquals(param.getStatementType(), OracleParallelSessionParam.StatementType.DML);
        assertEquals(param.getDegreeOfParallelism(), "");
    }

    @Test
    void createWithWrongClause() {
        String value = "fake_force.query.5";
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> oracleParallelSessionParamFactory.create(property, value, delimiter));
        String expectedMessage = "The 'clause' value 'FAKE_FORCE' in the parameter 'jdbc.session.property.alter_session_parallel.1' is not valid";
        String actualMessage = exception.getMessage();
        assertEquals(actualMessage, expectedMessage);
    }

    @Test
    void createWithWrongStatement() {
        String value = "enable.fake_statement";
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> oracleParallelSessionParamFactory.create(property, value, delimiter));
        String expectedMessage = "The 'statement type' value 'FAKE_STATEMENT' in the parameter 'jdbc.session.property.alter_session_parallel.1' is not valid";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void createWithWrongDegreeOfParallelism() {
        String value = "force.dml.fake_number";
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> oracleParallelSessionParamFactory.create(property, value, delimiter));
        String expectedMessage = "The 'degree of parallelism' value 'fake_number' in the parameter 'jdbc.session.property.alter_session_parallel.1' is not valid";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void createWithWrongValueMoreThen3() {
        String value = "force.dml.number.70";
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> oracleParallelSessionParamFactory.create(property, value, delimiter));
        String expectedMessage = "The parameter 'jdbc.session.property.alter_session_parallel.1' in jdbc-site.xml has to contain at least 2 but not more then 3 values delimited by \\.";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void createWithWrongValueLessThen2() {
        String value = "force";
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> oracleParallelSessionParamFactory.create(property, value, delimiter));
        String expectedMessage = "The parameter 'jdbc.session.property.alter_session_parallel.1' in jdbc-site.xml has to contain at least 2 but not more then 3 values delimited by \\.";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void createWithBlankValue() {
        String value = " ";
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> oracleParallelSessionParamFactory.create(property, value, delimiter));
        String expectedMessage = "The parameter 'jdbc.session.property.alter_session_parallel.1' is blank in jdbc-site.xml";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }
}
