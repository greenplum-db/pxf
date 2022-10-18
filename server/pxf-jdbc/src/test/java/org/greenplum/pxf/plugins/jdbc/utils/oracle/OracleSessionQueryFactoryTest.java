package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OracleSessionQueryFactoryTest {
    private static MockedConstruction<OracleParallelSessionParamFactory> mockedConstruction;
    private static final String property = "jdbc.session.property.alter_session_parallel.1";
    private static final String delimiter = "\\.";
    private static final String value1 = "force.query.4";
    private static final String value2 = "force.dml";
    private static final String value3 = "enable.dml.2";
    private static final String value4 = "disable.ddl";

    @BeforeAll
    public static void init() {
        HashMap<String, OracleParallelSessionParam> params = createParams();
        mockedConstruction = Mockito.mockConstruction(OracleParallelSessionParamFactory.class,
                (mock, context) -> {
                    when(mock.create(property, value1, delimiter)).thenReturn(params.get(value1));
                    when(mock.create(property, value2, delimiter)).thenReturn(params.get(value2));
                    when(mock.create(property, value3, delimiter)).thenReturn(params.get(value3));
                    when(mock.create(property, value4, delimiter)).thenReturn(params.get(value4));
                });
    }

    @Test
    @SuppressWarnings("try")
    void createParallelSessionQueryWithForceAndDegreeOfParallelism() {
        String expectedResult = "ALTER SESSION FORCE PARALLEL QUERY PARALLEL 4";

        OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
        String result = oracleSessionQueryFactory.create(property, value1);
        assertEquals(expectedResult, result);
    }

    @Test
    @SuppressWarnings("try")
    void createParallelSessionQueryWithForce() {
        String expectedResult = "ALTER SESSION FORCE PARALLEL DML";

        OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
        String result = oracleSessionQueryFactory.create(property, value2);
        assertEquals(expectedResult, result);
    }

    @Test
    @SuppressWarnings("try")
    void createParallelSessionQueryWithEnable() {
        String expectedResult = "ALTER SESSION ENABLE PARALLEL DML";

        OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
        String result = oracleSessionQueryFactory.create(property, value3);
        assertEquals(expectedResult, result);
    }

    @Test
    @SuppressWarnings("try")
    void createParallelSessionQueryWithDisable() {
        String expectedResult = "ALTER SESSION DISABLE PARALLEL DML";

        OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
        String result = oracleSessionQueryFactory.create(property, value4);
        assertEquals(expectedResult, result);
    }

    @Test
    @SuppressWarnings("try")
    void createNotParallelSessionQuery() {
        String property = "STATISTICS_LEVEL";
        String value = "TYPICAL";
        String expectedResult = "ALTER SESSION SET STATISTICS_LEVEL = TYPICAL";

        OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
        String result = oracleSessionQueryFactory.create(property, value);
        assertEquals(expectedResult, result);
    }

    @AfterAll
    public static void endTest() {
        mockedConstruction.close();
    }

    private static HashMap<String, OracleParallelSessionParam> createParams() {
        HashMap<String, OracleParallelSessionParam> params = new HashMap<>();

        OracleParallelSessionParam param1 = new OracleParallelSessionParam();
        param1.setClause(OracleParallelSessionParam.Clause.FORCE);
        param1.setStatementType(OracleParallelSessionParam.StatementType.QUERY);
        param1.setDegreeOfParallelism("4");
        params.put(value1, param1);

        OracleParallelSessionParam param2 = new OracleParallelSessionParam();
        param2.setClause(OracleParallelSessionParam.Clause.FORCE);
        param2.setStatementType(OracleParallelSessionParam.StatementType.DML);
        param2.setDegreeOfParallelism("");
        params.put(value2, param2);

        OracleParallelSessionParam param3 = new OracleParallelSessionParam();
        param3.setClause(OracleParallelSessionParam.Clause.ENABLE);
        param3.setStatementType(OracleParallelSessionParam.StatementType.DML);
        param3.setDegreeOfParallelism("");
        params.put(value3, param3);

        OracleParallelSessionParam param4 = new OracleParallelSessionParam();
        param4.setClause(OracleParallelSessionParam.Clause.DISABLE);
        param4.setStatementType(OracleParallelSessionParam.StatementType.DML);
        param4.setDegreeOfParallelism("");
        params.put(value4, param4);

        return params;
    }
}
