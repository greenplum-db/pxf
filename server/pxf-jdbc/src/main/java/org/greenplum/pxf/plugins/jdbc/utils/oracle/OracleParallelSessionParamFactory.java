package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.HashMap;

public class OracleParallelSessionParamFactory {
    public OracleParallelSessionParam create(String property, String value, String delimiter) {
        String[] values = splitValue(property, value, delimiter);

        HashMap<String, String> map = getParallelSessionParam(values);
        String clause = map.get("clause").toUpperCase();
        String statementType = map.get("statement_type").toUpperCase();
        String degreeOfParallelism = map.get("degree_of_parallelism");

        OracleParallelSessionParam param = new OracleParallelSessionParam();
        param.setClause(getClause(clause, property));
        param.setStatementType(getStatementType(statementType, property));
        param.setDegreeOfParallelism(getDegreeOfParallelism(degreeOfParallelism, property));
        return param;
    }

    private String[] splitValue(String property, String value, String delimiter) {
        validateValue(property, value);
        String[] values = value.split(delimiter);
        if (values.length < 2 || values.length > 3) {
            throw new IllegalArgumentException(String.format(
                    "The parameter '%s' in jdbc-site.xml has to contain at least 2 but not more then 3 values delimited by %s",
                    property, delimiter)
            );
        }
        return values;
    }

    private void validateValue(String property, String value) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(String.format("The parameter '%s' is blank in jdbc-site.xml", property));
        }
    }

    private HashMap<String, String> getParallelSessionParam(String[] values) {
        HashMap<String, String> params = new HashMap<>();
        params.put("clause", values[0]);
        params.put("statement_type", values[1]);
        if (values.length == 3 && Strings.isNotBlank(values[2])) {
            params.put("degree_of_parallelism", values[2]);
        }
        return params;
    }

    private OracleParallelSessionParam.Clause getClause(String clause, String property) {
        try {
            return OracleParallelSessionParam.Clause.valueOf(clause);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                    "The 'clause' value '%s' in the parameter '%s' is not valid", clause, property)
            );
        }
    }

    private OracleParallelSessionParam.StatementType getStatementType(String statementType, String property) {
        try {
            return OracleParallelSessionParam.StatementType.valueOf(statementType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                    "The 'statement type' value '%s' in the parameter '%s' is not valid", statementType, property)
            );
        }
    }

    private String getDegreeOfParallelism(String degreeOfParallelism, String property) {
        if (degreeOfParallelism == null) {
            return Strings.EMPTY;
        }
        try {
            Integer.parseInt(degreeOfParallelism);
            return degreeOfParallelism;
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(String.format(
                    "The 'degree of parallelism' value '%s' in the parameter '%s' is not valid", degreeOfParallelism, property)
            );
        }
    }
}
