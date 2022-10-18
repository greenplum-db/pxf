package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.util.Strings;

/**
 * Factory class to create and validate Oracle parallel session parameters.
 */
public class OracleParallelSessionParamFactory {
    /**
     * Returns parallel session parameters to build Oracle session query
     *
     * @param property  variable name
     * @param value     variable value
     * @param delimiter delimiter which is used to split value to get session parameters
     * @return An instance of {@link OracleParallelSessionParam} made from value
     */
    public OracleParallelSessionParam create(String property, String value, String delimiter) {
        String[] values = splitValue(property, value, delimiter);

        String clause = values[0];
        String statementType = values[1];
        String degreeOfParallelism = (values.length == 3 && Strings.isNotBlank(statementType)) ? values[2] : null;

        OracleParallelSessionParam param = new OracleParallelSessionParam();
        param.setClause(getClause(clause, property));
        param.setStatementType(getStatementType(statementType, property));
        param.setDegreeOfParallelism(getDegreeOfParallelism(degreeOfParallelism, clause, property));
        return param;
    }

    /**
     * Split and validate value to get parallel session parameters
     *
     * @param property  variable name
     * @param value     variable value
     * @param delimiter delimiter which is used to split value to get session parameters
     * @return string array with session parameters to build Clause, StatementType and DegreeOfParallelism
     * @throws IllegalArgumentException if the session parameter is not valid
     */
    private String[] splitValue(String property, String value, String delimiter) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(String.format("The parameter '%s' is blank in jdbc-site.xml", property));
        }
        String[] values = value.split(delimiter);
        if (values.length < 2 || values.length > 3) {
            throw new IllegalArgumentException(String.format(
                    "The parameter '%s' in jdbc-site.xml must contain at least 2 but not more than 3 values delimited by %s",
                    property, delimiter)
            );
        }
        return values;
    }

    /**
     * Return the first part of the
     * parallel session parameter {@link org.greenplum.pxf.plugins.jdbc.utils.oracle.OracleParallelSessionParam.Clause}
     *
     * @param clause   string to build the first part (Clause) of the parallel session parameter
     * @param property variable name
     * @return An instance of {@link org.greenplum.pxf.plugins.jdbc.utils.oracle.OracleParallelSessionParam.Clause}
     * @throws IllegalArgumentException if the Clause parameter is not valid
     */
    private OracleParallelSessionParam.Clause getClause(String clause, String property) {
        try {
            return OracleParallelSessionParam.Clause.valueOf(clause.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                    "The 'clause' value '%s' in the parameter '%s' is not valid", clause, property)
            );
        }
    }

    /**
     * Return the second part of the
     * parallel session parameter {@link org.greenplum.pxf.plugins.jdbc.utils.oracle.OracleParallelSessionParam.StatementType}
     *
     * @param statementType string to build the second part (StatementType) of the parallel session parameter
     * @param property      variable name
     * @return An instance of {@link org.greenplum.pxf.plugins.jdbc.utils.oracle.OracleParallelSessionParam.StatementType}
     * @throws IllegalArgumentException if the StatementType parameter is not valid
     */
    private OracleParallelSessionParam.StatementType getStatementType(String statementType, String property) {
        try {
            return OracleParallelSessionParam.StatementType.valueOf(statementType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                    "The 'statement type' value '%s' in the parameter '%s' is not valid", statementType, property)
            );
        }
    }

    /**
     * Return the third part of the parallel session parameter
     *
     * @param degreeOfParallelism string to build the third part (DegreeOfParallelism) of the parallel session parameter
     * @param clause              the value of the Clause session parameter
     * @param property            variable name
     * @return the degree of parallelism value
     * @throws IllegalArgumentException if the DegreeOfParallelism parameter is not valid
     */
    private String getDegreeOfParallelism(String degreeOfParallelism, String clause, String property) {
        if (degreeOfParallelism == null || !OracleParallelSessionParam.Clause.FORCE.name().equalsIgnoreCase(clause)) {
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
