package org.greenplum.pxf.plugins.jdbc.utils.oracle;

public class OracleJdbcUtils {
    private static final OracleSessionQueryFactory sessionQueryFactory = new OracleSessionQueryFactory();

    public static String buildSessionQuery(String property, String value) {
        return sessionQueryFactory.create(property, value);
    }
}
