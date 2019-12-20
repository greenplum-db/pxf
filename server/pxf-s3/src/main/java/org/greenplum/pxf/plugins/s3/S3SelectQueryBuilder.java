package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.JdbcPredicateBuilder;
import org.greenplum.pxf.plugins.jdbc.SQLQueryBuilder;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds a query for the S3 Select API
 */
public class S3SelectQueryBuilder extends SQLQueryBuilder {

    static final String S3_SELECT_SOURCE = "S3Object";
    static final String S3_TABLE_ALIAS = "s";
    static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    // TODO: LIKE is not supported on the C side
                    // Operator.LIKE,
                    Operator.NOT_EQUALS,
                    Operator.IN,
                    Operator.IS_NULL,
                    Operator.IS_NOT_NULL,
                    Operator.NOOP,
                    Operator.AND,
                    Operator.NOT,
                    Operator.OR
            );
    static final TreeVisitor PRUNER = new SupportedOperatorPruner(SUPPORTED_OPERATORS);

    private List<ColumnDescriptor> columns;
    private boolean usePositionToIdentifyColumn;

    /**
     * Constructor
     *
     * @param context                     the request context
     * @param usePositionToIdentifyColumn whether to use the column name or the
     *                                    position to identify the column
     * @throws SQLException when a SQL exception occurs
     */
    public S3SelectQueryBuilder(RequestContext context,
                                boolean usePositionToIdentifyColumn) throws SQLException {
        super(context, new S3SelectDatabaseMetaData());
        this.usePositionToIdentifyColumn = usePositionToIdentifyColumn;
        this.columns = context.getTupleDescription();
    }

    @Override
    protected String buildColumnsQuery() {
        return columns.stream()
                .map(c -> c.isProjected() ? getColumnName(c) : "null")
                .collect(Collectors.joining(", "));
    }

    @Override
    protected String getSource() {
        return String.format("%s %s", S3_SELECT_SOURCE, S3_TABLE_ALIAS);
    }

    @Override
    protected JdbcPredicateBuilder getPredicateBuilder() {
        return new S3SelectPredicateBuilder(
                usePositionToIdentifyColumn,
                context.getTupleDescription());
    }

    @Override
    protected TreeVisitor getPruner() {
        return PRUNER;
    }

    @Override
    public void buildFragmenterSql(RequestContext context, DbProduct dbProduct, String quoteString, StringBuilder query) {
        // DO NOTHING: fragmenter is not supported by S3 Select yet
    }

    /**
     * Returns the column name. If we use the column position to identify the column
     * we return the index of the column as the column name. Otherwise, we use
     * the actual column name
     *
     * @param column the column descriptor
     * @return the column name to use as part of the query
     */
    private String getColumnName(ColumnDescriptor column) {
        return usePositionToIdentifyColumn ?
                String.format("%s._%d", S3_TABLE_ALIAS, column.columnIndex() + 1) :
                String.format("%s.\"%s\"", S3_TABLE_ALIAS, column.columnName());
    }
}
