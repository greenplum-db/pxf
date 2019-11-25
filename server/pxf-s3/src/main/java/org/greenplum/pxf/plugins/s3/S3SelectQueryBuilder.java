package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.filter.BaseTreePruner;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.TreePruner;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public class S3SelectQueryBuilder {

    private static final String S3_SELECT_SOURCE = "S3Object";
    static final String S3_TABLE_ALIAS = "s";
    private static final EnumSet<Operator> SUPPORTED_OPERATORS =
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
    private static final TreePruner treePruner = new BaseTreePruner(SUPPORTED_OPERATORS);

    private final RequestContext context;
    private List<ColumnDescriptor> columns;
    private boolean usePositionToIdentifyColumn;
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    public S3SelectQueryBuilder(RequestContext context, boolean usePositionToIdentifyColumn) {
        this.context = context;
        this.usePositionToIdentifyColumn = usePositionToIdentifyColumn;
        this.columns = context.getTupleDescription();
    }

    /**
     * Build SELECT query with WHERE clause
     *
     * @return S3 Select SQL query
     */
    public String buildSelectQuery() {
        String columnsQuery = columns.stream()
                .map(c -> c.isProjected() ? getColumnName(c) : "null")
                .collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder("SELECT ")
                .append(columnsQuery)
                .append(" FROM ")
                .append(S3_SELECT_SOURCE)
                .append(" ")
                .append(S3_TABLE_ALIAS);

        // Insert regular WHERE constraints
        buildWhereSQL(sb);

        return sb.toString();
    }

    /**
     * Build a WHERE statement using the RequestContext provided to constructor.
     */
    private void buildWhereSQL(StringBuilder query) {
        if (!context.hasFilter()) return;

        S3SelectTreeVisitor s3SelectTreeVisitor = new S3SelectTreeVisitor(
                usePositionToIdentifyColumn,
                context.getTupleDescription());

        try {
            Node root = new FilterParser().parse(context.getFilterString().getBytes());
            root = treePruner.prune(root);
            new TreeTraverser().inOrderTraversal(root, s3SelectTreeVisitor);

            // No exceptions were thrown, change the provided query
            query.append(s3SelectTreeVisitor.toString());
        } catch (Exception e) {
            LOG.debug("WHERE clause is omitted: " + e.toString());
            // Silence the exception and do not insert constraints
        }
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
