package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class S3SelectQueryBuilder {

    private static final String S3_SELECT_SOURCE = "S3Object";
    static final String S3_TABLE_ALIAS = "s";

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

        S3SelectFilterParser filterParser = new S3SelectFilterParser();
        filterParser.setColumnDescriptors(context.getTupleDescription());
        filterParser.setUsePositionToIdentifyColumn(usePositionToIdentifyColumn);

        try {
            // No exceptions were thrown, change the provided query
            query.append(filterParser.buildWhereClause(context.getFilterString()));
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
        // TODO: this code is duplicated in S3SelectFilterParser
        return usePositionToIdentifyColumn ?
                String.format("%s._%d", S3_TABLE_ALIAS, column.columnIndex() + 1) :
                String.format("%s.\"%s\"", S3_TABLE_ALIAS, column.columnName());
    }
}
