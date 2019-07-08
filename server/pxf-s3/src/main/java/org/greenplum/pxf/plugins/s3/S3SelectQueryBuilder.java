package org.greenplum.pxf.plugins.s3;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.BasicFilter;
import org.greenplum.pxf.api.FilterParser;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.JdbcFilterParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class S3SelectQueryBuilder {

    private static final String S3_SELECT_SOURCE = "S3Object";
    private static final String S3_TABLE_ALIAS = "s";
    private static final EnumSet<FilterParser.Operation> SUPPORTED_OPERATORS =
            EnumSet.of(
                    FilterParser.Operation.HDOP_LT,
                    FilterParser.Operation.HDOP_GT,
                    FilterParser.Operation.HDOP_LE,
                    FilterParser.Operation.HDOP_GE,
                    FilterParser.Operation.HDOP_EQ,
                    // TODO: LIKE is not supported on the C side
                    // FilterParser.Operation.HDOP_LIKE,
                    FilterParser.Operation.HDOP_NE,
                    FilterParser.Operation.HDOP_IN,
                    FilterParser.Operation.HDOP_IS_NULL,
                    FilterParser.Operation.HDOP_IS_NOT_NULL
            );

    private final RequestContext context;
    private List<ColumnDescriptor> columns;
    private boolean usePositionToIdentifyColumn;
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    public S3SelectQueryBuilder(RequestContext context, boolean usePositionToIdentifyColumn) {
        this.context = context;
        this.usePositionToIdentifyColumn = usePositionToIdentifyColumn;
        columns = context.getTupleDescription();
    }

    /**
     * Build SELECT query with WHERE clause
     *
     * @return S3 Select SQL query
     */
    public String buildSelectQuery() throws ParseException {
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
     *
     * @return SQL string
     * @throws ParseException if 'RequestContext' has filter, but its 'filterString' is incorrect
     */
    private void buildWhereSQL(StringBuilder query) throws ParseException {
        if (!context.hasFilter()) return;

        try {
            StringBuilder prepared = new StringBuilder(" WHERE ");

            // Get constraints
            List<BasicFilter> filters = JdbcFilterParser
                    .parseFilters(context.getFilterString());

            String andDivisor = "";
            for (Object obj : filters) {
                prepared.append(andDivisor);
                andDivisor = " AND ";

                // Insert constraint column name
                BasicFilter filter = (BasicFilter) obj;
                ColumnDescriptor column = context.getColumn(filter.getColumn().index());
                String columnName = getColumnName(column);

                switch (DataType.get(column.columnTypeCode())) {
                    case BIGINT:
                    case INTEGER:
                    case SMALLINT:
                        prepared.append("CAST (")
                                .append(columnName)
                                .append(" AS int)");
                        break;
                    case BOOLEAN:
                        prepared.append("CAST (")
                                .append(columnName)
                                .append(" AS bool)");
                        break;
                    case FLOAT8:
                        prepared.append("CAST (")
                                .append(columnName)
                                .append(" AS FLOAT)");
                        break;
                    case REAL:
                        prepared.append("CAST (")
                                .append(columnName)
                                .append(" AS decimal)");
                        break;
                    case TEXT:
                        prepared.append(columnName);
                        break;
                    case DATE:
                    case TIMESTAMP:
                        prepared.append("TO_TIMESTAMP(")
                                .append(columnName)
                                .append(")");
                        break;
                    default:
                        throw new UnsupportedOperationException(String.format(
                                "Unsupported column type for filtering '%s' ",
                                column.columnTypeCode()));
                }

                // Insert constraint operator
                FilterParser.Operation op = filter.getOperation();
                if (SUPPORTED_OPERATORS.contains(op)) {
                    prepared.append(" ")
                            .append(op.getOperator());
                } else {
                    throw new UnsupportedOperationException(String.format(
                            "Unsupported filter operation '%s'", op));
                }

                if (op != FilterParser.Operation.HDOP_IS_NULL && op != FilterParser.Operation.HDOP_IS_NOT_NULL) {

                    DataType dataType = DataType.get(column.columnTypeCode());

                    // Insert constraint constant
                    Object val = filter.getConstant().constant();
                    if (val instanceof Iterable) {
                        Iterable<?> iterable = (Iterable<?>) val;
                        String listValue = StreamSupport.stream(iterable.spliterator(), false)
                                .map(s -> mapValue(s, dataType))
                                .collect(Collectors.joining(","));
                        prepared.append(" (").append(listValue).append(")");
                    } else {
                        prepared.append(" ").append(mapValue(val, dataType));
                    }
                }
            }

            // No exceptions were thrown, change the provided query
            query.append(prepared);
        } catch (UnsupportedOperationException e) {
            LOG.debug("WHERE clause is omitted: " + e.toString());
            // Silence the exception and do not insert constraints
        }
    }

    private String mapValue(Object val, DataType dataType) {
        switch (dataType) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT8:
            case REAL:
            case BOOLEAN:
                return val.toString();
            case TEXT:
                return "'" + StringUtils.replace(val.toString(), "'", "''") + "'";
            case DATE:
            case TIMESTAMP:
                return "TO_TIMESTAMP('" + val.toString() + "')";
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported column type for filtering '%s' ", dataType.getOID()));
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
