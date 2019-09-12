package org.greenplum.pxf.plugins.s3;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.BaseFilterBuilder;
import org.greenplum.pxf.api.FilterParser;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.EnumSet;

public class S3SelectFilterParser extends BaseFilterBuilder {

    private boolean usePositionToIdentifyColumn;
    private static final EnumSet<FilterParser.Operation> SUPPORTED_OPERATIONS =
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
    private static final EnumSet<FilterParser.LogicalOperation> SUPPORTED_OPERATORS =
            EnumSet.of(
                    FilterParser.LogicalOperation.HDOP_AND,
                    FilterParser.LogicalOperation.HDOP_NOT,
                    FilterParser.LogicalOperation.HDOP_OR
            );

    S3SelectFilterParser() {
        super(SUPPORTED_OPERATIONS, SUPPORTED_OPERATORS);
    }

    @Override
    public String buildFilterString(String filterInput) throws Exception {
        String filterString = super.buildFilterString(filterInput);
        return StringUtils.isNotBlank(filterString) ? " WHERE " + filterString : "";
    }

    @Override
    protected void serializeColumnName(StringBuilder result, FilterParser.Operation operation, DataType type, ColumnDescriptor filterColumn, String columnName) {
        if (operation == FilterParser.Operation.HDOP_IS_NULL || operation == FilterParser.Operation.HDOP_IS_NOT_NULL) {
            result.append(columnName);
        } else {
            switch (type) {
                case BIGINT:
                case INTEGER:
                case SMALLINT:
                    result.append("CAST (")
                            .append(columnName)
                            .append(" AS int)");
                    break;
                case BOOLEAN:
                    result.append("CAST (")
                            .append(columnName)
                            .append(" AS bool)");
                    break;
                case FLOAT8:
                    result.append("CAST (")
                            .append(columnName)
                            .append(" AS float)");
                    break;
                case REAL:
                    result.append("CAST (")
                            .append(columnName)
                            .append(" AS decimal)");
                    break;
                case TEXT:
                case VARCHAR:
                case BPCHAR:
                    result.append(columnName);
                    break;
                case DATE:
                case TIMESTAMP:
                    result.append("TO_TIMESTAMP(")
                            .append(columnName)
                            .append(")");
                    break;
                default:
                    throw new UnsupportedOperationException(String.format("Unsupported column type for filtering '%s'", filterColumn.columnTypeCode()));
            }
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
    @Override
    protected String getColumnName(ColumnDescriptor column) {
        // TODO: this code is duplicated in S3SelectQueryBuilder
        return usePositionToIdentifyColumn ?
                String.format("%s._%d", S3SelectQueryBuilder.S3_TABLE_ALIAS, column.columnIndex() + 1) :
                String.format("%s.\"%s\"", S3SelectQueryBuilder.S3_TABLE_ALIAS, column.columnName());
    }

    @Override
    protected boolean isCompliantWithOperator(FilterParser.LogicalOperation operator) {
        return true;
    }

    @Override
    protected boolean isFilterCompatible(String filterColumnName, FilterParser.Operation operation, FilterParser.LogicalOperation logicalOperation) {
        return true;
    }

    @Override
    protected String serializeValue(Object val, DataType type) {
        switch (type) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT8:
            case REAL:
            case BOOLEAN:
                return val.toString();
            case TEXT:
            case VARCHAR:
            case BPCHAR:
                return "'" + StringUtils.replace(val.toString(), "'", "''") + "'";
            case DATE:
            case TIMESTAMP:
                return "TO_TIMESTAMP('" + val.toString() + "')";
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported column type for filtering '%s' ", type.getOID()));
        }
    }

    void setUsePositionToIdentifyColumn(boolean usePositionToIdentifyColumn) {
        this.usePositionToIdentifyColumn = usePositionToIdentifyColumn;
    }
}
