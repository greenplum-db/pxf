package org.greenplum.pxf.plugins.s3;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.BasicFilter;
import org.greenplum.pxf.api.FilterParser;
import org.greenplum.pxf.api.LogicalFilter;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class S3SelectFilterParser implements FilterParser.FilterBuilder {

    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private boolean usePositionToIdentifyColumn;
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

    private List<ColumnDescriptor> columnDescriptors;

    @Override
    public Object build(FilterParser.LogicalOperation op, Object leftOperand, Object rightOperand) {
        return handleLogicalOperation(op, leftOperand, rightOperand);
    }

    @Override
    public Object build(FilterParser.LogicalOperation op, Object filter) {
        return handleLogicalOperation(op, filter);
    }

    @Override
    public Object build(FilterParser.Operation opId, Object leftOperand,
                        Object rightOperand) {
        // Assume column is on the left
        return handleSimpleOperations(opId,
                (FilterParser.ColumnIndex) leftOperand,
                (FilterParser.Constant) rightOperand
        );
    }

    @Override
    public Object build(FilterParser.Operation operation, Object operand) throws UnsupportedOperationException {
        if (operation == FilterParser.Operation.HDOP_IS_NULL || operation == FilterParser.Operation.HDOP_IS_NOT_NULL) {
            // Use null for the constant value of null comparison
            return handleSimpleOperations(operation, (FilterParser.ColumnIndex) operand, null);
        } else {
            throw new UnsupportedOperationException("Unsupported unary operation '" + operation + "'");
        }
    }

    /*
     * Handles simple column-operator-constant expressions.
     * Creates a special filter in the case the column is the row key column
     */
    private BasicFilter handleSimpleOperations(FilterParser.Operation opId,
                                               FilterParser.ColumnIndex column,
                                               FilterParser.Constant constant) {
        return new BasicFilter(opId, column, constant);
    }

    private Object handleLogicalOperation(FilterParser.LogicalOperation operator, Object leftOperand, Object rightOperand) {
        List<Object> result = new LinkedList<>();

        result.add(leftOperand);
        result.add(rightOperand);
        return new LogicalFilter(operator, result);
    }

    private Object handleLogicalOperation(FilterParser.LogicalOperation operator, Object filter) {
        return new LogicalFilter(operator, Collections.singletonList(filter));
    }

    /**
     * Translates a filterString into a {@link BasicFilter} or a
     * list of such filters.
     *
     * @param filterString the string representation of the filter
     * @return a single {@link BasicFilter}
     * object or a {@link java.util.List} of
     * {@link BasicFilter} objects.
     * @throws Exception if parsing the filter failed or filter is not a basic
     *                   filter or list of basic filters
     */
    private Object getFilterObject(String filterString) throws Exception {
        FilterParser parser = new FilterParser(this);
        Object result = parser.parse(filterString.getBytes(FilterParser.DEFAULT_CHARSET));

        if (!(result instanceof LogicalFilter) && !(result instanceof BasicFilter)
                && !(result instanceof List)) {
            throw new Exception(String.format("String '%s' resolved to no filter", filterString));
        }

        return result;
    }


    public String buildWhereClause(String filterInput) throws Exception {
        LOG.debug("Filter string input: {}", filterInput);

        String filterString;
        Object filter = getFilterObject(filterInput);

        if (filter instanceof LogicalFilter) {
            filterString = buildCompoundFilter((LogicalFilter) filter);
        } else {
            filterString = buildSingleFilter(filter, null);
        }

        return StringUtils.isNotBlank(filterString) ? " WHERE " + filterString : "";
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
            case VARCHAR:
            case BPCHAR:
                return "'" + StringUtils.replace(val.toString(), "'", "''") + "'";
            case DATE:
            case TIMESTAMP:
                return "TO_TIMESTAMP('" + val.toString() + "')";
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported column type for filtering '%s' ", dataType.getOID()));
        }
    }


    private String buildCompoundFilter(LogicalFilter filter) {
        String logicalOperator;
        String outputFormat = "(%s)";
        switch (filter.getOperator()) {
            case HDOP_AND:
                logicalOperator = " AND ";
                break;
            case HDOP_OR:
                logicalOperator = " OR ";
                break;
            case HDOP_NOT:
                logicalOperator = " NOT ";
                outputFormat = "%s";
                break;
            default:
                // should not get here
                return null;
        }

        StringBuilder filterString = new StringBuilder();
        String deserializedFilter;
        for (Object f : filter.getFilterList()) {
            if (f instanceof LogicalFilter) {
                deserializedFilter = buildCompoundFilter((LogicalFilter) f);
            } else {
                deserializedFilter = buildSingleFilter(f, filter.getOperator());
            }

            if (deserializedFilter != null) {
                if (filter.getOperator() == FilterParser.LogicalOperation.HDOP_NOT) {
                    filterString
                            .append("NOT (")
                            .append(deserializedFilter)
                            .append(")");
                } else {
                    // We only append the operator if there is something on the filterString
                    if (filterString.length() > 0) {
                        filterString.append(logicalOperator);
                    }
                    filterString.append(deserializedFilter);
                }
            }
        }

        return (filterString.length() > 0) ?
                String.format(outputFormat, filterString.toString()) : null;
    }

    private String buildSingleFilter(Object filter, FilterParser.LogicalOperation logicalOperation) {

        // Let's look first at the filter
        BasicFilter bFilter = (BasicFilter) filter;

        // Extract column name and value
        int filterColumnIndex = bFilter.getColumn().index();
        ColumnDescriptor filterColumn = columnDescriptors.get(filterColumnIndex);
        String filterColumnName = getColumnName(filterColumn);
        FilterParser.Operation operation = ((BasicFilter) filter).getOperation();
        DataType dataType = DataType.get(filterColumn.columnTypeCode());

        StringBuilder result = new StringBuilder();

        if (operation == FilterParser.Operation.HDOP_IS_NULL || operation == FilterParser.Operation.HDOP_IS_NOT_NULL) {
            result.append(filterColumnName);
        } else {
            switch (dataType) {
                case BIGINT:
                case INTEGER:
                case SMALLINT:
                    result.append("CAST (")
                            .append(filterColumnName)
                            .append(" AS int)");
                    break;
                case BOOLEAN:
                    result.append("CAST (")
                            .append(filterColumnName)
                            .append(" AS bool)");
                    return result.toString();
                case FLOAT8:
                    result.append("CAST (")
                            .append(filterColumnName)
                            .append(" AS float)");
                    break;
                case REAL:
                    result.append("CAST (")
                            .append(filterColumnName)
                            .append(" AS decimal)");
                    break;
                case TEXT:
                case VARCHAR:
                case BPCHAR:
                    result.append(filterColumnName);
                    break;
                case DATE:
                case TIMESTAMP:
                    result.append("TO_TIMESTAMP(")
                            .append(filterColumnName)
                            .append(")");
                    break;
                default:
                    throw new UnsupportedOperationException(String.format("Unsupported column type for filtering '%s'", filterColumn.columnTypeCode()));
            }
        }

        if (SUPPORTED_OPERATORS.contains(operation)) {
            result.append(" ")
                    .append(operation.getOperator());
        } else {
            result.setLength(0);
            return null;
        }

        boolean operationRequiresValue =
                operation != FilterParser.Operation.HDOP_IS_NULL && operation != FilterParser.Operation.HDOP_IS_NOT_NULL;

        if (operationRequiresValue) {

            // Insert constraint constant
            Object val = bFilter.getConstant().constant();
            if (val instanceof Iterable) {
                Iterable<?> iterable = (Iterable<?>) val;
                String listValue = StreamSupport.stream(iterable.spliterator(), false)
                        .map(s -> mapValue(s, dataType))
                        .collect(Collectors.joining(","));
                result.append(" (").append(listValue).append(")");
            } else {
                result.append(" ").append(mapValue(val, dataType));
            }
        }

        return result.toString();
    }

    public void setColumnDescriptors(List<ColumnDescriptor> columnDescriptors) {
        this.columnDescriptors = columnDescriptors;
    }

    public void setUsePositionToIdentifyColumn(boolean usePositionToIdentifyColumn) {
        this.usePositionToIdentifyColumn = usePositionToIdentifyColumn;
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
        // TODO: this code is duplicated in S3SelectQueryBuilder
        return usePositionToIdentifyColumn ?
                String.format("%s._%d", S3SelectQueryBuilder.S3_TABLE_ALIAS, column.columnIndex() + 1) :
                String.format("%s.\"%s\"", S3SelectQueryBuilder.S3_TABLE_ALIAS, column.columnName());
    }
}
