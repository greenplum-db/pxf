package org.greenplum.pxf.api;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class BaseFilterBuilder implements FilterBuilder {

    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final EnumSet<FilterParser.Operation> supportedOperations;
    private final EnumSet<FilterParser.LogicalOperation> supportedOperators;

    private List<ColumnDescriptor> columnDescriptors;

    public BaseFilterBuilder(EnumSet<FilterParser.Operation> supportedOperations, EnumSet<FilterParser.LogicalOperation> supportedOperators) {
        this.supportedOperations = supportedOperations;
        this.supportedOperators = supportedOperators;
    }

    public String buildFilterString(String filterInput) throws Exception {
        LOG.debug("Filter string input: {}", filterInput);

        String filterString;
        Object filter = getFilterObject(filterInput);

        if (filter instanceof LogicalFilter) {
            filterString = buildCompoundFilter((LogicalFilter) filter);
        } else {
            filterString = buildSingleFilter(filter, null);
        }

        return StringUtils.isNotBlank(filterString) ? filterString : null;
    }

    public void setColumnDescriptors(List<ColumnDescriptor> columnDescriptors) {
        this.columnDescriptors = columnDescriptors;
    }

    @Override
    public Object build(FilterParser.LogicalOperation operation, Object left, Object right) {
        return handleLogicalOperation(operation, left, right);
    }

    @Override
    public Object build(FilterParser.LogicalOperation operation, Object filter) {
        return handleLogicalOperation(operation, filter);
    }

    @Override
    public Object build(FilterParser.Operation operation, Object left, Object right) throws Exception {
        // Assume column is on the left
        return handleSimpleOperations(operation, (FilterParser.ColumnIndex) left, (FilterParser.Constant) right);
    }

    @Override
    public Object build(FilterParser.Operation operation, Object operand) throws UnsupportedOperationException {
        if (operation == FilterParser.Operation.HDOP_IS_NULL || operation == FilterParser.Operation.HDOP_IS_NOT_NULL) {
            // Use null for the constant value of null comparison
            return handleSimpleOperations(operation, (FilterParser.ColumnIndex) operand, null);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported unary operation '%s'", operation));
        }
    }

    protected abstract String getColumnName(ColumnDescriptor column);

    protected abstract boolean isCompliantWithOperator(FilterParser.LogicalOperation operator);

    protected abstract boolean isFilterCompatible(String filterColumnName, FilterParser.Operation operation, FilterParser.LogicalOperation logicalOperation);

    protected abstract void addColumnName(StringBuilder result, FilterParser.Operation operation, DataType type, ColumnDescriptor filterColumn, String columnName);

    protected abstract String mapValue(Object val, DataType type);

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
    public Object getFilterObject(String filterString) throws Exception {
        if (filterString == null) return null;

        FilterParser parser = new FilterParser(this);
        Object result = parser.parse(filterString.getBytes(FilterParser.DEFAULT_CHARSET));

        if (!(result instanceof LogicalFilter) && !(result instanceof BasicFilter)
                && !(result instanceof List)) {
            throw new Exception(String.format("String '%s' resolved to no filter", filterString));
        }

        return result;
    }

    /**
     * Build filter string for a single filter and append to the filters string.
     *
     * @param filter the filter
     * @param logicalOperation the logical operation
     * @return the filter string if successful, null otherwise
     */
    private String buildSingleFilter(Object filter, FilterParser.LogicalOperation logicalOperation) {

        // Let's look first at the filter
        BasicFilter bFilter = (BasicFilter) filter;

        // Extract column name and value
        int filterColumnIndex = bFilter.getColumn().index();
        ColumnDescriptor filterColumn = columnDescriptors.get(filterColumnIndex);
        String filterColumnName = getColumnName(filterColumn);
        FilterParser.Operation operation = ((BasicFilter) filter).getOperation();
        DataType dataType = DataType.get(filterColumn.columnTypeCode());

        // if filter is determined to be not being able to be pushed down, but not violating logical correctness,
        // we just skip it and return null
        if (!isFilterCompatible(filterColumnName, operation, logicalOperation)) {
            return null;
        }

        StringBuilder result = new StringBuilder();

        addColumnName(result, operation, dataType, filterColumn, filterColumnName);

        if (dataType == DataType.BOOLEAN)
            return result.toString();

        if (supportedOperations.contains(operation)) {
            result
                    .append(" ")
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

    private String buildCompoundFilter(LogicalFilter filter) {

        if (!supportedOperators.contains(filter.getOperator())) {
            return null;
        }

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
        String serializedFilter;
        for (Object f : filter.getFilterList()) {
            if (f instanceof LogicalFilter) {
                serializedFilter = buildCompoundFilter((LogicalFilter) f);
            } else {
                serializedFilter = buildSingleFilter(f, filter.getOperator());
            }

            if (serializedFilter != null) {
                if (filter.getOperator() == FilterParser.LogicalOperation.HDOP_NOT) {
                    filterString
                            .append("NOT (")
                            .append(serializedFilter)
                            .append(")");
                } else {
                    // We only append the operator if there is something on the filterString
                    if (filterString.length() > 0) {
                        filterString.append(logicalOperator);
                    }
                    filterString.append(serializedFilter);
                }
            } else if (!isCompliantWithOperator(filter.getOperator())) {
                return null;
            }
        }

        return (filterString.length() > 0) ?
                String.format(outputFormat, filterString.toString()) : null;
    }

    /*
     * Handles simple column-operator-constant expressions.
     * Creates a special filter in the case the column is the row key column
     */
    private BasicFilter handleSimpleOperations(FilterParser.Operation operation,
                                               FilterParser.ColumnIndex column,
                                               FilterParser.Constant constant) {
        return new BasicFilter(operation, column, constant);
    }

    private Object handleLogicalOperation(FilterParser.LogicalOperation operator, Object left, Object right) {
        return new LogicalFilter(operator, Arrays.asList(left, right));
    }

    private Object handleLogicalOperation(FilterParser.LogicalOperation operator, Object filter) {
        return new LogicalFilter(operator, Collections.singletonList(filter));
    }
}
