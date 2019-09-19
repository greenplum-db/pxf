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

import static org.greenplum.pxf.api.FilterParser.Operator.NOT;

public abstract class BaseFilterBuilder implements FilterBuilder {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final EnumSet<FilterParser.Operator> supportedOperators;
    private final EnumSet<FilterParser.Operator> supportedLogicalOperators;

    private List<ColumnDescriptor> columnDescriptors;

    public BaseFilterBuilder(EnumSet<FilterParser.Operator> supportedOperations,
                             EnumSet<FilterParser.Operator> supportedOperators) {
        this.supportedOperators = supportedOperations;
        this.supportedLogicalOperators = supportedOperators;
    }

    public String buildFilterString(String filterInput) throws Exception {
        LOG.debug("Filter string input: {}", filterInput);

        String filterString;
        Object filter = getFilterObject(filterInput);

        if (filter instanceof LogicalFilter) {
            filterString = serializeLogicalFilter((LogicalFilter) filter);
        } else {
            filterString = buildSingleFilter(filter, null);
        }

        return StringUtils.isNotBlank(filterString) ? filterString : null;
    }

    public void setColumnDescriptors(List<ColumnDescriptor> columnDescriptors) {
        this.columnDescriptors = columnDescriptors;
    }

    @Override
    public Object build(FilterParser.Operator operator, Object left, Object right) throws Exception {
        if (operator.isLogical())
            return buildLogicalFilter(operator, left, right);
        else
            // Assume column is on the left
            return handleSimpleOperations(operator, (FilterParser.ColumnIndex) left, (FilterParser.Constant) right);
    }

    @Override
    public Object build(FilterParser.Operator operator, Object operand) throws UnsupportedOperationException {
        if (operator.isLogical()) {
            return buildLogicalFilter(operator, operand);
        } else {
            if (operator == FilterParser.Operator.IS_NULL || operator == FilterParser.Operator.IS_NOT_NULL) {
                // Use null for the constant value of null comparison
                return handleSimpleOperations(operator, (FilterParser.ColumnIndex) operand, null);
            } else {
                throw new UnsupportedOperationException(String.format("Unsupported unary operation '%s'", operator));
            }
        }
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

    protected void serializeColumnName(StringBuilder result, FilterParser.Operator operation, DataType type, ColumnDescriptor filterColumn, String columnName) {
        result.append(columnName);
    }

    protected String getColumnName(ColumnDescriptor column) {
        return column.columnName();
    }

    protected abstract boolean canRightOperandBeOmitted(FilterParser.Operator logicalOperator);

    protected abstract boolean shouldIncludeFilter(String filterColumnName, FilterParser.Operator operator, FilterParser.Operator logicalOperation);

    protected abstract String serializeValue(Object val, DataType type);

    /**
     * Build filter string for a single filter and append to the filters string.
     *
     * @param filter           the filter
     * @param logicalOperation the logical operation
     * @return the filter string if successful, null otherwise
     */
    private String buildSingleFilter(Object filter, FilterParser.Operator logicalOperation) {

        // Let's look first at the filter
        BasicFilter bFilter = (BasicFilter) filter;

        // Extract column name and value
        int filterColumnIndex = bFilter.getColumn().index();
        ColumnDescriptor filterColumn = columnDescriptors.get(filterColumnIndex);
        String filterColumnName = getColumnName(filterColumn);
        FilterParser.Operator operation = bFilter.getOperation();
        DataType dataType = DataType.get(filterColumn.columnTypeCode());

        // if filter is determined to be not being able to be pushed down, but not violating logical correctness,
        // we just skip it and return null
        if (!shouldIncludeFilter(filterColumnName, operation, logicalOperation)) {
            return null;
        }

        StringBuilder result = new StringBuilder();

        serializeColumnName(result, operation, dataType, filterColumn, filterColumnName);

        if (dataType == DataType.BOOLEAN)
            return result.toString();

        if (supportedOperators.contains(operation)) {
            result
                    .append(" ")
                    .append(operation.getOperator());
        } else {
            return null;
        }

        boolean operationRequiresValue =
                operation != FilterParser.Operator.IS_NULL && operation != FilterParser.Operator.IS_NOT_NULL;

        if (operationRequiresValue) {

            // Insert constraint constant
            Object val = bFilter.getConstant().constant();
            if (val instanceof Iterable) {
                Iterable<?> iterable = (Iterable<?>) val;
                String listValue = StreamSupport.stream(iterable.spliterator(), false)
                        .map(s -> serializeValue(s, dataType))
                        .collect(Collectors.joining(","));
                result.append(" (").append(listValue).append(")");
            } else {
                result.append(" ").append(serializeValue(val, dataType));
            }
        }

        return result.toString();
    }

    private String serializeLogicalFilter(LogicalFilter filter) {

        if (!supportedLogicalOperators.contains(filter.getOperator())) {
            return null;
        }

        String logicalOperator = String.format(" %s ", filter.getOperator().getOperator());
        String outputFormat = filter.getOperator() == NOT ? "%s" : "(%s)";

        StringBuilder filterString = new StringBuilder();
        String serializedFilter;
        for (Object f : filter.getFilterList()) {
            if (f instanceof LogicalFilter) {
                serializedFilter = serializeLogicalFilter((LogicalFilter) f);
            } else {
                serializedFilter = buildSingleFilter(f, filter.getOperator());
            }

            if (serializedFilter == null) {
                // If the filter was skipped, a null is returned, so
                // we need to decided whether the operator can handle
                // the operation with the skipped filter
                if (!canRightOperandBeOmitted(filter.getOperator())) {
                    return null;
                }
                continue;
            }

            if (filter.getOperator() == NOT) {
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
        }

        return (filterString.length() > 0) ?
                String.format(outputFormat, filterString.toString()) : null;
    }

    /*
     * Handles simple column-operator-constant expressions.
     * Creates a special filter in the case the column is the row key column
     */
    private BasicFilter handleSimpleOperations(FilterParser.Operator operation,
                                               FilterParser.ColumnIndex column,
                                               FilterParser.Constant constant) {
        return new BasicFilter(operation, column, constant);
    }

    private LogicalFilter buildLogicalFilter(FilterParser.Operator operator, Object left, Object right) {
        return new LogicalFilter(operator, Arrays.asList(left, right));
    }

    private LogicalFilter buildLogicalFilter(FilterParser.Operator operator, Object filter) {
        return new LogicalFilter(operator, Collections.singletonList(filter));
    }
}
