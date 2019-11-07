//package org.greenplum.pxf.plugins.hdfs;
//
//import org.apache.parquet.filter2.compat.FilterCompat;
//import org.apache.parquet.filter2.predicate.FilterPredicate;
//import org.apache.parquet.filter2.predicate.Operators;
//import org.greenplum.pxf.api.BasicFilter;
//import org.greenplum.pxf.api.FilterBuilder;
//import org.greenplum.pxf.api.FilterParser;
//import org.greenplum.pxf.api.LogicalFilter;
//import org.greenplum.pxf.api.io.DataType;
//import org.greenplum.pxf.api.utilities.ColumnDescriptor;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;
//
//import static org.apache.parquet.filter.ColumnPredicates.equalTo;
//import static org.apache.parquet.filter.ColumnRecordFilter.column;
//import static org.apache.parquet.filter2.predicate.FilterApi.*;
//import static org.greenplum.pxf.api.FilterParser.Operator.*;
//
//public class ParquetRecordFilterBuilder implements FilterBuilder {
//
//    protected Logger LOG = LoggerFactory.getLogger(this.getClass());
//
//    private final List<ColumnDescriptor> columnDescriptors;
//
//    public ParquetRecordFilterBuilder(List<ColumnDescriptor> columnDescriptors) {
//        this.columnDescriptors = columnDescriptors;
//    }
//
//    public FilterCompat.Filter buildRecordFilter(String filterString) throws Exception {
//        LOG.debug("Filter string input: {}", filterString);
//
//        Object filter = getFilterObject(filterString);
//
//        FilterCompat.Filter recordFilter;
//
//        if (filter instanceof LogicalFilter) {
//            recordFilter = serializeLogicalFilter((LogicalFilter) filter);
//        } else {
//            recordFilter = buildSingleFilter((BasicFilter) filter);
//        }
//
//        return recordFilter;
//    }
//
//    private FilterCompat.Filter serializeLogicalFilter(LogicalFilter filter) {
//
//        String logicalOperator = String.format(" %s ", filter.getOperator().getOperator());
//        String outputFormat = filter.getOperator() == NOT ? "%s" : "(%s)";
//
//        StringBuilder filterString = new StringBuilder();
//        FilterCompat.Filter recordFilter;
//        for (Object f : filter.getFilterList()) {
//            if (f instanceof LogicalFilter) {
//                recordFilter = serializeLogicalFilter((LogicalFilter) f);
//            } else {
//                recordFilter = buildSingleFilter((BasicFilter) f);
//            }
//
//            if (recordFilter == null) {
//                continue;
//            }
//
//            if (filter.getOperator() == NOT) {
//                filterString
//                        .append("NOT (")
//                        .append(recordFilter)
//                        .append(")");
//            } else {
//                // We only append the operator if there is something on the filterString
//                if (filterString.length() > 0) {
//                    filterString.append(logicalOperator);
//                }
//                filterString.append(recordFilter);
//            }
//        }
//
//        return (filterString.length() > 0) ?
//                String.format(outputFormat, filterString.toString()) : null;
//    }
//
//    /**
//     * Build FilterCompat.Filter for a single filter.
//     *
//     * @param bFilter the filter
//     * @return the FilterCompat.Filter object if successful, null otherwise
//     */
//    private FilterCompat.Filter buildSingleFilter(BasicFilter bFilter) {
//
//        // Extract column name and value
//        int filterColumnIndex = bFilter.getColumn().index();
//        ColumnDescriptor filterColumn = columnDescriptors.get(filterColumnIndex);
//        String filterColumnName = filterColumn.columnName();
//        FilterParser.Operator operation = bFilter.getOperation();
//        DataType dataType = DataType.get(filterColumn.columnTypeCode());
//
//        if (dataType == DataType.BOOLEAN)
//            return FilterCompat.get(column(filterColumnName, equalTo(true)));
//
//        if (operation == IS_NULL || operation == IS_NOT_NULL) {
//            return null;
//        }
//
//        Operators.Column column = intColumn(filterColumnName);
//
//        // Insert constraint constant
//        Object val = bFilter.getConstant().constant();
//
//        FilterCompat.Filter recordFilter;
//        switch (operation) {
//            case EQUALS:
//                recordFilter = FilterCompat.get(column(filterColumnName, equalTo(val));
//                break;
//            case LESS_THAN:
//                recordFilter = FilterCompat.get(foo(lt(), column, dataType, val));
//                break;
//            case GREATER_THAN:
//                recordFilter = FilterCompat.get(column(filterColumnName, gt(true)));
//                break;
//            case LESS_THAN_OR_EQUAL:
//                recordFilter = FilterCompat.get(column(filterColumnName, ltEq(true)));
//                break;
//            case GREATER_THAN_OR_EQUAL:
//                recordFilter = FilterCompat.get(column(filterColumnName, gtEq(true)));
//                break;
//            case NOT_EQUALS:
//                recordFilter = FilterCompat.get(column(filterColumnName, notEq(true)));
//                break;
//            case IN:
//                recordFilter = FilterCompat.get(column(filterColumnName, equalTo(true)));
//                break;
//            default:
//                return null;
//        }
//
//
////                LIKE
////                IS_NULL
////                IS_NOT_NULL
////                IN
//
//
////        switch (dataType) {
////            case SMALLINT:
////            case INTEGER:
////            case BIGINT:
////            case FLOAT8:
////            case REAL:
////            case BOOLEAN:
////                return val.toString();
////            case TEXT:
////                return String.format("'%s'",
////                        StringUtils.replace(val.toString(), "'", "''"));
////            case DATE:
////                // Date field has different format in different databases
////                return dbProduct.wrapDate(val);
////            case TIMESTAMP:
////                // Timestamp field has different format in different databases
////                return dbProduct.wrapTimestamp(val);
////            default:
////                throw new UnsupportedOperationException(String.format(
////                        "Unsupported column type for filtering '%s' ", dataType.getOID()));
////        }
////
////        if (val instanceof Iterable) {
////            Iterable<?> iterable = (Iterable<?>) val;
////            String listValue = StreamSupport.stream(iterable.spliterator(), false)
////                    .map(s -> serializeValue(s, dataType))
////                    .collect(Collectors.joining(","));
////            result.append(" (").append(listValue).append(")");
////        } else {
////            result.append(" ").append(serializeValue(val, dataType));
////        }
//
//        return recordFilter;
//    }
//
//    /**
//     * Translates a filterString into a {@link BasicFilter} or a
//     * list of such filters.
//     *
//     * @param filterString the string representation of the filter
//     * @return a single {@link BasicFilter}
//     * object or a {@link java.util.List} of
//     * {@link BasicFilter} objects.
//     * @throws Exception if parsing the filter failed or filter is not a basic
//     *                   filter or list of basic filters
//     */
//    public Object getFilterObject(String filterString) throws Exception {
//        if (filterString == null) return null;
//
//        FilterParser parser = new FilterParser(this);
//        Object result = parser.parse(filterString.getBytes(FilterParser.DEFAULT_CHARSET));
//
//        if (!(result instanceof LogicalFilter) && !(result instanceof BasicFilter)
//                && !(result instanceof List)) {
//            throw new Exception(String.format("String '%s' resolved to no filter", filterString));
//        }
//
//        return result;
//    }
//
//    @Override
//    public Object build(FilterParser.Operator operator, Object left, Object right) throws Exception {
//        if (operator.isLogical())
//            return buildLogicalFilter(operator, left, right);
//        else
//            // Assume column is on the left
//            return handleSimpleOperations(operator, (FilterParser.ColumnIndex) left, (FilterParser.Constant) right);
//    }
//
//    @Override
//    public Object build(FilterParser.Operator operator, Object operand) throws UnsupportedOperationException {
//        if (operator.isLogical()) {
//            return buildLogicalFilter(operator, operand);
//        } else {
//            if (operator == IS_NULL || operator == IS_NOT_NULL) {
//                // Use null for the constant value of null comparison
//                return handleSimpleOperations(operator, (FilterParser.ColumnIndex) operand, null);
//            } else {
//                throw new UnsupportedOperationException(String.format("Unsupported unary operation '%s'", operator));
//            }
//        }
//    }
//
//    /*
//     * Handles simple column-operator-constant expressions.
//     * Creates a special filter in the case the column is the row key column
//     */
//    private BasicFilter handleSimpleOperations(FilterParser.Operator operation,
//                                               FilterParser.ColumnIndex column,
//                                               FilterParser.Constant constant) {
//        return new BasicFilter(operation, column, constant);
//    }
//
//    private LogicalFilter buildLogicalFilter(FilterParser.Operator operator, Object left, Object right) {
//        return new LogicalFilter(operator, Arrays.asList(left, right));
//    }
//
//    private LogicalFilter buildLogicalFilter(FilterParser.Operator operator, Object filter) {
//        return new LogicalFilter(operator, Collections.singletonList(filter));
//    }
//}
