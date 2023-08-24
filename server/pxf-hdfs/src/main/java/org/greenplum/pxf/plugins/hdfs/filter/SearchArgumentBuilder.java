package org.greenplum.pxf.plugins.hdfs.filter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.filter.CollectionOperandNode;
import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.ScalarOperandNode;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class implements {@link TreeVisitor} and generates a
 * {@link SearchArgument.Builder} for the given filter string.
 * For example, for the filter string
 * ( _1_ < 5 OR _1_ > 10 ) AND ( _2_ IS NOT NULL )
 * it will generate the following {@link SearchArgument.Builder}
 * startAnd
 * ..startOr
 * ....lessThan
 * ....greaterThan
 * ..endOr
 * ..startNot
 * ....isNull
 * ..endNot
 * endAnd
 */
public class SearchArgumentBuilder implements TreeVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(SearchArgumentBuilder.class);

    private final SearchArgument.Builder filterBuilder;
    private final List<ColumnDescriptor> columnDescriptors;

    public SearchArgumentBuilder(List<ColumnDescriptor> tupleDescription, Configuration configuration) {
        this.filterBuilder = SearchArgumentFactory.newBuilder(configuration);
        this.columnDescriptors = tupleDescription;
    }

    @Override
    public Node before(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (operator.isLogical() || level == 0) {
                // AND / OR / NOT
                switch (operator) {
                    case OR:
                        filterBuilder.startOr();
                        break;
                    case NOT:
                        filterBuilder.startNot();
                        break;

                    default:
                        /*
                         * If there is only a single filter it will need special
                         * case logic to make sure to still wrap the filter in a
                         * startAnd() & end() block
                         */
                        filterBuilder.startAnd();
                        break;
                }
            }
        }
        return node;
    }

    @Override
    public Node visit(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (!operator.isLogical()) {
                buildArgument(operatorNode);
            }
        }
        return node;
    }

    @Override
    public Node after(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            if (operatorNode.getOperator().isLogical() || level == 0) {
                // AND / OR / NOT
                filterBuilder.end();
            }
        }
        return node;
    }

    public SearchArgument.Builder getFilterBuilder() {
        return filterBuilder;
    }

    /**
     * Builds a single argument
     *
     * @param operatorNode the operatorNode node
     * @return true if the argument is build, false otherwise
     */
    private boolean buildArgument(OperatorNode operatorNode) {

        Operator operator = operatorNode.getOperator();
        ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
        OperandNode valueOperandNode = operatorNode.getValueOperand();

        ColumnDescriptor filterColumn = columnDescriptors.get(columnIndexOperand.index());
        String filterColumnName = filterColumn.columnName();
        Object filterValue = null;

        // In Hive 1, boxing of values happened inside the builder
        // For Hive 2 libraries, we need to do it before passing values to
        // Hive jars

        if (valueOperandNode instanceof CollectionOperandNode) {
            CollectionOperandNode collectionOperand = (CollectionOperandNode) valueOperandNode;

            filterValue = collectionOperand
                    .getData()
                    .stream()
                    .map(data -> boxLiteral(convertDataType(collectionOperand.getDataType().getTypeElem(), data)))
                    .collect(Collectors.toList());
        } else if (valueOperandNode instanceof ScalarOperandNode) {
            ScalarOperandNode scalarOperand = (ScalarOperandNode) valueOperandNode;

            filterValue = convertDataType(scalarOperand);
            filterValue = boxLiteral(filterValue);
        }

        PredicateLeaf.Type predicateLeafType = PredicateLeaf.Type.STRING;

        if (filterValue != null) {
            predicateLeafType = getType(filterValue);
        }

        if (operator == Operator.NOOP) {
            // NOT boolean wraps a NOOP
            //       NOT
            //        |
            //       NOOP
            //        |
            //    ---------
            //   |         |
            //   4        true
            // that needs to be replaced with equals

            // also IN
            operator = Operator.EQUALS;
        }

        switch (operator) {
            case LESS_THAN:
                filterBuilder.lessThan(filterColumnName, predicateLeafType, filterValue);
                break;
            case GREATER_THAN:
                filterBuilder.startNot().lessThanEquals(filterColumnName, predicateLeafType, filterValue).end();
                break;
            case LESS_THAN_OR_EQUAL:
                filterBuilder.lessThanEquals(filterColumnName, predicateLeafType, filterValue);
                break;
            case GREATER_THAN_OR_EQUAL:
                filterBuilder.startNot().lessThan(filterColumnName, predicateLeafType, filterValue).end();
                break;
            case EQUALS:
                filterBuilder.equals(filterColumnName, predicateLeafType, filterValue);
                break;
            case NOT_EQUALS:
                filterBuilder.startNot().equals(filterColumnName, predicateLeafType, filterValue).end();
                break;
            case IS_NULL:
                filterBuilder.isNull(filterColumnName, predicateLeafType);
                break;
            case IS_NOT_NULL:
                filterBuilder.startNot().isNull(filterColumnName, predicateLeafType).end();
                break;
            case IN:
                if (filterValue instanceof Collection) {
                    @SuppressWarnings("unchecked")
                    Collection<Object> l = (Collection<Object>) filterValue;
                    filterBuilder.in(filterColumnName, predicateLeafType, l.toArray(new Object[0]));
                } else {
                    throw new IllegalArgumentException("filterValue should be instance of List for IN operation");
                }
                break;
            default: {
                LOG.debug("Filter push-down is not supported for {} operation.", operator);
                return false;
            }
        }
        return true;
    }

    /**
     * Get the type of the given expression node.
     *
     * @param literal the object
     * @return int, string, or float or null if we don't know the type
     */
    private PredicateLeaf.Type getType(Object literal) {
        if (literal instanceof Byte ||
                literal instanceof Short ||
                literal instanceof Integer ||
                literal instanceof Long) {
            return PredicateLeaf.Type.LONG;
        } else if (literal instanceof HiveChar ||
                literal instanceof HiveVarchar ||
                literal instanceof String) {
            return PredicateLeaf.Type.STRING;
        } else if (literal instanceof Float ||
                literal instanceof Double) {
            return PredicateLeaf.Type.FLOAT;
        } else if (literal instanceof Date) {
            return PredicateLeaf.Type.DATE;
        } else if (literal instanceof Timestamp) {
            return PredicateLeaf.Type.TIMESTAMP;
        } else if (literal instanceof HiveDecimal ||
                literal instanceof HiveDecimalWritable ||
                literal instanceof BigDecimal) {
            return PredicateLeaf.Type.DECIMAL;
        } else if (literal instanceof Boolean) {
            return PredicateLeaf.Type.BOOLEAN;
        } else if (literal instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> l = (List<Object>) literal;
            if (l.size() > 0)
                return getType(l.get(0));
        }
        throw new IllegalArgumentException(String.format("Unknown type for literal %s", literal));
    }

    private static Object boxLiteral(Object literal) {
        if (literal instanceof String ||
                literal instanceof Long ||
                literal instanceof Double ||
                literal instanceof Date ||
                literal instanceof Timestamp ||
                literal instanceof HiveDecimal ||
                literal instanceof HiveDecimalWritable ||
                literal instanceof BigDecimal ||
                literal instanceof Boolean) {
            return literal;
        } else if (literal instanceof HiveChar ||
                literal instanceof HiveVarchar) {
            return StringUtils.stripEnd(literal.toString(), null);
        } else if (literal instanceof Byte ||
                literal instanceof Short ||
                literal instanceof Integer) {
            return ((Number) literal).longValue();
        } else if (literal instanceof Float) {
            // to avoid change in precision when upcasting float to double
            // we convert the literal to string and parse it as double. (HIVE-8460)
            return Double.parseDouble(literal.toString());
        } else {
            throw new IllegalArgumentException("Unknown type for literal " +
                    literal);
        }
    }

    /**
     * Converts the scalar operand value to its original type
     *
     * @param scalarOperand the scalar operand
     * @return the scalar operand value to its original type
     */
    private Object convertDataType(ScalarOperandNode scalarOperand) {
        return convertDataType(scalarOperand.getDataType(), scalarOperand.getValue());
    }

    /**
     * Converts the string value to the given type
     *
     * @param dataType the data type
     * @param value    the value
     * @return the string value to the given type
     */
    private Object convertDataType(DataType dataType, String value) {
        try {
            switch (dataType) {
                case BIGINT:
                    return Long.parseLong(value);
                case INTEGER:
                case SMALLINT:
                    return Integer.parseInt(value);
                case REAL:
                    return Float.parseFloat(value);
                case NUMERIC:
                    HiveDecimalWritable decimalValue = new HiveDecimalWritable(value);
                    if (!decimalValue.isSet() || new BigDecimal(value).precision() > HiveDecimal.MAX_PRECISION) {
                        throw new NumberFormatException("Failed to set HiveDecimal value");
                    }
                    return decimalValue;
                case FLOAT8:
                    return Double.parseDouble(value);
                case TEXT:
                case VARCHAR:
                case BPCHAR:
                    return value;
                case BOOLEAN:
                    return Boolean.parseBoolean(value);
                case DATE:
                    return Date.valueOf(value);
                case TIMESTAMP:
                    return Timestamp.valueOf(value);
                case TIME:
                    return Time.valueOf(value);
                case BYTEA:
                    return value.getBytes();
                default:
                    throw new UnsupportedTypeException(String.format("DataType %s unsupported", dataType));
            }
        } catch (NumberFormatException nfe) {
            throw new IllegalStateException(String.format("failed to parse number data %s for type %s", value, dataType), nfe);
        }
    }

}
