package org.greenplum.pxf.plugins.hbase;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.greenplum.pxf.api.filter.ColumnIndexOperand;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operand;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseDoubleComparator;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseFloatComparator;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseIntegerComparator;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseTupleDescription;

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.greenplum.pxf.api.io.DataType.TEXT;

/**
 * This is the implementation of {@code TreeVisitor} for HBase.
 * <p>
 * The class visits all the {@code Node}s from the expression tree,
 * and builds a simple (single {@link Filter} class) or a compound
 * ({@link FilterList}) for {@link HBaseAccessor} to use for its scan.
 * <p>
 * This is done before the scan starts. It is not a scan time operation.
 * <p>
 * HBase row key column is a special case.
 * If the user defined row key column as TEXT and used {@code <,>,<=,>=,=}
 * operators, the startKey ({@code >/>=}) and the endKey ({@code </<=}) are
 * stored in addition to the created filter.
 * <p>
 * This is an addition on top of regular filters and does not replace
 * any logic in HBase filter objects.
 */
public class HBaseFilterBuilder implements TreeVisitor {

    private static final Map<Operator, CompareFilter.CompareOp> OPERATORS_MAP =
            Collections.unmodifiableMap(new HashMap<Operator, CompareFilter.CompareOp>() {{
                put(Operator.LESS_THAN, CompareFilter.CompareOp.LESS); // "<"
                put(Operator.GREATER_THAN, CompareFilter.CompareOp.GREATER); // ">"
                put(Operator.LESS_THAN_OR_EQUAL, CompareFilter.CompareOp.LESS_OR_EQUAL); // "<="
                put(Operator.GREATER_THAN_OR_EQUAL, CompareFilter.CompareOp.GREATER_OR_EQUAL); // ">="
                put(Operator.EQUALS, CompareFilter.CompareOp.EQUAL); // "="
                put(Operator.NOT_EQUALS, CompareFilter.CompareOp.NOT_EQUAL); // "!="
            }});
    private static final Map<Operator, FilterList.Operator> LOGICAL_OPERATORS_MAP =
            Collections.unmodifiableMap(new HashMap<Operator, FilterList.Operator>() {{
                put(Operator.AND, FilterList.Operator.MUST_PASS_ALL);
                put(Operator.OR, FilterList.Operator.MUST_PASS_ONE);
            }});

    private boolean endKeyAssigned = false;
    private boolean startKeyAssigned = false;
    private byte[] endKey;
    private byte[] startKey;
    private Deque<Filter> filterQueue;
    private Filter currentFilter;
    private HBaseTupleDescription tupleDescription;

    public HBaseFilterBuilder(HBaseTupleDescription tupleDescription) {
        this.filterQueue = new LinkedList<>();
        this.tupleDescription = tupleDescription;
        this.startKey = HConstants.EMPTY_START_ROW;
        this.endKey = HConstants.EMPTY_END_ROW;
    }

    @Override
    public Node before(Node node) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (operator.isLogical()) {
                Filter filter = new FilterList(LOGICAL_OPERATORS_MAP.get(operator));
                processFilter(filter);
                filterQueue.push(filter);
            }
        }
        return node;
    }

    @Override
    public Node visit(Node node) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            if (!operator.isLogical()) {
                ColumnIndexOperand columnIndexOperand = operatorNode.getColumnIndexOperand();
                HBaseColumnDescriptor hBaseColumn = tupleDescription.getColumn(columnIndexOperand.index());
                Filter filter;

                if (operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL) {
                    filter = processNullOperator(hBaseColumn, operator);
                } else {
                    Operand data = operatorNode.getOperand().orElse(null);
                    filter = processSimpleColumnOperator(hBaseColumn, operator, data);
                }

                processFilter(filter);
            }
        }
        return node;
    }

    @Override
    public Node after(Node node) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (operator.isLogical()) {
                currentFilter = filterQueue.poll();
            }
        }
        return node;
    }

    /**
     * Returns the HBase {@link Filter} object after visiting the expression tree.
     *
     * @return filter object
     */
    public Filter buildFilter() {
        if (!filterQueue.isEmpty()) {
            throw new IllegalStateException("Filter queue is not empty after visiting all nodes");
        }
        return currentFilter;
    }

    /**
     * Returns the startKey for scanning the HBase table.
     * If the user specified a {@code > / >=} operation
     * on a textual row key column, this value will be returned.
     * Otherwise, the start of table.
     *
     * @return start key for scanning HBase table
     */
    public byte[] getStartKey() {
        return startKey;
    }

    /**
     * Returns the endKey for scanning the HBase table.
     * If the user specified a {@code < / <=} operation
     * on a textual row key column, this value will be returned.
     * Otherwise, the end of table.
     *
     * @return end key for scanning HBase table
     */
    public byte[] getEndKey() {
        return endKey;
    }

    /**
     * If the result is of type {@link FilterList}, add the {@link Filter} to
     * the filter list. Otherwise, set it as the resulting {@link Filter}
     *
     * @param filter the filter to process
     */
    private void processFilter(Filter filter) {
        Filter head = filterQueue.peek();
        if (head instanceof FilterList) {
            /*
             * Handles operation between already calculated expressions.
             * Currently only {@code AND}, in the future {@code OR} can be added.
             * Four cases here:
             *  - Both are simple filters.
             *  - Left is a FilterList and right is a filter.
             *  - Left is a filter and right is a FilterList.
             *  - Both are FilterLists.
             */
            ((FilterList) head).addFilter(filter);
        }
        currentFilter = filter;
    }

    /**
     * Handles simple column-operator-constant expressions.
     * Creates a special filter in the case the column is the row key column.
     *
     * @param hBaseColumn the HBase column
     * @param operator    the simple column operator
     * @param data        the optional operand
     * @return the {@link Filter} for the given simple column operator
     */
    private Filter processSimpleColumnOperator(HBaseColumnDescriptor hBaseColumn, Operator operator, Operand data) {
        // The value of lastOperand has to be stored after visiting
        // the operand child of this node.
        ByteArrayComparable comparator = getComparator(
                hBaseColumn.columnTypeCode(),
                data);

        /*
         * If row key is of type TEXT, allow filter in start/stop row
         * key API in HBaseAccessor/Scan object.
         */
        if (data != null && textualRowKey(hBaseColumn)) {
            storeStartEndKeys(operator, data.toString());
        }

        if (hBaseColumn.isKeyColumn()) {
            // Special filter for row key column
            return new RowFilter(
                    OPERATORS_MAP.get(operator),
                    comparator);
        } else {
            return new SingleColumnValueFilter(
                    hBaseColumn.columnFamilyBytes(),
                    hBaseColumn.qualifierBytes(),
                    OPERATORS_MAP.get(operator),
                    comparator);
        }
    }

    /**
     * Handles IS NULL and IS NOT NULL operators
     *
     * @param hBaseColumn the HBase column
     * @param operator    the IS NULL/IS NOT NULL operator
     * @return the filter for the given operator
     */
    private Filter processNullOperator(HBaseColumnDescriptor hBaseColumn, Operator operator) {
        CompareFilter.CompareOp compareOperation = (operator == Operator.IS_NULL) ?
                CompareFilter.CompareOp.EQUAL :
                CompareFilter.CompareOp.NOT_EQUAL;
        return new SingleColumnValueFilter(
                hBaseColumn.columnFamilyBytes(),
                hBaseColumn.qualifierBytes(),
                compareOperation,
                new NullComparator());
    }

    /**
     * Returns true if column is of type TEXT and is a row key column.
     */
    private boolean textualRowKey(HBaseColumnDescriptor column) {
        return column.isKeyColumn() && column.columnTypeCode() == TEXT.getOID();
    }

    /**
     * Resolves the column's type to a comparator class to be used.
     * Currently, supported types are TEXT and INTEGER types.
     */
    private ByteArrayComparable getComparator(int type, Operand data) {
        ByteArrayComparable result;
        switch (DataType.get(type)) {
            case TEXT:
                result = new BinaryComparator(Bytes.toBytes(data.toString()));
                break;
            case SMALLINT:
            case INTEGER:
                result = new HBaseIntegerComparator(Long.parseLong(data.toString()));
                break;
            case BIGINT:
            case REAL:

                // Check the operand's original data type
                switch (data.getDataType()) {
                    case BIGINT:
                    case INTEGER:
                    case SMALLINT:
                        result = new HBaseIntegerComparator(Long.parseLong(data.toString()));
                        break;
                    case REAL:
                        result = new HBaseFloatComparator(Float.parseFloat(data.toString()));
                        break;
                    case NUMERIC:
                    case FLOAT8:
                        result = new HBaseDoubleComparator(Double.parseDouble(data.toString()));
                        break;
                    default:
                        result = null;
                        break;
                }
                break;

            case FLOAT8:
                result = new HBaseDoubleComparator(Double.parseDouble(data.toString()));
                break;
            default:
                throw new IllegalArgumentException(String.format("unsupported column type for filtering %d", type));
        }

        return result;
    }

    /**
     * Sets startKey/endKey and their inclusiveness
     * according to the operation op.
     */
    private void storeStartEndKeys(Operator op, String data) {

        // Adding a zero byte to endKey, makes it inclusive
        // Adding a zero byte to startKey, makes it exclusive
        byte[] zeroByte = new byte[1];

        switch (op) {
            case LESS_THAN:
                setEndKey(Bytes.toBytes(data));
                break;
            case GREATER_THAN:
                setStartKey(Bytes.add(Bytes.toBytes(data), zeroByte));
                break;
            case LESS_THAN_OR_EQUAL:
                setEndKey(Bytes.add(Bytes.toBytes(data), zeroByte));
                break;
            case GREATER_THAN_OR_EQUAL:
                setStartKey(Bytes.toBytes(data));
                break;
            case EQUALS:
                setStartKey(Bytes.toBytes(data));
                setEndKey(Bytes.add(Bytes.toBytes(data), zeroByte));
                break;
        }
    }

    /**
     * Sets the start key. Only allows for setting the start key during the
     * first call. Subsequent calls trying to set the start key will be ignored
     *
     * @param startKey the start key
     */
    private void setStartKey(byte[] startKey) {
        if (startKeyAssigned) return;
        this.startKey = startKey;
        this.startKeyAssigned = true;
    }

    /**
     * Sets the end key. Only allows for setting the end key during the
     * first call. Subsequent calls trying to set the end key will be ignored
     *
     * @param endKey the end key
     */
    private void setEndKey(byte[] endKey) {
        if (endKeyAssigned) return;
        this.endKey = endKey;
        this.endKeyAssigned = true;
    }
}
