package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.EnumSet;
import java.util.List;

/**
 * A tree pruner that prunes a tree based on the supported column data types.
 */
public class SupportedDataTypePruner extends BaseTreePruner {

    private final List<ColumnDescriptor> columnDescriptors;
    private final EnumSet<DataType> supportedDataTypes;

    /**
     * Constructor, assumes all possible predicate pushdown datatypes will be supported
     *
     * @param columnDescriptors  the list of column descriptors for the table
     */
    public SupportedDataTypePruner(List<ColumnDescriptor> columnDescriptors) {
        this(columnDescriptors, FilterParser.SUPPORTED_DATA_TYPES);
    }

    /**
     * Constructor
     *
     * @param columnDescriptors  the list of column descriptors for the table
     * @param supportedDataTypes the EnumSet of supported data types
     */
    public SupportedDataTypePruner(List<ColumnDescriptor> columnDescriptors,
                                   EnumSet<DataType> supportedDataTypes) {
        this.columnDescriptors = columnDescriptors;
        this.supportedDataTypes = supportedDataTypes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node visit(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            if (!operatorNode.getOperator().isLogical()) {
                DataType datatype = getColumnType(operatorNode);
                if (!supportedDataTypes.contains(datatype)) {
                    // prune the operator node if its operand is a column of unsupported type
                    LOG.debug("DataType oid={} is not supported", datatype.getOID());
                    return null;
                }
            }
        }
        return node;
    }

    /**
     * Returns the data type for the given column index
     *
     * @param operatorNode the operator node
     * @return the data type for the given column index
     */
    private DataType getColumnType(OperatorNode operatorNode) {
        return columnDescriptors.get(operatorNode.getColumnIndexOperand().index()).getDataType();
    }
}
