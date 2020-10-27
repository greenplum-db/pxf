package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Prunes unsupported
 * {@link org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName}s from an
 * expression tree.
 */
public class ParquetOperatorPruner extends SupportedOperatorPruner {
    // INT96 and FIXED_LEN_BYTE_ARRAY cannot be pushed down
    // for more details look at
    // org.apache.parquet.filter2.dictionarylevel.DictionaryFilter#expandDictionary
    // where INT96 and FIXED_LEN_BYTE_ARRAY are not dictionary values
    private static final EnumSet<PrimitiveType.PrimitiveTypeName> SUPPORTED_PRIMITIVE_TYPES =
            EnumSet.of(
                    PrimitiveType.PrimitiveTypeName.INT32,
                    PrimitiveType.PrimitiveTypeName.INT64,
                    PrimitiveType.PrimitiveTypeName.BOOLEAN,
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    PrimitiveType.PrimitiveTypeName.FLOAT,
                    PrimitiveType.PrimitiveTypeName.DOUBLE);

    private final Map<String, Type> fields;
    private final List<ColumnDescriptor> columnDescriptors;

    /**
     * Constructor
     *
     * @param columnDescriptors  the list of column descriptors for the table
     * @param originalFields     a map of field names to types
     * @param supportedOperators the EnumSet of supported operators
     */
    public ParquetOperatorPruner(List<ColumnDescriptor> columnDescriptors,
                                 Map<String, Type> originalFields,
                                 EnumSet<Operator> supportedOperators) {
        super(supportedOperators);
        this.columnDescriptors = columnDescriptors;
        this.fields = originalFields;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node visit(Node node, int level) {

        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (!operator.isLogical()
                    && !SUPPORTED_PRIMITIVE_TYPES.contains(getPrimitiveType(operatorNode))) {
                return null;
            }
        }
        return super.visit(node, level);
    }

    /**
     * Returns the parquet primitive type for the given column index
     *
     * @param operatorNode the operator node
     * @return the parquet primitive type for the given column index
     */
    private PrimitiveType.PrimitiveTypeName getPrimitiveType(OperatorNode operatorNode) {
        ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
        ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndexOperand.index());
        String filterColumnName = columnDescriptor.columnName();
        Type type = fields.get(filterColumnName);
        return type.asPrimitiveType().getPrimitiveTypeName();
    }
}
