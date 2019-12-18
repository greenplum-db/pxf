package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A class that generates predicates with the provided column names
 */
public class ColumnPredicateBuilder extends ToStringTreeVisitor {

    private final String quoteString;
    private final List<ColumnDescriptor> columnDescriptors;

    /**
     * Stores the index of the last processed column
     */
    protected int lastIndex;

    public ColumnPredicateBuilder(List<ColumnDescriptor> columnDescriptors) {
        this("", columnDescriptors);
    }

    public ColumnPredicateBuilder(String quoteString,
                                  List<ColumnDescriptor> columnDescriptors) {
        this.quoteString = quoteString;
        this.columnDescriptors = columnDescriptors;
    }

    @Override
    protected String getNodeValue(Operand operand) {
        if (operand instanceof ColumnIndexOperand) {
            ColumnIndexOperand columnIndexOperand = (ColumnIndexOperand) operand;

            /* We need the column index (column is guaranteed to be on the left,
             * so it always comes first. The column index is needed to get the
             * column type. The column type information is required to determine
             * how the value will be processed
             */
            lastIndex = columnIndexOperand.index();
            ColumnDescriptor columnDescriptor = columnDescriptors.get(lastIndex);
            return String.format("%s%s%s", quoteString, columnDescriptor.columnName(), quoteString);
        }

        // Obtain the datatype of the column
        ColumnDescriptor columnDescriptor = columnDescriptors.get(lastIndex);
        DataType type = DataType.get(columnDescriptor.columnTypeCode());

        if (operand instanceof CollectionOperand) {
            CollectionOperand collectionOperand = (CollectionOperand) operand;
            String listValue = collectionOperand.getData().stream()
                    .map(s -> serializeValue(type, s))
                    .collect(Collectors.joining(","));
            return String.format("(%s)", listValue);
        } else {
            String value = super.getNodeValue(operand);
            return serializeValue(type, value);
        }
    }

    /**
     * Serializes the value
     *
     * @param type  the value type
     * @param value the value
     * @return the serialized value
     */
    protected String serializeValue(DataType type, String value) {
        return value;
    }

    /**
     * Returns the list of {@link ColumnDescriptor}
     *
     * @return the list of {@link ColumnDescriptor}
     */
    protected List<ColumnDescriptor> getColumnDescriptors() {
        return columnDescriptors;
    }
}
