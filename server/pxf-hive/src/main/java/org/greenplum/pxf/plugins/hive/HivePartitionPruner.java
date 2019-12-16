package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.ColumnIndexOperand;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Prune the tree based on partition keys and whether or not pushing down
 * of integrals is enabled on the MetaStore.
 */
public class HivePartitionPruner extends SupportedOperatorPruner {

    private static final Logger LOG = LoggerFactory.getLogger(HivePartitionPruner.class);

    private final boolean canPushDownIntegral;
    private final Map<String, String> partitionKeys;
    private final List<ColumnDescriptor> columnDescriptors;

    public HivePartitionPruner(EnumSet<Operator> supportedOperators,
                               boolean canPushDownIntegral,
                               Map<String, String> partitionKeys,
                               List<ColumnDescriptor> columnDescriptors) {
        super(supportedOperators);
        this.canPushDownIntegral = canPushDownIntegral;
        this.partitionKeys = partitionKeys;
        this.columnDescriptors = columnDescriptors;
    }

    @Override
    public Node visit(Node node) {
        if (node instanceof OperatorNode &&
                !isFilterCompatible((OperatorNode) node)) {
            return null;
        }
        return super.visit(node);
    }

    /**
     * Returns true when the operator is logical, or for simple operators
     * true when the column is a partitioned column, and push-down is enabled
     * for integral types the column type is of string type
     *
     * @param operatorNode the operator node
     * @return true when the filter is compatible, false otherwise
     */
    private boolean isFilterCompatible(OperatorNode operatorNode) {
        Operator operation = operatorNode.getOperator();

        if (operation.isLogical()) {
            // Skip AND / OR
            return true;
        }

        Optional<ColumnIndexOperand> columnIndexOperand = operatorNode.getChildren().stream()
                .filter(child -> child instanceof ColumnIndexOperand)
                .map(child -> (ColumnIndexOperand) child)
                .findFirst();

        if (!columnIndexOperand.isPresent()) {
            throw new RuntimeException("Unable to find the column index operand");
        }

        ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndexOperand.get().index());
        String columnName = columnDescriptor.columnName();

        String colType = partitionKeys.get(columnName);
        boolean isPartitionColumn = colType != null;

        boolean isIntegralSupported =
                canPushDownIntegral &&
                        (operation == Operator.EQUALS || operation == Operator.NOT_EQUALS);

        boolean canPushDown = isPartitionColumn && (
                colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME) ||
                        isIntegralSupported && serdeConstants.IntegralTypes.contains(colType)
        );

        LOG.trace("Filter is on a non-partition column or on a partition column that is not supported for push-down, ignore this filter for column: {}", columnName);
        return canPushDown;
    }
}
