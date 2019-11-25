package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.greenplum.pxf.api.filter.BaseTreePruner;
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
 * of integrals is enabled on the metastore.
 */
public class HiveTreePruner extends BaseTreePruner {

    private static final Logger LOG = LoggerFactory.getLogger(HiveTreePruner.class);

    private final boolean canPushDownIntegral;
    private final Map<String, String> partitionKeys;
    private final List<ColumnDescriptor> columnDescriptors;

    public HiveTreePruner(EnumSet<Operator> supportedOperators,
                          boolean canPushDownIntegral,
                          Map<String, String> partitionKeys,
                          List<ColumnDescriptor> columnDescriptors) {
        super(supportedOperators);
        this.canPushDownIntegral = canPushDownIntegral;
        this.partitionKeys = partitionKeys;
        this.columnDescriptors = columnDescriptors;
    }

    @Override
    public Node prune(Node node) {
        if (node instanceof OperatorNode &&
                !isFilterCompatible((OperatorNode) node)) {
            return null;
        }
        return super.prune(node);
    }

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

        LOG.trace("Filter is on a non-partition column or on a partition column that is not supported for pushdown, ignore this filter for column: {}", columnName);
        return canPushDown;
    }
}
