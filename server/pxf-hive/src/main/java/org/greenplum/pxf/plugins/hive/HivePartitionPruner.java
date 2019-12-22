package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

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
    public Node visit(Node node, final int level) {
        if (node instanceof OperatorNode &&
                !canOperatorBePushedDown((OperatorNode) node)) {
            return null;
        }
        return super.visit(node, level);
    }

    /**
     * Returns true when the operatorNode is logical, or for simple operators
     * true when the column is a partitioned column, and push-down is enabled
     * for integral types or when the column is of string
     * <p>
     * Say P is a conforming predicate based on partition column and supported
     * comparison operatorNode NP is a non conforming predicate based on either a
     * non-partition column or an unsupported operatorNode.
     * <p>
     * The following rule will be used during filter pruning
     * P <op> P -> P <op> P (op can be any logical operatorNode)
     * P AND NP -> P
     * P OR NP -> null
     * NP <op> NP -> null
     *
     * @param operatorNode the operatorNode node
     * @return true when the filter is compatible, false otherwise
     */
    private boolean canOperatorBePushedDown(OperatorNode operatorNode) {
        Operator operator = operatorNode.getOperator();

        if (operator.isLogical()) {
            // Skip AND / OR
            return true;
        }

        ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
        ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndexOperand.index());
        String columnName = columnDescriptor.columnName();

        String colType = partitionKeys.get(columnName);
        boolean isPartitionColumn = colType != null;

        boolean isIntegralSupported =
                canPushDownIntegral &&
                        (operator == Operator.EQUALS || operator == Operator.NOT_EQUALS);

        boolean canPushDown = isPartitionColumn && (
                colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME) ||
                        isIntegralSupported && serdeConstants.IntegralTypes.contains(colType)
        );

        if (!canPushDown) {
            LOG.trace("Filter is on a non-partition column or on a partition column that is not supported for push-down, ignore this filter for column: {}", columnName);
        }
        return canPushDown;
    }
}
