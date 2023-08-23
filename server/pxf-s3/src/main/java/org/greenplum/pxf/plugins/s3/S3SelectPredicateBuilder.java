package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.JdbcPredicateBuilder;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.util.List;

/**
 * Builds a predicate to query data on S3 Select. It supports querying data
 * using the column name or the column position.
 */
public class S3SelectPredicateBuilder extends JdbcPredicateBuilder {

    private final boolean usePositionToIdentifyColumn;

    /**
     * Constructor for S3 Select predicate builder
     *
     * @param usePositionToIdentifyColumn true if column position is used, false to use the column name
     * @param tupleDescription            the list of column descriptors
     */
    public S3SelectPredicateBuilder(boolean usePositionToIdentifyColumn,
                                    List<ColumnDescriptor> tupleDescription) {
        super(DbProduct.S3_SELECT, tupleDescription);
        this.usePositionToIdentifyColumn = usePositionToIdentifyColumn;
    }

    @Override
    protected String getNodeValue(OperandNode operandNode) {
        if (operandNode instanceof ColumnIndexOperandNode) {
            ColumnIndexOperandNode columnIndexOperand = (ColumnIndexOperandNode) operandNode;
            lastIndex = columnIndexOperand.index();
            ColumnDescriptor columnDescriptor = getColumnDescriptors().get(lastIndex);
            DataType type = columnDescriptor.getDataType();

            String columnName;
            String format = "%s";

            /*
             * Returns the column name. If we use the column position to
             * identify the column we return the index of the column as the
             * column name. Otherwise, we use the actual column name.
             */
            if (usePositionToIdentifyColumn) {
                columnName = String.format("%s._%d", S3SelectQueryBuilder.S3_TABLE_ALIAS, columnIndexOperand.index() + 1);
            } else {
                columnName = String.format("%s.\"%s\"", S3SelectQueryBuilder.S3_TABLE_ALIAS,
                        columnDescriptor.columnName());
            }

            switch (type) {
                case BIGINT:
                case INTEGER:
                case SMALLINT:
                    format = "CAST (%s AS int)";
                    break;
                case BOOLEAN:
                    format = "CAST (%s AS bool)";
                    break;
                case FLOAT8:
                    format = "CAST (%s AS float)";
                    break;
                case REAL:
                case NUMERIC:
                    format = "CAST (%s AS decimal)";
                    break;
                case TEXT:
                case VARCHAR:
                case BPCHAR:
                    break;
                case DATE:
                case TIMESTAMP:
                    format = "TO_TIMESTAMP(%s)";
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Unsupported column type for filtering '%s'", columnDescriptor.columnTypeName()));
            }

            return String.format(format, columnName);
        }

        return super.getNodeValue(operandNode);
    }

}
