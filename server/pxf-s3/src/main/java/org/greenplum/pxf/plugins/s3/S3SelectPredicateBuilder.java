package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.filter.ColumnIndexOperand;
import org.greenplum.pxf.api.filter.Operand;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.JdbcPredicateBuilder;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.util.List;

public class S3SelectPredicateBuilder extends JdbcPredicateBuilder {

    private final boolean usePositionToIdentifyColumn;

    public S3SelectPredicateBuilder(boolean usePositionToIdentifyColumn, List<ColumnDescriptor> tupleDescription) {
        super(DbProduct.S3_SELECT, tupleDescription);
        this.usePositionToIdentifyColumn = usePositionToIdentifyColumn;
    }

    @Override
    protected String getNodeValue(Operand operand) {
        if (operand instanceof ColumnIndexOperand) {
            ColumnIndexOperand columnIndexOperand = (ColumnIndexOperand) operand;
            lastIndex = columnIndexOperand.index();
            ColumnDescriptor columnDescriptor = getColumnDescriptors().get(lastIndex);
            DataType type = DataType.get(columnDescriptor.columnTypeCode());

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

        return super.getNodeValue(operand);
    }

    @Override
    protected String serializeValue(DataType type, String value) {
        switch (type) {
            case VARCHAR:
            case BPCHAR:
                type = DataType.TEXT;
                break;
        }
        return super.serializeValue(type, value);
    }
}
