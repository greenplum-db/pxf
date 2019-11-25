package org.greenplum.pxf.plugins.s3;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.filter.ColumnIndexOperand;
import org.greenplum.pxf.api.filter.Operand;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.JdbcTreeVisitor;

import java.util.List;

public class S3SelectTreeVisitor extends JdbcTreeVisitor {

    private final boolean usePositionToIdentifyColumn;

    public S3SelectTreeVisitor(boolean usePositionToIdentifyColumn, List<ColumnDescriptor> tupleDescription) {
        super(tupleDescription);
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
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT8:
            case REAL:
            case BOOLEAN:
                return value;
            case TEXT:
            case VARCHAR:
            case BPCHAR:
                return "'" + StringUtils.replace(value, "'", "''") + "'";
            case DATE:
            case TIMESTAMP:
                return "TO_TIMESTAMP('" + value + "')";
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported column type for filtering '%s' ", type.getOID()));
        }
    }
}
