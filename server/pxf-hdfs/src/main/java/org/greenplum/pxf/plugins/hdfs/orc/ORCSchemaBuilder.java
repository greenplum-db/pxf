package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.orc.TypeDescription;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.List;

/**
 * Class for building an ORC schema from a Greenplum table definition.
 */
public class ORCSchemaBuilder {

    /**
     * Builds an ORC schema from Greenplum table description
     * @param columnDescriptors list of column descriptors of a Greenplum table
     * @return an ORC schema
     */
    public TypeDescription buildSchema(List<ColumnDescriptor> columnDescriptors) {
        if (columnDescriptors == null) {
            return null;
        }
        // top level will always be a struct to align with how Hive would expect it
        TypeDescription writeSchema = TypeDescription.createStruct();
        for (int i = 0; i < columnDescriptors.size(); i++) {
            ColumnDescriptor columnDescriptor = columnDescriptors.get(i);

            String columnName = columnDescriptor.columnName(); // TODO: what about quoted / case sensitive / with spaces ?
            // columnName = StringEscapeUtils.escapeJava(columnName);

            TypeDescription orcType = orcTypeFromGreenplumType(columnDescriptor);
            // TODO: can we get a multi-dimensional array in GP type description ?
            writeSchema.addField(columnName, orcType);
        }
        return writeSchema;
    }

    /**
     * Maps Greenplum primitive type to ORC primitive type.
     * @param columnDescriptor
     * @return
     */
    private TypeDescription orcTypeFromGreenplumType(ColumnDescriptor columnDescriptor) {

        DataType dataType = columnDescriptor.getDataType();

        TypeDescription orcCategory = null;
        Integer[] columnTypeModifiers = columnDescriptor.columnTypeModifiers();

        switch (dataType) {
            case BOOLEAN:
                orcCategory = TypeDescription.createBoolean();
                break;
            case BYTEA:
                orcCategory = TypeDescription.createBinary();
                break;
            case BIGINT:
                orcCategory = TypeDescription.createLong();
                break;
            case SMALLINT:
                orcCategory = TypeDescription.createShort();
                break;
            case INTEGER:
                orcCategory = TypeDescription.createInt();
                break;
            case TEXT:
            case UUID:  // TODO: will it load back from string to UUID ?
                orcCategory = TypeDescription.createString();
                break;
            case REAL:
                orcCategory = TypeDescription.createFloat();
                break;
            case FLOAT8:
                orcCategory = TypeDescription.createDouble();
                break;
            case BPCHAR:
                orcCategory = TypeDescription.createChar();
                Integer maxLength = null;
                if(columnTypeModifiers != null && columnTypeModifiers.length > 0) {
                    maxLength = columnTypeModifiers[0];
                }
                if(maxLength != null && maxLength > 0) {
                    orcCategory = TypeDescription.createChar().withMaxLength(maxLength);
                }
                break;
            case VARCHAR:
                orcCategory = TypeDescription.createVarchar();
                maxLength = null;
                if(columnTypeModifiers != null && columnTypeModifiers.length > 0) {
                    maxLength = columnTypeModifiers[0];
                 }
                if(maxLength != null && maxLength > 0) {
                    orcCategory = TypeDescription.createVarchar().withMaxLength(maxLength);
                }
                break;
            case DATE:
                orcCategory = TypeDescription.createDate();
                break;

                /* TODO: Does ORC supports time?
                case TIME:
                 */
            case TIMESTAMP:
                orcCategory = TypeDescription.createTimestamp();
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                orcCategory = TypeDescription.createTimestampInstant();
                break;
            case NUMERIC:
                orcCategory = TypeDescription.createDecimal();

                Integer precision = null, scale =null;
                // In case of no precision, columnTypeModifiers will be null
                // If there is no scale in GPDB column then scale comes as 0

                if(columnTypeModifiers != null && columnTypeModifiers.length > 1) {
                    precision = columnTypeModifiers[0];
                    scale = columnTypeModifiers[1];
                }
                if(precision != null) {
                    orcCategory = orcCategory.withPrecision(precision);
                }
                if(scale != null) {
                    orcCategory = orcCategory.withScale(scale);
                }
                break;
            default:
                throw new PxfRuntimeException("Unsupported Greenplum type: " + dataType);
        }

        return dataType.isArrayType() ? TypeDescription.createList(orcCategory) : orcCategory;
    }
}
