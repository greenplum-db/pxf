package org.greenplum.pxf.plugins.hdfs.orc;

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
            // TODO: check that deleted columns do not come in
            // if (!columnDescriptor.isProjected()) continue;
            String columnName = columnDescriptor.columnName(); // TODO: what about quoted / case sensitive / with spaces ?
            DataType dataType = columnDescriptor.getDataType();
            TypeDescription orcType = dataType.isArrayType() ?
                    TypeDescription.createList(orcTypeFromGreenplumType(dataType.getTypeElem())) : orcTypeFromGreenplumType(dataType);
            // TODO: can we get a multi-dimensional array in GP type description ?
            writeSchema.addField(columnName, orcType);
        }
        return writeSchema;
    }

    /**
     * Maps Greenplum primitive type to ORC primitive type.
     * @param dataType Greenplum type
     * @return ORC type
     */
    private TypeDescription orcTypeFromGreenplumType(DataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return TypeDescription.createBoolean();
            case BYTEA:
                return TypeDescription.createBinary();
            case BIGINT:
                return TypeDescription.createLong();
            case SMALLINT:
                return TypeDescription.createShort();
            case INTEGER:
                return TypeDescription.createInt();
            case TEXT:
            case UUID:  // TODO: will it load back from string to UUID ?
                return TypeDescription.createString();
            case REAL:
                return TypeDescription.createFloat();
            case FLOAT8:
                return TypeDescription.createDouble();
            case BPCHAR:
                return TypeDescription.createChar(); // TODO: handle maxLength
            case VARCHAR:
                return TypeDescription.createVarchar(); // TODO: handle maxLength
            case DATE:
                return TypeDescription.createDate();

                /* TODO:
                case TIME:
                 */
            case TIMESTAMP:
                return TypeDescription.createTimestamp();
            case TIMESTAMP_WITH_TIME_ZONE:
                return TypeDescription.createTimestampInstant();
            case NUMERIC:
                return TypeDescription.createDecimal(); // TODO: handle precision and scale

/*
                    INT2ARRAY(1005),
                    INT4ARRAY(1007),
                    INT8ARRAY(1016),
                    BOOLARRAY(1000),
                    TEXTARRAY(1009),
                    FLOAT4ARRAY(1021),
                    FLOAT8ARRAY(1022),
                    BYTEAARRAY(1001),
                    BPCHARARRAY(1014),
                    VARCHARARRAY(1015),
                    DATEARRAY(1182),
                    UUIDARRAY(2951),
                    NUMERICARRAY(1231),
                    TIMEARRAY(1183),
                    TIMESTAMPARRAY(1115),
                    TIMESTAMP_WITH_TIMEZONE_ARRAY(1185),
*/
            default:
                throw new PxfRuntimeException("Unsupported Greenplum type: " + dataType);
        }
    }
}
