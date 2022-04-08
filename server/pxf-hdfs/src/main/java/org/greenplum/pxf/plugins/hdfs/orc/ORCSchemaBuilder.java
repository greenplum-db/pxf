package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.orc.TypeDescription;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Class for building an ORC schema from a Greenplum table definition.
 */
public class ORCSchemaBuilder {

    public static final Logger LOG = LoggerFactory.getLogger(ORCSchemaBuilder.class);

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
        for (ColumnDescriptor columnDescriptor: columnDescriptors) {
            String columnName = columnDescriptor.columnName(); // TODO: what about quoted / case sensitive / with spaces ?
            TypeDescription orcType = orcTypeFromGreenplumType(columnDescriptor);
            LOG.debug("Mapped column {} of type {} to ORC type {}", columnName, columnDescriptor.getDataType().getOID(), orcType);
            writeSchema.addField(columnName, orcType);
        }
        LOG.debug("Built schema: {}", writeSchema);
        return writeSchema;
    }

    /**
     * Maps Greenplum type to ORC logical type, including handling for arrays.
     * @param columnDescriptor the Greenplum column descriptor
     * @return ORC logical type to use when storing the values of provided Greenplum type
     */
    private TypeDescription orcTypeFromGreenplumType(ColumnDescriptor columnDescriptor) {
        DataType dataType = columnDescriptor.getDataType();

        // Greenplum does not report dimensionality of arrays, so for auto-generated schema we assume only 1-dim arrays
        // are supported. Once we allow a user to provide a schema, this might change.
        DataType scalarDataType = dataType.isArrayType() ? dataType.getTypeElem() : dataType;
        TypeDescription typeDescription;
        switch (scalarDataType) {
            case BOOLEAN:
                typeDescription = TypeDescription.createBoolean();
                break;
            case BYTEA:
                typeDescription = TypeDescription.createBinary();
                break;
            case BIGINT:
                typeDescription = TypeDescription.createLong();
                break;
            case SMALLINT:
                typeDescription = TypeDescription.createShort();
                break;
            case INTEGER:
                typeDescription = TypeDescription.createInt();
                break;
            case TEXT:
            case UUID:  // TODO: will it load back from string to UUID ?
                typeDescription = TypeDescription.createString();
                break;
            case REAL:
                typeDescription = TypeDescription.createFloat();
                break;
            case FLOAT8:
                typeDescription = TypeDescription.createDouble();
                break;
            case BPCHAR:
                typeDescription = setMaxLength(TypeDescription.createChar(), columnDescriptor.columnTypeModifiers());
                break;
            case VARCHAR:
                typeDescription = setMaxLength(TypeDescription.createVarchar(), columnDescriptor.columnTypeModifiers());
                break;
            case DATE:
                typeDescription = TypeDescription.createDate();
                break;

                /* TODO: Does ORC supports time?
                case TIME:
                 */
            case TIMESTAMP:
                typeDescription = TypeDescription.createTimestamp();
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                typeDescription = TypeDescription.createTimestampInstant();
                break;
            case NUMERIC:
                typeDescription = setPrecisionAndScale(TypeDescription.createDecimal(), columnDescriptor.columnTypeModifiers());
                break;
            default:
                throw new PxfRuntimeException(String.format("Unsupported Greenplum type %d for column %s",
                        dataType.getOID(), columnDescriptor.columnName()));
        }
        // wrap a primitive ORC TypeDescription into an list if Greenplum type was an array
        return dataType.isArrayType() ? TypeDescription.createList(typeDescription) : typeDescription;
    }

    /**
     * Sets maximum length for CHAR / VARCHAR ORC types if a corresponding Greenplum column had a size modifier
     * @param typeDescription ORC type description
     * @param columnTypeModifiers Greenplum type modifiers
     * @return type description object with the specified max length, if any
     */
    private TypeDescription setMaxLength(TypeDescription typeDescription, Integer[] columnTypeModifiers) {
        Integer maxLength = ArrayUtils.isNotEmpty(columnTypeModifiers) ? columnTypeModifiers[0] : null;
        if (maxLength != null && maxLength > 0) {
            return typeDescription.withMaxLength(maxLength);
        }
        return typeDescription;
    }

    /**
     * Sets precision and scale for NUMERIC ORC type if a corresponding Greenplum column had modifiers
     * @param typeDescription ORC type description
     * @param columnTypeModifiers Greenplum type modifiers
     * @return type description object with the specified precision and scale, if any
     */
    private TypeDescription setPrecisionAndScale(TypeDescription typeDescription, Integer[] columnTypeModifiers) {
        // if precision is not defined in Greenplum for a column, columnTypeModifiers will be null
        if (ArrayUtils.isNotEmpty(columnTypeModifiers)) {
            Integer precision = columnTypeModifiers[0];
            if (precision != null) {
                // due to ORC code, can't set precision which is less than current scale, which is 10 by default
                // so need to set correct scale before setting precision, default scale to 0 if missing
                int scale = (columnTypeModifiers.length > 1 && columnTypeModifiers[1] != null) ? columnTypeModifiers[1] : 0;
                typeDescription = typeDescription.withScale(scale); // should be less than 38, default / max ORC precision
                typeDescription = typeDescription.withPrecision(precision);
            }
            // if precision was not sent, ORC defaults will be assumed
            // TODO: check at runtime that actual value fits into (38,10) if precision was not sent
        }
        return typeDescription;
    }
}
