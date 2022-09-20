package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.commons.lang.StringUtils;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.plugins.hdfs.orc.OrcUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.greenplum.pxf.plugins.hdfs.ParquetResolver.TIMESTAMP_PATTERN;

/**
 * Utility methods for converting between Parquet types and Postgres types text format
 */
public class ParquetUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(OrcUtilities.class);

    private PgUtilities pgUtilities;

    /**
     * default constructor
     * @param pgUtilities
     */
    public ParquetUtilities(PgUtilities pgUtilities) {
        this.pgUtilities = pgUtilities;
    }

    /**
     *
     * @param val
     * @param schemaType
     * @return
     */
    public List<Object> parsePostgresArray(String val, Type schemaType) {
        LOG.trace("schema type={}, value={}", schemaType, val);

        if (val == null) {
            return null;
        }
        String[] splits = pgUtilities.splitArray(val);
        List<Object> data = new ArrayList<>(splits.length);
        for (String split : splits) {
            try {
                split = split.trim();
                data.add(decodeString(split, schemaType));
            } catch (NumberFormatException | PxfRuntimeException e) {
                String hint = createErrorHintFromValue(StringUtils.startsWith(split, "["), val);
                throw new PxfRuntimeException(String.format("Error parsing array element: %s was not of expected type %s", split, schemaType), hint, e);
            }
        }
        return data;
    }

    /**
     * parse a string base off the parquet primitive type
     * @param val a string representation of the value
     * @param schemaType parquet schema type
     * @return a java representation of the value for the given valType
     */
    private Object decodeString(String val, Type schemaType) {
        if (val == null || val.equals("null")) {
            return null;
        }
        PrimitiveType.PrimitiveTypeName primitiveTypeName = schemaType.asPrimitiveType().getPrimitiveTypeName();
        switch (primitiveTypeName) {
            case BINARY:
                if (schemaType.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                    return val;
                } else {
                    return pgUtilities.parseByteaLiteral(val);
                }
            case BOOLEAN:
                //parquet bool val is "true" or "false" but pgUtilities only accept "t" or "f"
                return pgUtilities.parseBoolLiteral(val.substring(0,1));
            case INT32:
                if (schemaType.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    return ParquetTypeConverter.getDaysFromEpochFromDateString(val);
                } else if (schemaType.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation &&
                        ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) schemaType.getLogicalTypeAnnotation()).getBitWidth() == 16) {
                    return Short.parseShort(val);
                } else {
                    return Integer.parseInt(val);
                }
            case INT64:
                return Long.parseLong(val);
            case DOUBLE:
                return Double.parseDouble(val);
            case FLOAT:
                return Float.parseFloat(val);
            case INT96:
                String timestamp = val;
                if (TIMESTAMP_PATTERN.matcher(timestamp).find()) {
                    // Note: this conversion convert type "timestamp with time zone" will lose timezone information
                    // while preserving the correct value. (as Parquet doesn't support timestamp with time zone.
                    return ParquetTypeConverter.getBinaryFromTimestampWithTimeZone(timestamp);
                } else {
                    return ParquetTypeConverter.getBinaryFromTimestamp(timestamp);
                }
            case FIXED_LEN_BYTE_ARRAY:
                return val;
            default:
                throw new PxfRuntimeException(String.format("type: %s is not supported", primitiveTypeName));
        }
    }

    /**
     *
     * @param isMultiDimensional
     * @param val
     * @return
     */
    private String createErrorHintFromValue(boolean isMultiDimensional, String val) {
        if (isMultiDimensional) {
            return "Column value \"" + val + "\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing ORC files.";
        } else {
            return "Unexpected state since PXF generated the ORC schema.";
        }
    }

}
