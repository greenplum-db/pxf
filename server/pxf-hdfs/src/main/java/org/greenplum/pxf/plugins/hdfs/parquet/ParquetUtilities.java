package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.commons.lang.StringUtils;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
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
     * @param primitiveTypeName
     * @return
     */
    public List<Object> parsePostgresArray(String val, PrimitiveType.PrimitiveTypeName primitiveTypeName) {
        LOG.trace("primivitve element type={}, value={}", primitiveTypeName, val);

        if (val == null) {
            return null;
        }
        val=val.replaceAll("\\s","");
        String[] splits = pgUtilities.splitArray(val);
        List<Object> data = new ArrayList<>(splits.length);
        for (String split : splits) {
            try {
                data.add(decodeString(split, primitiveTypeName));
            } catch (NumberFormatException | PxfRuntimeException e) {
                String hint = createErrorHintFromValue(StringUtils.startsWith(split, "["), val);
                throw new PxfRuntimeException(String.format("Error parsing array element: %s was not of expected type %s", split, primitiveTypeName), hint, e);
            }
        }
        return data;
    }

    /**
     * parse a string base off the parquet primitive type
     * @param val a string representation of the value
     * @param primitiveTypeName parquet primitive type
     * @return a java representation of the value for the given valType
     */
    private Object decodeString(String val, PrimitiveType.PrimitiveTypeName primitiveTypeName) {
        if (val == null || val.equals("null")) {
            return null;
        }
        switch (primitiveTypeName) {
            case BINARY:
                return pgUtilities.parseByteaLiteral(val);
            case BOOLEAN:
                return pgUtilities.parseBoolLiteral(val);
            case INT32:
                return Integer.parseInt(val);
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
