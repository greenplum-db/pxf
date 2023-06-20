package org.greenplum.pxf.plugins.json;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Utility methods for dealing with Json data mappings.
 */
@Component
public final class JsonUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(JsonUtilities.class);

    // primitive types that are text and are allowed to have "{" as the first character of their value
    // multi-dimensionality of arrays of these types cannot be easily determined
    private static final EnumSet<DataType> TEXT_TYPES = EnumSet.of(DataType.BPCHAR, DataType.VARCHAR, DataType.TEXT);

    // primitive types that are serialized into JSON as Strings
    private static final EnumSet<DataType> STRING_TYPES = EnumSet.of(
            DataType.BPCHAR, DataType.VARCHAR, DataType.TEXT, DataType.DATE, DataType.TIME, DataType.TIMESTAMP,
            DataType.TIMESTAMP_WITH_TIME_ZONE, DataType.UUID);

    private PgUtilities pgUtilities;

    @Autowired
    public void setPgUtilities(PgUtilities pgUtilities) {
        this.pgUtilities = pgUtilities;
    }

    /**
     * Given a OneField object that represents a column in a row and a Column Descriptor describing the column,
     * uses provided Json Generator to write the column and its value as Json.
     * @param jsonGenerator Json generator
     * @param columnDescriptor column descriptor
     * @param field files with the value
     * @throws IOException if writing fails
     */
    public void writeField(JsonGenerator jsonGenerator, ColumnDescriptor columnDescriptor, OneField field) throws IOException {
        jsonGenerator.writeFieldName(columnDescriptor.columnName());
        writeValue(jsonGenerator, columnDescriptor.getDataType(), field.val);
    }

    /**
     * Decodes and writes the provided value of the provided column data type as Json using the provided Json generator.
     * Can handle single-dimensional arrays by parsing the serialized array value into decoded elements and then
     * recursively invoking itself for each array element.
     * @param jsonGenerator Json generator
     * @param dataType Greenplum data type of the value
     * @param value value to decode and write (can be a primitive or an array)
     * @throws IOException if writing fails
     */
    private void writeValue(JsonGenerator jsonGenerator, DataType dataType, Object value) throws IOException {
        if (value == null) {
            jsonGenerator.writeNull();
            return;
        }
        // first check if we are dealing with arrays
        if (dataType.isArrayType()) {
            jsonGenerator.writeStartArray();
            DataType elementDataType = dataType.getTypeElem();
            // split array into elements and write the value for each element
            List<Object> elementValues = parsePostgresArray((String) value, elementDataType);
            for (Object elementValue : elementValues) {
                writeValue(jsonGenerator, elementDataType, elementValue);
            }
            jsonGenerator.writeEndArray();
            return;
        }

        // if the value is a String, as is the case with array elements of many otherwise numerical types
        // it should be already decoded with any GP-specific escaping removed
        // so that it can be written out with jsonGenerator that will apply escaping per Json rules
        if (value instanceof String) {
            if (STRING_TYPES.contains(dataType)) {
                jsonGenerator.writeString((String) value); // need quotes around the value and escaping
            } else {
                jsonGenerator.writeRawValue((String) value); // numeric, does not need quotes / escaping
            }
            return;
        }

        // write different types based on the field type
        switch (dataType) {
            case BYTEA:
                if (value instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) value;
                    jsonGenerator.writeBinary(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
                } else {
                    jsonGenerator.writeBinary((byte[]) value);
                }
                break;
            case BOOLEAN:
                jsonGenerator.writeBoolean((boolean) value);
                break;
            case SMALLINT:
                jsonGenerator.writeNumber((short) value);
                break;
            case INTEGER:
                jsonGenerator.writeNumber((int) value);
                break;
            case BIGINT:
                jsonGenerator.writeNumber((long) value);
                break;
            case REAL:
                jsonGenerator.writeNumber((float) value);
                break;
            case FLOAT8:
                jsonGenerator.writeNumber((double) value);
                break;
            // TODO: maybe later use ISO8601 timestamp representation (2012-04-23T18:25:43.511Z) but for now use GP format
            default:
                // everything else is represented as a String and should have been already handled before this switch
                // but just in case, it will be converted to string and written out
                jsonGenerator.writeString(value.toString());
        }
    }

    /**
     * Parse a String representation of Greenplum array into a list of objects
     * @param val               a String representation of a Greenplum array
     * @param primitiveType     a primitive type of each element inside the array
     * @return a list of Java objects
     */
    private List<Object> parsePostgresArray(String val, DataType primitiveType) {
        if (val == null) {
            return null;
        }
        // split array value into elements and unescape each element
        String[] splits = pgUtilities.splitArray(val);
        List<Object> data = new ArrayList<>(splits.length);
        for (String split : splits) {
            try {
                // convert each element to a corresponding Java object
                data.add(decodeString(split, primitiveType));
            } catch (Exception e) {
                String hint = createErrorHintFromValue(StringUtils.startsWith(split, "{"), val);
                throw new PxfRuntimeException(String.format("Error parsing array element: %s was not of expected type %s", split, primitiveType), hint, e);
            }
        }
        return data;
    }

    /**
     * Decode a String representation of a Greenplum primitive type into a Java object
     * @param val               a String representation of a Greenplum object
     * @param primitiveType     primitive type
     * @return A Java object
     */
    private Object decodeString(String val, DataType primitiveType) {
        if (val == null) {
            return null;
        }
        // if the value is start of another array, it means the array is multi-dimensional which is not supported
        if (val.startsWith("{") && !TEXT_TYPES.contains(primitiveType)) {
            throw new PxfRuntimeException(String.format("Array value %s was encountered where a scalar value was expected", val));
        }
        // most of the datatypes that have been serialized into a String by GP will just preserve its value
        // including all numerical types that do not require String -> Java -> String serialization roundtrip
        switch (primitiveType) {
            case BYTEA:
                return pgUtilities.parseByteaLiteral(val);
            case BOOLEAN:
                // Json boolean value is "true" or "false" but GP uses "t" or "f", so we cannot reuse and have to parse
                return pgUtilities.parseBoolLiteral(val);
            default:
                return val;
        }
    }

    /**
     * Generate an error hint for a PXF exception message
     * @param isMultiDimensional whether this error is caused by inserting a multi-dimensional array
     * @param val                the string representation being decoded
     * @return the generated hint message
     */
    private String createErrorHintFromValue(boolean isMultiDimensional, String val) {
        if (isMultiDimensional) {
            return String.format("Column value \"%s\" is a multi-dimensional array; PXF does not support writing multi-dimensional arrays to Json files.", val);
        } else {
            return null;
        }
    }
}
