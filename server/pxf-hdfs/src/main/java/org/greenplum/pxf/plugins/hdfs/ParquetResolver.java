package org.greenplum.pxf.plugins.hdfs;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.util.StringUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTypeConverter;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public class ParquetResolver extends BasePlugin implements Resolver {

    // used to distinguish string pattern between type "timestamp" ("2019-03-14 14:10:28")
    // and type "timestamp with time zone" ("2019-03-14 14:10:28+07:30")
    public static final Pattern TIMESTAMP_PATTERN = Pattern.compile("[+-]\\d{2}(:\\d{2})?$");
    public static final String PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME = "pxf.parquet.write.decimal.overflow";
    private static final String PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_OPTION_ERROR = "error";
    private static final String PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_OPTION_ROUND = "round";
    private static final String PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_OPTION_IGNORE = "ignore";
    private static final PgUtilities pgUtilities = new PgUtilities();
    private final ObjectMapper mapper = new ObjectMapper();
    private final ParquetUtilities parquetUtilities = new ParquetUtilities(pgUtilities);
    private MessageType schema;
    private SimpleGroupFactory groupFactory;
    private List<ColumnDescriptor> columnDescriptors;

    private boolean isDecimalOverflowOptionError = false;
    private boolean isDecimalOverflowOptionRound = false;
    private boolean isPrecisionOverflowWarningLogged = false;
    private boolean isIntegerDigitCountOverflowWarningLogged = false;
    private boolean isScaleOverflowWarningLogged = false;

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        columnDescriptors = context.getTupleDescription();
        parseDecimalOverflowOption(configuration);
    }

    /**
     * Get fields based on the row
     *
     * @param row the row to get the fields from
     * @return a list of fields containing Greenplum data type and data value
     */
    @Override
    public List<OneField> getFields(OneRow row) {
        validateSchema();
        Group group = (Group) row.getData();
        List<OneField> output = new LinkedList<>();
        int columnIndex = 0;

        for (ColumnDescriptor columnDescriptor : columnDescriptors) {
            OneField oneField;
            if (!columnDescriptor.isProjected()) {
                oneField = new OneField(columnDescriptor.columnTypeCode(), null);
            } else {
                oneField = resolveField(group, columnIndex);
                columnIndex++;
            }
            output.add(oneField);
        }
        return output;
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     * @throws IOException if constructing a row from the fields failed
     */
    @Override
    public OneRow setFields(List<OneField> record) throws IOException {
        validateSchema();
        Group group = groupFactory.newGroup();
        for (int i = 0; i < record.size(); i++) {
            OneField field = record.get(i);
            ColumnDescriptor columnDescriptor = columnDescriptors.get(i);

            /*
             * We need to right trim the incoming value from Greenplum. This is
             * consistent with the behaviour in Hive, where char fields are right
             * trimmed during write. Note that String and varchar Hive types are
             * not right trimmed. Hive does not trim tabs or newlines
             */
            if (columnDescriptor.getDataType() == DataType.BPCHAR && field.val instanceof String) {
                field.val = Utilities.rightTrimWhiteSpace((String) field.val);
            }
            fillGroup(group, i, field);
        }
        return new OneRow(null, group);
    }

    /**
     * Fill the element of Parquet Group at the given index with provided value
     *
     * @param group       the Parquet Group object being filled
     * @param columnIndex the index of the column in a row that needs to be filled with data
     * @param field       OneField object holding the value we need to fill into the Parquet Group object
     */
    private void fillGroup(Group group, int columnIndex, OneField field) {
        if (field.val == null) {
            return;
        }

        Type type = schema.getType(columnIndex);
        if (type.isPrimitive()) {
            fillGroupWithPrimitive(group, columnIndex, field.val, type.asPrimitiveType());
            return;
        }

        LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation == null) {
            throw new UnsupportedTypeException("Parquet group type without logical annotation is not supported");
        }

        if (logicalTypeAnnotation != LogicalTypeAnnotation.listType()) {
            throw new UnsupportedTypeException(String.format("Parquet complex type %s is not supported", logicalTypeAnnotation));
        }
        /*
         * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
         * Parquet LIST must always annotate a 3-level structure:
         * <list-repetition> group <name> (LIST) {            // listType, a listType always has only 1 repeatedType
         *   repeated group list {                            // repeatedType, a repeatedType always has only 1 element type
         *     <element-repetition> <element-type> element;   // elementType
         *   }
         * }
         */
        GroupType listType = type.asGroupType();
        GroupType repeatedType = listType.getType(0).asGroupType();
        PrimitiveType elementType = repeatedType.getType(0).asPrimitiveType();
        // Decode Postgres String representation of an array into a list of Objects
        List<Object> values = parquetUtilities.parsePostgresArray(field.val.toString(), elementType.getPrimitiveTypeName(), elementType.getLogicalTypeAnnotation());

        /*
         * For example, the value of a text array ["hello","",null,"test"] would look like:
         * text_arr
         *    list
         *      element: hello
         *    list
         *      element:         --> empty element ""
         *    list               --> NULL element
         *    list
         *      element: test
         */
        Group listGroup = group.addGroup(columnIndex);
        for (Object value : values) {
            Group repeatedGroup = listGroup.addGroup(0);
            if (value != null) {
                fillGroupWithPrimitive(repeatedGroup, 0, value, elementType);
            }
        }
    }

    /**
     * Fill the element of Parquet Group at the given index with provided field value and Parquet Primitive type
     *
     * @param group         the Parquet Group object being filled
     * @param columnIndex   the index of the column in a row that needs to be filled with data
     * @param fieldValue    OneField object holding the value we need to fill into the Parquet Group object
     * @param primitiveType the Primitive Parquet schema we need to fill into the Parquet Group object
     */
    private void fillGroupWithPrimitive(Group group, int columnIndex, Object fieldValue, PrimitiveType primitiveType) {
        LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
        PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();

        switch (primitiveTypeName) {
            case BINARY:
                if (logicalTypeAnnotation instanceof StringLogicalTypeAnnotation) {
                    group.add(columnIndex, (String) fieldValue);
                } else if (fieldValue instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
                    group.add(columnIndex, Binary.fromReusedByteArray(byteBuffer.array(), 0, byteBuffer.limit()));
                } else {
                    group.add(columnIndex, Binary.fromReusedByteArray((byte[]) fieldValue));
                }
                break;
            case INT32:
                if (logicalTypeAnnotation instanceof DateLogicalTypeAnnotation) {
                    String dateString = (String) fieldValue;
                    group.add(columnIndex, ParquetTypeConverter.getDaysFromEpochFromDateString(dateString));
                } else if (logicalTypeAnnotation instanceof IntLogicalTypeAnnotation &&
                        ((IntLogicalTypeAnnotation) logicalTypeAnnotation).getBitWidth() == 16) {
                    group.add(columnIndex, (Short) fieldValue);
                } else {
                    group.add(columnIndex, (Integer) fieldValue);
                }
                break;
            case INT64:
                group.add(columnIndex, (Long) fieldValue);
                break;
            case DOUBLE:
                group.add(columnIndex, (Double) fieldValue);
                break;
            case FLOAT:
                group.add(columnIndex, (Float) fieldValue);
                break;
            case FIXED_LEN_BYTE_ARRAY:
                byte[] fixedLenByteArray = getFixedLenByteArray((String) fieldValue, primitiveType);
                if (fixedLenByteArray == null) {
                    return;
                }
                group.add(columnIndex, Binary.fromReusedByteArray(fixedLenByteArray));
                break;
            case INT96:  // SQL standard timestamp string value with or without time zone literals: https://www.postgresql.org/docs/9.4/datatype-datetime.html
                String timestamp = (String) fieldValue;
                if (TIMESTAMP_PATTERN.matcher(timestamp).find()) {
                    // Note: this conversion convert type "timestamp with time zone" will lose timezone information
                    // while preserving the correct value. (as Parquet doesn't support timestamp with time zone)
                    group.add(columnIndex, ParquetTypeConverter.getBinaryFromTimestampWithTimeZone(timestamp));
                } else {
                    group.add(columnIndex, ParquetTypeConverter.getBinaryFromTimestamp(timestamp));
                }
                break;
            case BOOLEAN:
                group.add(columnIndex, (Boolean) fieldValue);
                break;
            default:
                throw new UnsupportedTypeException(String.format("Parquet primitive type %s is not supported.", primitiveTypeName));
        }
    }

    private byte[] getFixedLenByteArray(String value, Type type) {
        // From org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.DecimalDataWriter#decimalToBinary
        DecimalLogicalTypeAnnotation typeAnnotation = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        int precision = Math.min(HiveDecimal.MAX_PRECISION, typeAnnotation.getPrecision());
        int scale = Math.min(HiveDecimal.MAX_SCALE, typeAnnotation.getScale());

        /*
        When this column is defined as NUMERIC(precision,scale), GPDB will handle writing values with overflow.
        When this column is defined as NUMERIC, the column type will be treated as DECIMAL(38,18),
        and HiveDecimal.create has different behaviors for different types of overflow:

        (1) When the data integer digit count is greater than 38,
        HiveDecimal.create will return a null value. To make the behavior consistent with Hive's behavior
        when storing on a Parquet-backed table, we store the value as null.
        For example, the integer digit count of 1234567890123456789012345678901234567890.123 is 40,
        which is greater than 38. HiveDecimal.create returns null.

        (2) When data integer digit count is not greater than 38,
        and the overall data precision is greater than 38,
        HiveDecimal.create will return a rounded-off value to fit in the Hive maximum supported precision 38.
        For example, the integer digit count of 1234567890123456789012345.12345678901234567890 is 25 which is less than 38,
        but its overall precision is 45 which is greater than 38.
        Then data will be created as a rounded value 1234567890123456789012345.1234567890123

        (3) When data integer digit count is not greater than 38,
        and the overall data precision is not greater than 38,
        HiveDecimal.create will return the same decimal value as provided.
        For example, 123456.123456 can fit in DECIMAL(38,18) without and data loss, so the data will be created as the same
         */
        // HiveDecimal.create will return a decimal value which can fit in DECIMAL(38)
        HiveDecimal parsedValue = HiveDecimal.create(value);
        String columnName = columnDescriptors.get(0).columnName();
        if (parsedValue == null) {
            if (isDecimalOverflowOptionError || isDecimalOverflowOptionRound) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds maximum precision %d.",
                        value, columnName, precision));
            }

            LOG.trace(String.format("The value %s for the NUMERIC column %s exceeds maximum precision %d and has been stored as NULL.",
                    value, columnName, precision));

            if (!isPrecisionOverflowWarningLogged) {
                LOG.warn(String.format("There are rows where for the NUMERIC column %s the values exceed maximum precision %d " +
                                "and have been stored as NULL. Enable TRACE log level for row-level details.",
                        columnName, precision));
                isPrecisionOverflowWarningLogged = true;
            }
            return null;
        }

        // At this point data can fit in precision 38, but still need enforcePrecisionScale to check whether it can fit in scale 18
        HiveDecimal hiveDecimal = HiveDecimal.enforcePrecisionScale(
                parsedValue,
                precision,
                scale);

        /*
        When data integer digit count is greater than the maximum supported integer digit count 20 (precision - scale),
        enforcePrecisionScale will return null, it means we cannot store the value in Parquet because we have
        exceeded the maximum integer digit count. To make the behavior consistent with Hive's behavior
        when storing on a Parquet-backed table, we store the value as null.
        For example, in the case 2 in the previous HiveDecimal.create example,
        we got a rounded value 1234567890123456789012345.1234567890123.
        Its integer digit count is 25 which exceeds the maximum integer digit count 20.
        So it cannot fit in DECIMAL(38,18) and Null is returned.
         */
        if (hiveDecimal == null) {
            if (isDecimalOverflowOptionError || isDecimalOverflowOptionRound) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds maximum precision and scale (%d,%d).",
                        value, columnName, precision, scale));
            }

            LOG.trace(String.format("The value %s for the NUMERIC column %s exceeds maximum precision and scale (%d,%d) and has been stored as NULL.",
                    value, columnName, precision, scale));

            if (!isIntegerDigitCountOverflowWarningLogged) {
                LOG.warn(String.format("There are rows where for the NUMERIC column %s the values exceed maximum precision and scale (%d,%d) " +
                                "and have been stored as NULL. Enable TRACE log level for row-level details.",
                        columnName, precision, scale));
                isIntegerDigitCountOverflowWarningLogged = true;
            }
            return null;
        }

        BigDecimal accurateDecimal = new BigDecimal(value);
        // At this point data can fit in DECIMAL(38,18), but may have been rounded off
        if ((isDecimalOverflowOptionError || isDecimalOverflowOptionRound) && accurateDecimal.compareTo(hiveDecimal.bigDecimalValue()) != 0) {
            if (isDecimalOverflowOptionError) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds maximum scale %d.",
                        value, columnName, scale));
            }

            LOG.trace(String.format("The value %s for the NUMERIC column %s exceeds maximum scale %d and has been rounded off.",
                    value, columnName, scale));

            if (!isScaleOverflowWarningLogged) {
                LOG.warn(String.format("There are rows where for the NUMERIC column %s the values exceed maximum scale %d " +
                                "and have been rounded off. Enable TRACE log level for row-level details.",
                        columnName, scale));
                isScaleOverflowWarningLogged = true;
            }
        }

        byte[] decimalBytes = hiveDecimal.bigIntegerBytesScaled(scale);

        // Estimated number of bytes needed.
        int precToBytes = ParquetFileAccessor.PRECISION_TO_BYTE_COUNT[precision - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return decimalBytes;
        }

        byte[] tgt = new byte[precToBytes];
        if (hiveDecimal.signum() == -1) {
            // For negative number, initializing bits to 1
            for (int i = 0; i < precToBytes; i++) {
                tgt[i] |= 0xFF;
            }
        }
        System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length); // Padding leading zeroes/ones.
        return tgt;
        // end -- org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.DecimalDataWriter#decimalToBinary
    }

    // Set schema from context if null
    // TODO: Fix the bridge interface so the schema is set before get/setFields is called
    //       Then validateSchema can be done during initialize phase
    private void validateSchema() {
        if (schema == null) {
            schema = (MessageType) context.getMetadata();
            if (schema == null)
                throw new RuntimeException("No schema detected in request context");
            groupFactory = new SimpleGroupFactory(schema);
        }
    }

    /**
     * Resolve the Parquet data at the columnIndex into Greenplum representation
     *
     * @param group       contains parquet schema and data for a row
     * @param columnIndex is the index of the column in the row that needs to be resolved
     * @return a field containing Greenplum data type and data value
     */
    private OneField resolveField(Group group, int columnIndex) {
        OneField field = new OneField();
        // get type converter based on the field data type
        // schema is the readSchema, if there is column projection, the schema will be a subset of tuple descriptions
        Type type = schema.getType(columnIndex);
        ParquetTypeConverter converter = ParquetTypeConverter.from(type);
        // determine how many values for the field are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);
        if (type.getRepetition() == REPEATED) {
            // For REPEATED type, repetitionCount can be any non-negative number,
            // the element value will be converted into JSON value
            ArrayNode jsonArray = mapper.createArrayNode();
            for (int repeatIndex = 0; repeatIndex < repetitionCount; repeatIndex++) {
                converter.addValueToJsonArray(group, columnIndex, repeatIndex, type, jsonArray);
            }
            field.type = DataType.TEXT.getOID();
            try {
                field.val = mapper.writeValueAsString(jsonArray);
            } catch (Exception e) {
                String typeName;
                if (type.isPrimitive()) {
                    typeName = type.asPrimitiveType().getPrimitiveTypeName().name();
                } else {
                    typeName = type.asGroupType().getLogicalTypeAnnotation() == null ?
                            "customized struct" : type.asGroupType().getLogicalTypeAnnotation().toString();
                }
                throw new RuntimeException(String.format("Failed to serialize repeated parquet type %s.", typeName), e);
            }
        } else if (repetitionCount == 0) {
            // For non-REPEATED type, repetitionCount can only be 0 or 1
            // repetitionCount == 0 means this is a null LIST/Primitive
            field.type = converter.getDataType(type).getOID();
            field.val = null;
        } else {
            // repetitionCount can only be 1
            field.type = converter.getDataType(type).getOID();
            field.val = converter.getValue(group, columnIndex, 0, type);
        }
        return field;
    }

    /**
     * Sets configuration variables based on server configuration properties of pxf.parquet.write.decimal.overflow.
     *
     * @param configuration contains server configuration properties
     */
    public void parseDecimalOverflowOption(Configuration configuration) {
        String decimalOverflowOption = configuration.get(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_OPTION_ROUND);
        if (StringUtils.equalsIgnoreCase(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_OPTION_IGNORE, decimalOverflowOption)) {
            isDecimalOverflowOptionRound = false;
            isDecimalOverflowOptionError = false;
            return;
        } else if (StringUtils.equalsIgnoreCase(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_OPTION_ERROR, decimalOverflowOption)) {
            isDecimalOverflowOptionError = true;
            isDecimalOverflowOptionRound = false;
            return;
        }
        // if configuration value is not "error", "round" or "ignore", use default option "round"
        isDecimalOverflowOptionRound = true;
        isDecimalOverflowOptionError = false;
    }
}
