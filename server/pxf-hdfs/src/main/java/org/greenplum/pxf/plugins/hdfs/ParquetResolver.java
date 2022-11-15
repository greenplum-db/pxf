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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
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

    private MessageType schema;
    private SimpleGroupFactory groupFactory;
    private List<ColumnDescriptor> columnDescriptors;
    private final ObjectMapper mapper = new ObjectMapper();
    private static final PgUtilities pgUtilities = new PgUtilities();
    private ParquetUtilities parquetUtilities=new ParquetUtilities(pgUtilities);

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        columnDescriptors = context.getTupleDescription();
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
            fillGroup(i, field, group, schema.getType(i));
        }
        return new OneRow(null, group);
    }

    /**
     * Fill the index-th group based on the Greenplum data type and value string provided by {@link OneField} field
     *
     * @param index The index we are going to fill data at
     * @param field Provide the String value of record[index] we are going to convert into parquet
     * @param group The outermost group for the {@link OneRow} parquet schema
     * @param type  The parquet schema type of record[index]
     */
    private void fillGroup(int index, OneField field, Group group, Type type) {
        if (field.val == null)
            return;
        if (type.isPrimitive()) {
            fillPrimitiveGroup(index, field.val, group, type, true);
        } else if (type.asGroupType().getOriginalType() == LogicalTypeAnnotation.listType().toOriginalType()) {
            /*
             * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
             * Parquet LIST must always annotate a 3-level structure:
             * <list-repetition> group <name> (LIST) {
             *   repeated group list {
             *     <element-repetition> <element-type> element;
             *   }
             * }
             */
            GroupType listType = type.asGroupType();
            GroupType repeatedType = listType.getType(0).asGroupType();
            Type elementType = repeatedType.getType(0).asPrimitiveType();
            // Decode Postgres String representation of an array into a list of Objects of the required element type
            List<Object> values = parquetUtilities.parsePostgresArray(field.val.toString(), elementType.asPrimitiveType().getPrimitiveTypeName(), elementType.getLogicalTypeAnnotation());
            Group arrayGroup = new SimpleGroup(listType);

            for (Object value : values) {
                Group repeatedGroup = new SimpleGroup(repeatedType);
                // If current element is null, directly add repeatedGroup into group
                if (value != null) {
                    fillPrimitiveGroup(0, value, repeatedGroup, elementType, false);
                }
                arrayGroup.add(0, repeatedGroup);
            }
            group.add(index, arrayGroup);
        } else {
            throw new UnsupportedTypeException(String.format("Parquet complex type %s is not supported", type.asGroupType().getOriginalType().name()));
        }
    }

    /**
     * Fill the index-th primitive group based on the Greenplum data type and value string provided by {@link OneField} field
     * This primitive group could be an actual primitive group or the innermost primitive group of a List group
     *
     * @param index       The index we are going to fill data at
     * @param fieldValue  Provide the String value of record[index] we are going to convert into parquet
     * @param group       The out most group for the {@link OneRow} parquet schema
     * @param type        The parquet schema type of record[index]
     * @param isPrimitive This method is called to construct a Primitive type or a LIST type
     */
    private void fillPrimitiveGroup(int index, Object fieldValue, Group group, Type type, boolean isPrimitive) {
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
                if (type.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation) {
                    group.add(index, (String) fieldValue);
                } else if (isPrimitive) {
                    group.add(index, Binary.fromReusedByteArray((byte[]) fieldValue));
                } else {
                    group.add(index, Binary.fromReusedByteArray(((ByteBuffer) fieldValue).array(), 0, ((ByteBuffer) fieldValue).limit()));
                }
                break;
            case INT32:
                if (type.getLogicalTypeAnnotation() instanceof DateLogicalTypeAnnotation) {
                    String dateString = (String) fieldValue;
                    group.add(index, ParquetTypeConverter.getDaysFromEpochFromDateString(dateString));
                } else if (type.getLogicalTypeAnnotation() instanceof IntLogicalTypeAnnotation &&
                        ((IntLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getBitWidth() == 16) {
                    group.add(index, (Short) fieldValue);
                } else {
                    group.add(index, (Integer) fieldValue);
                }
                break;
            case INT64:
                group.add(index, (Long) fieldValue);
                break;
            case DOUBLE:
                group.add(index, (Double) fieldValue);
                break;
            case FLOAT:
                group.add(index, (Float) fieldValue);
                break;
            case FIXED_LEN_BYTE_ARRAY:
                byte[] fixedLenByteArray = getFixedLenByteArray((String) fieldValue, type);
                if (fixedLenByteArray == null) {
                    return;
                }
                group.add(index, Binary.fromReusedByteArray(fixedLenByteArray));
                break;
            case INT96:  // SQL standard timestamp string value with or without time zone literals: https://www.postgresql.org/docs/9.4/datatype-datetime.html
                String timestamp = (String) fieldValue;
                if (TIMESTAMP_PATTERN.matcher(timestamp).find()) {
                    // Note: this conversion convert type "timestamp with time zone" will lose timezone information
                    // while preserving the correct value. (as Parquet doesn't support timestamp with time zone.
                    group.add(index, ParquetTypeConverter.getBinaryFromTimestampWithTimeZone(timestamp));
                } else {
                    group.add(index, ParquetTypeConverter.getBinaryFromTimestamp(timestamp));
                }
                break;
            case BOOLEAN:
                group.add(index, (Boolean) fieldValue);
                break;
            default:
                throw new UnsupportedTypeException("Not supported primitive type " + type.asPrimitiveType().getPrimitiveTypeName());
        }
    }

    private byte[] getFixedLenByteArray(String value, Type type) {
        // From org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.DecimalDataWriter#decimalToBinary
        DecimalLogicalTypeAnnotation typeAnnotation = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        int precision = Math.min(HiveDecimal.MAX_PRECISION, typeAnnotation.getPrecision());
        int scale = Math.min(HiveDecimal.MAX_SCALE, typeAnnotation.getScale());
        HiveDecimal hiveDecimal = HiveDecimal.enforcePrecisionScale(
                HiveDecimal.create(value),
                precision,
                scale);

        if (hiveDecimal == null) {
            // When precision is higher than HiveDecimal.MAX_PRECISION
            // and enforcePrecisionScale returns null, it means we
            // cannot store the value in Parquet because we have
            // exceeded the precision. To make the behavior consistent
            // with Hive's behavior when storing on a Parquet-backed
            // table, we store the value as null.
            return null;
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
                    typeName = type.asGroupType().getOriginalType() == null ?
                            "customized struct" : type.asGroupType().getOriginalType().name();
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
}
