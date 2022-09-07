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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTypeConverter;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;

import java.io.IOException;
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

    @Override
    public List<OneField> getFields(OneRow row) {
        validateSchema();
        Group group = (Group) row.getData();
        List<OneField> output = new LinkedList<>();
        int columnIndex = 0;

        // schema is the readSchema, if there is column projection
        // the schema will be a subset of tuple descriptions
        for (ColumnDescriptor columnDescriptor : columnDescriptors) {
            OneField oneField;
            if (!columnDescriptor.isProjected()) {
                oneField = new OneField(columnDescriptor.columnTypeCode(), null);
            } else if (schema.getType(columnIndex).isPrimitive()) {
                oneField = resolvePrimitive(group, columnIndex, schema.getType(columnIndex), 0);
                columnIndex++;
            } else {
                throw new UnsupportedOperationException("Parquet complex type support is not yet available.");
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

    private void fillGroup(int index, OneField field, Group group, Type type) throws IOException {
        if (field.val == null)
            return;
        if(type.isPrimitive()){
            fillPrimitiveGroup(index,field,group,type);
        }else{
            fillComplexGroup(index,field,group,type);
        }

    }


    private void fillPrimitiveGroup(int index, OneField field, Group group, Type type) throws IOException {
        if (field.val == null)
            return;
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
                if (type.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation)
                    group.add(index, (String) field.val);
                else
                    group.add(index, Binary.fromReusedByteArray((byte[]) field.val));
                break;
            case INT32:
                if (type.getLogicalTypeAnnotation() instanceof DateLogicalTypeAnnotation) {
                    String dateString = (String) field.val;
                    group.add(index, ParquetTypeConverter.getDaysFromEpochFromDateString(dateString));
                } else if (type.getLogicalTypeAnnotation() instanceof IntLogicalTypeAnnotation &&
                        ((IntLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getBitWidth() == 16) {
                    group.add(index, (Short) field.val);
                } else {
                    group.add(index, (Integer) field.val);
                }
                break;
            case INT64:
                group.add(index, (Long) field.val);
                break;
            case DOUBLE:
                group.add(index, (Double) field.val);
                break;
            case FLOAT:
                group.add(index, (Float) field.val);
                break;
            case FIXED_LEN_BYTE_ARRAY:
                // From org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.DecimalDataWriter#decimalToBinary
                String value = (String) field.val;
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
                    return;
                }

                byte[] decimalBytes = hiveDecimal.bigIntegerBytesScaled(scale);

                // Estimated number of bytes needed.
                int precToBytes = ParquetFileAccessor.PRECISION_TO_BYTE_COUNT[precision - 1];
                if (precToBytes == decimalBytes.length) {
                    // No padding needed.
                    group.add(index, Binary.fromReusedByteArray(decimalBytes));
                } else {
                    byte[] tgt = new byte[precToBytes];
                    if (hiveDecimal.signum() == -1) {
                        // For negative number, initializing bits to 1
                        for (int i = 0; i < precToBytes; i++) {
                            tgt[i] |= 0xFF;
                        }
                    }
                    System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length); // Padding leading zeroes/ones.
                    group.add(index, Binary.fromReusedByteArray(tgt));
                }
                // end -- org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.DecimalDataWriter#decimalToBinary
                break;
            case INT96:  // SQL standard timestamp string value with or without time zone literals: https://www.postgresql.org/docs/9.4/datatype-datetime.html
                String timestamp = (String) field.val;
                if (TIMESTAMP_PATTERN.matcher(timestamp).find()) {
                    // Note: this conversion convert type "timestamp with time zone" will lose timezone information
                    // while preserving the correct value. (as Parquet doesn't support timestamp with time zone.
                    group.add(index, ParquetTypeConverter.getBinaryFromTimestampWithTimeZone(timestamp));
                } else {
                    group.add(index, ParquetTypeConverter.getBinaryFromTimestamp(timestamp));
                }
                break;
            case BOOLEAN:
                group.add(index, (Boolean) field.val);
                break;
            default:
                throw new IOException("Not supported type " + type.asPrimitiveType().getPrimitiveTypeName());
        }
    }

    private void fillComplexGroup(int index, OneField field, Group group, Type type) throws IOException {
        if (field.val == null)
            return;
        switch (type.asGroupType().getOriginalType()){
            case LIST:

                PrimitiveType.PrimitiveTypeName elementTypeName = type.asGroupType().getFields().get(0).asGroupType().getFields().get(0).asPrimitiveType().getPrimitiveTypeName();
                LOG.debug(elementTypeName.toString());
                LOG.debug(context.getMetadata().toString());
                LOG.debug(type.asGroupType().getFields()+"");
                LOG.debug(schema.getType(0).asGroupType().getType(0).asGroupType().getRepetition().toString());
                LOG.debug(schema.getType(0).asGroupType().getType(0).asGroupType().getFields().toString());
                fillListGroup(index, field, group, elementTypeName);
                break;
            default:
                throw new IOException("Not supported type " + type.asPrimitiveType().getPrimitiveTypeName());
        }
    }

    private void fillListGroup(int index, OneField field, Group group, PrimitiveType.PrimitiveTypeName elementTypeName) throws IOException {
        if (field.val == null)
            return;
        switch (elementTypeName) {
            case BOOLEAN:
                List<Object> vals = parquetUtilities.parsePostgresArray(field.val.toString(),elementTypeName);
//                int count = group.getFieldRepetitionCount(index);
//                schema.getType(0).asGroupType().getFields();
//                Group listGroup= new SimpleGroup(schema.getType(0).asGroupType());
//                Group elementGroup= new SimpleGroup(schema.getType(0).asGroupType().getType(0).asGroupType());
//                for(int i = 0 ; i < vals.size() ; i++){
//
//                    elementGroup.add(0 , vals.get(i));
//                    listGroup.add(0, elementGroup);
//                }
//                group.add(index,listGroup);
                break;
            default:
                throw new IOException("Not supported type " + elementTypeName);
        }
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

    private OneField resolvePrimitive(Group group, int columnIndex, Type type, int level) {

        OneField field = new OneField();
        // get type converter based on the primitive type
        ParquetTypeConverter converter = ParquetTypeConverter.from(type.asPrimitiveType());

        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        // at the top level (top field), non-repeated primitives will convert to typed OneField
        if (level == 0 && type.getRepetition() != REPEATED) {
            field.type = converter.getDataType(type).getOID();
            field.val = repetitionCount == 0 ? null : converter.getValue(group, columnIndex, 0, type);
        } else if (type.getRepetition() == REPEATED) {
            // repeated primitive at any level will convert into JSON
            ArrayNode jsonArray = mapper.createArrayNode();
            for (int repeatIndex = 0; repeatIndex < repetitionCount; repeatIndex++) {
                converter.addValueToJsonArray(group, columnIndex, repeatIndex, type, jsonArray);
            }
            // but will become a string only at top level
            if (level == 0) {
                field.type = DataType.TEXT.getOID();
                try {
                    field.val = mapper.writeValueAsString(jsonArray);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to serialize repeated parquet type " + type.asPrimitiveType().getName(), e);
                }
            } else {
                // just return the array node within OneField container
                field.val = jsonArray;
            }
        } else {
            // level > 0 and type != REPEATED -- primitive type as a member of complex group -- NOT YET SUPPORTED
            throw new UnsupportedOperationException("Parquet complex type support is not yet available.");
        }
        return field;
    }

}
