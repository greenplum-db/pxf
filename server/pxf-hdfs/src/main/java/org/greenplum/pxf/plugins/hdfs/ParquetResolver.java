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

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ParquetResolver extends BasePlugin implements Resolver {

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final long MILLIS_IN_DAY = 24 * 3600 * 1000;

    private MessageType schema;
    private SimpleGroupFactory groupFactory;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        schema = context.getFragmentUserData() == null ?
                autoGenSchema(requestContext.getTupleDescription()) :
                MessageTypeParser.parseMessageType(new String(context.getFragmentUserData()));
        groupFactory = new SimpleGroupFactory(schema);
    }

    @Override
    public List<OneField> getFields(OneRow row) {
        Group group = (Group) row.getData();
        List<OneField> output = new LinkedList<>();

        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (schema.getType(i).isPrimitive()) {
                output.add(resolvePrimitive(i, group, schema.getType(i)));
            } else {
                throw new UnsupportedTypeException("Only primitive types are supported.");
            }
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
        Group group = groupFactory.newGroup();
        for (int i = 0; i < record.size(); i++) {
            resolveType(i, record.get(i), group);
        }
        return new OneRow(null, group);
    }

    /**
     * generate schema automatically
     */
    private MessageType autoGenSchema(List<ColumnDescriptor> columns) {

        List<Type> fields = new ArrayList<>();
        for (ColumnDescriptor column: columns) {
            String columnName = column.columnName();
            int columnTypeCode = column.columnTypeCode();

            PrimitiveTypeName typeName;
            OriginalType origType = null;
            switch (DataType.get(columnTypeCode)) {
                case BOOLEAN:
                    typeName = PrimitiveTypeName.BOOLEAN;
                    break;
                case BYTEA:
                    typeName = PrimitiveTypeName.BINARY;
                    break;
                case BIGINT:
                    typeName = PrimitiveTypeName.INT64;
                    break;
                case SMALLINT:
                    origType = OriginalType.INT_16;
                    typeName = PrimitiveTypeName.INT32;
                    break;
                case INTEGER:
                    typeName = PrimitiveTypeName.INT32;
                    break;
                case REAL:
                    typeName = PrimitiveTypeName.FLOAT;
                    break;
                case FLOAT8:
                    typeName = PrimitiveTypeName.DOUBLE;
                    break;
                case NUMERIC:
                    origType = OriginalType.DECIMAL;
                    typeName = PrimitiveTypeName.BINARY;
                    break;
                case VARCHAR:
                case BPCHAR:
                case DATE:
                case TIME:
                case TIMESTAMP:
                case TEXT:
                    origType = OriginalType.UTF8;
                    typeName = PrimitiveTypeName.BINARY;
                    break;
                default:
                    throw new UnsupportedTypeException("Type " + columnTypeCode + "is not supported");
            }
            fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, typeName, columnName, origType));
        }

        return new MessageType("schema", fields);
    }

    @SuppressWarnings( "deprecation" )
    private void resolveType(int index, OneField field, Group group) throws IOException {
        switch (DataType.get(field.type)) {
            case DATE:
            case TIME:
            case TIMESTAMP:
                group.add(index, (String)field.val);
                break;
            case BPCHAR:
            case VARCHAR:
            case TEXT:
                group.add(index, (String)field.val);
                break;
            case BYTEA:
                group.add(index, Binary.fromByteArray((byte [])field.val));
                break;
            case REAL:
                group.add(index, (Float)field.val);
                break;
            case NUMERIC:
            case BIGINT:
                group.add(index, (Long)field.val);
                break;
            case BOOLEAN:
                group.add(index, (Boolean)field.val);
                break;
            case FLOAT8:
                group.add(index, (Double)field.val);
                break;
            case INTEGER:
                group.add(index, (Integer)field.val);
                break;
            case SMALLINT:
                group.add(index, (Short)field.val);
                break;
            default:
                throw new IOException("Not supported type, typeId = " + field.type);
        }
   }

   private OneField resolvePrimitive(Integer columnIndex, Group g, Type type) {
        OneField field = new OneField();
        OriginalType originalType = type.getOriginalType();
        PrimitiveType primitiveType = type.asPrimitiveType();
        switch (primitiveType.getPrimitiveTypeName()) {
            case BINARY: {
                if (originalType == null) {
                    field.type = DataType.BYTEA.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                            null : g.getBinary(columnIndex, 0).getBytes();
                } else if (originalType == OriginalType.DATE) { // DATE type
                    field.type = DataType.DATE.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ? null : g.getString(columnIndex, 0);
                } else if (originalType == OriginalType.TIMESTAMP_MILLIS) { // TIMESTAMP type
                    field.type = DataType.TIMESTAMP.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ? null : g.getString(columnIndex, 0);
                } else {
                    field.type = DataType.TEXT.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                            null : g.getString(columnIndex, 0);
                }
                break;
            }
            case INT32: {
                if (originalType == OriginalType.INT_8 || originalType == OriginalType.INT_16) {
                    field.type = DataType.SMALLINT.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                            null : (short) g.getInteger(columnIndex, 0);
                } else {
                    field.type = DataType.INTEGER.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                            null : g.getInteger(columnIndex, 0);
                }
                break;
            }
            case INT64: {
                field.type = DataType.BIGINT.getOID();
                field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : g.getLong(columnIndex, 0);
                break;
            }
            case DOUBLE: {
                field.type = DataType.FLOAT8.getOID();
                field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : g.getDouble(columnIndex, 0);
                break;
            }
            case INT96: {
                field.type = DataType.TIMESTAMP.getOID();
                field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : bytesToTimestamp(g.getInt96(columnIndex, 0).getBytes());
                break;
            }
            case FLOAT: {
                field.type = DataType.REAL.getOID();
                field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : g.getFloat(columnIndex, 0);
                break;
            }
            case FIXED_LEN_BYTE_ARRAY: {
                field.type = DataType.NUMERIC.getOID();
                if (g.getFieldRepetitionCount(columnIndex) > 0) {
                    int scale = type.asPrimitiveType().getDecimalMetadata().getScale();
                    field.val = new BigDecimal(new BigInteger(g.getBinary(columnIndex, 0).getBytes()), scale);
                }
                break;
            }
            case BOOLEAN: {
                field.type = DataType.BOOLEAN.getOID();
                field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : g.getBoolean(columnIndex, 0);
                break;
            }
            default: {
                throw new UnsupportedTypeException("Type " + primitiveType.getPrimitiveTypeName()
                        + "is not supported");
            }
        }
        return field;
    }

    private Timestamp bytesToTimestamp(byte[] bytes) {

        long numberOfDays = ByteBuffer.wrap(new byte[]{
                bytes[7],
                bytes[6],
                bytes[5],
                bytes[4],
                bytes[3],
                bytes[2],
                bytes[1],
                bytes[0]
        }).getLong();

        int julianDays = (ByteBuffer.wrap(new byte[]{bytes[11],
                bytes[10],
                bytes[9],
                bytes[8]
        })).getInt();
        long unixTimeMs = (julianDays - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY + numberOfDays / 1000000;
        return new Timestamp(unixTimeMs);
    }
}
