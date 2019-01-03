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
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

public class ParquetResolver extends BasePlugin implements Resolver {

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final int SECOND_IN_MILLIS = 1000;
    private static final long MILLIS_IN_DAY = 24 * 3600 * 1000;
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private MessageType schema;
    private SimpleGroupFactory groupFactory;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        schema = (MessageType) requestContext.getMetadata();
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
            fillGroup(i, record.get(i), group, schema.getType(i));
        }
        return new OneRow(null, group);
    }

    private void fillGroup(int index, OneField field, Group group, Type type) throws IOException {
        if (field.val == null)
            return;
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
                if (type.getOriginalType() == OriginalType.UTF8)
                    group.add(index, (String) field.val);
                else
                    group.add(index, Binary.fromReusedByteArray((byte[]) field.val));
                break;
            case INT32:
                if (type.getOriginalType() == OriginalType.INT_16)
                    group.add(index, (Short) field.val);
                else
                    group.add(index, (Integer) field.val);
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
                BigDecimal value = new BigDecimal((String) field.val);
                byte fillByte = (byte) (value.signum() < 0 ? 0xFF : 0x00);
                byte[] unscaled = value.unscaledValue().toByteArray();
                byte[] bytes = new byte[16];
                int offset = bytes.length - unscaled.length;
                for (int i = 0; i < bytes.length; i += 1) {
                    if (i < offset) {
                        bytes[i] = fillByte;
                    } else {
                        bytes[i] = unscaled[i - offset];
                    }
                }
                group.add(index, Binary.fromReusedByteArray(bytes));
                break;
            case INT96:
                LocalDateTime date = LocalDateTime.parse((String) field.val, dateFormatter);
                long millisSinceEpoch = date.toEpochSecond(ZoneOffset.UTC) * SECOND_IN_MILLIS;
                group.add(index, getBinary(millisSinceEpoch));
                break;
            case BOOLEAN:
                group.add(index, (Boolean) field.val);
                break;
            default:
                throw new IOException("Not supported type " + type.asPrimitiveType().getPrimitiveTypeName());
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

    // Convert parquet byte array to java timestamp
    private Timestamp bytesToTimestamp(byte[] bytes) {
        long timeOfDayNanos = ByteBuffer.wrap(new byte[]{
                bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]}).getLong();
        int julianDays = (ByteBuffer.wrap(new byte[]{bytes[11], bytes[10], bytes[9], bytes[8]})).getInt();
        long unixTimeMs = (julianDays - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY + timeOfDayNanos / 1000000;
        return new Timestamp(unixTimeMs);
    }

    // Convert epoch timestamp to byte array (INT96)
    // Inverse of the function above
    private Binary getBinary(long timeMillis) {
        long daysSinceEpoch = timeMillis / MILLIS_IN_DAY;
        int julianDays = JULIAN_EPOCH_OFFSET_DAYS + (int) daysSinceEpoch;
        long timeOfDayNanos = (timeMillis % MILLIS_IN_DAY) * 1000000;
        return new NanoTime(julianDays, timeOfDayNanos).toBinary();
    }
}
