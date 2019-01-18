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

import org.apache.commons.lang.ObjectUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.GPDBWritableMapper;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.GPDBWritable;
import org.greenplum.pxf.api.io.Writable;
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
import java.util.List;

public class ParquetResolver extends BasePlugin implements Resolver {

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final int SECOND_IN_MILLIS = 1000;
    private static final long MILLIS_IN_DAY = 24 * 3600 * 1000;
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private MessageType schema;
    private SimpleGroupFactory groupFactory;
    private GPDBWritable gpdbOutput;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        gpdbOutput = makeGPDBWritableOutput();
    }

    @Override
    public List<OneField> getFields(OneRow row) {
        return null;
    }

    @Override
    public boolean supportsWritable() {
        return true;
    }

    @Override
    public Writable getWritable(OneRow row) throws IOException {
        Group group = (Group) row.getData();
        MessageType schema = (MessageType) row.getKey();

        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (schema.getType(i).isPrimitive()) {
                resolvePrimitive(i, group, schema.getType(i));
            } else {
                throw new UnsupportedTypeException("Only primitive types are supported.");
            }
        }
        return gpdbOutput;
    }

    @Override
    public OneRow setFields(List<OneField> record) {
        return null;
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param  writable of {@link GPDBWritable}
     * @return the constructed {@link OneRow}
     * @throws IOException if constructing a row from the fields failed
     */
    @Override
    public OneRow setWritable(Writable writable) throws IOException {
        if (groupFactory == null) {
            schema = (MessageType)context.getMetadata();
            groupFactory = new SimpleGroupFactory(schema);
        }
        Group group = groupFactory.newGroup();
        GPDBWritable gpdbWritable = (GPDBWritable)writable;
        int[] colTypes = gpdbWritable.getColType();
        GPDBWritableMapper mapper = new GPDBWritableMapper(gpdbWritable);
        for (int i = 0; i < colTypes.length; i++) {
            mapper.setDataType(colTypes[i]);
            fillGroup(i, mapper.getData(i), group, schema.getType(i));
        }
        return new OneRow(null, group);
    }

    /**
     * Creates the GPDBWritable object. The object is created one time and is
     * refilled from recFields for each record sent
     *
     * @return empty GPDBWritable object with set columns
     */
    private GPDBWritable makeGPDBWritableOutput() {
        int num_actual_fields = context.getColumns();
        int[] schema = new int[num_actual_fields];
        for (int i = 0; i < num_actual_fields; i++) {
            schema[i] = context.getColumn(i).columnTypeCode();
        }
        return new GPDBWritable(schema);
    }

    private void fillGroup(int index, Object val, Group group, Type type) throws IOException {
        if (val == null)
            return;
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
                if (type.getOriginalType() == OriginalType.UTF8)
                    group.add(index, (String) val);
                else
                    group.add(index, Binary.fromReusedByteArray((byte[]) val));
                break;
            case INT32:
                if (type.getOriginalType() == OriginalType.INT_16)
                    group.add(index, (Short) val);
                else
                    group.add(index, (Integer) val);
                break;
            case INT64:
                group.add(index, (Long) val);
                break;
            case DOUBLE:
                group.add(index, (Double) val);
                break;
            case FLOAT:
                group.add(index, (Float) val);
                break;
            case FIXED_LEN_BYTE_ARRAY:
                BigDecimal value = new BigDecimal((String) val);
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
                LocalDateTime date = LocalDateTime.parse((String) val, dateFormatter);
                long millisSinceEpoch = date.toEpochSecond(ZoneOffset.UTC) * SECOND_IN_MILLIS;
                group.add(index, getBinary(millisSinceEpoch));
                break;
            case BOOLEAN:
                group.add(index, (Boolean) val);
                break;
            default:
                throw new IOException("Not supported type " + type.asPrimitiveType().getPrimitiveTypeName());
        }
    }

    private void resolvePrimitive(Integer colIndex, Group g, Type type) throws GPDBWritable.TypeMismatchException {
        OriginalType originalType = type.getOriginalType();
        PrimitiveType primitiveType = type.asPrimitiveType();
        int repetitionCount = g.getFieldRepetitionCount( colIndex);
        switch (primitiveType.getPrimitiveTypeName()) {
            case BINARY:
                if (originalType == null) {
                    gpdbOutput.setBytes(colIndex, repetitionCount == 0 ? null : g.getBinary(colIndex, 0).getBytes());
                } else {
                    gpdbOutput.setString(colIndex, ObjectUtils.toString(repetitionCount == 0 ? null : g.getString(colIndex, 0)));
                }
                break;
            case INT32:
                if (originalType == OriginalType.INT_8 || originalType == OriginalType.INT_16) {
                    gpdbOutput.setShort(colIndex, repetitionCount == 0 ? null : (short)g.getInteger(colIndex, 0));
                } else {
                    gpdbOutput.setInt(colIndex, repetitionCount == 0 ? null : g.getInteger(colIndex, 0));
                }
                break;
            case INT64:
                gpdbOutput.setLong(colIndex, repetitionCount == 0 ? null : g.getLong(colIndex, 0));
                break;
            case DOUBLE:
                gpdbOutput.setDouble(colIndex, repetitionCount == 0 ? null : g.getDouble(colIndex, 0));
                break;
            case INT96:
                gpdbOutput.setString(colIndex, ObjectUtils.toString(
                        repetitionCount == 0 ? null : bytesToTimestamp(g.getInt96(colIndex, 0).getBytes()), null));
                break;
            case FLOAT:
                gpdbOutput.setFloat(colIndex, repetitionCount == 0 ? null : g.getFloat(colIndex, 0));
                break;
            case FIXED_LEN_BYTE_ARRAY:
                if (repetitionCount > 0) {
                    int scale = type.asPrimitiveType().getDecimalMetadata().getScale();
                    gpdbOutput.setString(colIndex, ObjectUtils.toString(
                            new BigDecimal(new BigInteger(g.getBinary(colIndex, 0).getBytes()), scale), null));
                } else {
                    gpdbOutput.setString(colIndex, null);
                }
                break;
            case BOOLEAN:
                gpdbOutput.setBoolean(colIndex, repetitionCount == 0 ? null : g.getBoolean( colIndex, 0));
                break;
            default: {
                throw new UnsupportedTypeException("Type " + primitiveType.getPrimitiveTypeName()
                        + "is not supported");
            }
        }
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
