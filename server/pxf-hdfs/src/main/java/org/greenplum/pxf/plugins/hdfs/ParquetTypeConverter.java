package org.greenplum.pxf.plugins.hdfs;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

/**
 * Converter for Parquet types and values into PXF data types and values.
 */
public enum ParquetTypeConverter {

    BINARY {
        @Override
        public DataType getDataType(Type type) {
            OriginalType originalType = type.getOriginalType();
            if (originalType == null) {
                return DataType.BYTEA;
            }
            switch (originalType) {
                case DATE: return DataType.DATE;
                case TIMESTAMP_MILLIS: return DataType.TIMESTAMP;
                default: return DataType.TEXT;
            }
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
            if (getDataType(type) == DataType.BYTEA) {
                jsonNode.add(group.getBinary(columnIndex, repeatIndex).getBytes());
            } else {
                jsonNode.add(group.getString(columnIndex, repeatIndex));
            }
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            if (getDataType(type) == DataType.BYTEA) {
                return group.getBinary(columnIndex, repeatIndex).getBytes();
            } else {
                return group.getString(columnIndex, repeatIndex);
            }
        }
    },

    INT32 {
        @Override
        public DataType getDataType(Type type) {
            OriginalType originalType = type.getOriginalType();
            if (originalType == OriginalType.INT_8 || originalType == OriginalType.INT_16) {
                return DataType.SMALLINT;
            } else {
                return DataType.INTEGER;
            }
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            Integer result = group.getInteger(columnIndex, repeatIndex);
            if (getDataType(type) == DataType.SMALLINT) {
                return Short.valueOf(result.shortValue());
            } else {
                return result;
            }
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
            jsonNode.add(group.getInteger(columnIndex, repeatIndex));
        }
    },

    INT64 {
        @Override
        public DataType getDataType(Type type) {
            return DataType.BIGINT;
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            return group.getLong(columnIndex, repeatIndex);
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
            jsonNode.add(group.getLong(columnIndex, repeatIndex));
        }
    },

    DOUBLE {
        @Override
        public DataType getDataType(Type type) {
            return DataType.FLOAT8;
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            return group.getDouble(columnIndex, repeatIndex);
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
            jsonNode.add(group.getDouble(columnIndex, repeatIndex));
        }
    },

    INT96 {
        @Override
        public DataType getDataType(Type type) {
            return DataType.TIMESTAMP;
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            return bytesToTimestamp(group.getInt96(columnIndex, repeatIndex).getBytes());
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
            Timestamp timestamp = (Timestamp) getValue(group, columnIndex, repeatIndex, type);
            jsonNode.add(timestamp.getTime());
        }
    },

    FLOAT {
        @Override
        public DataType getDataType(Type type) {
            return DataType.REAL;
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            return group.getFloat(columnIndex, repeatIndex);
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
            jsonNode.add(group.getFloat(columnIndex, repeatIndex));
        }
    },

    FIXED_LEN_BYTE_ARRAY {
        @Override
        public DataType getDataType(Type type) {
            return DataType.NUMERIC;
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            int scale = type.asPrimitiveType().getDecimalMetadata().getScale();
            return new BigDecimal(new BigInteger(group.getBinary(columnIndex, repeatIndex).getBytes()), scale);
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
            jsonNode.add((BigDecimal) getValue(group, columnIndex, repeatIndex, type));
        }
    },

    BOOLEAN {
        @Override
        public DataType getDataType(Type type) {
            return DataType.BOOLEAN;
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            return group.getBoolean(columnIndex, repeatIndex);
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
            jsonNode.add(group.getBoolean(columnIndex, repeatIndex));
        }
    };


    public static ParquetTypeConverter from(PrimitiveType primitiveType) {
        return valueOf(primitiveType.getPrimitiveTypeName().name());
    }

    // ********** PUBLIC INTERFACE **********
    public abstract DataType getDataType(Type type);
    public abstract Object getValue(Group group, int columnIndex, int repeatIndex, Type type);
    public abstract void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode);


    // Convert parquet byte array to java timestamp
    public static Timestamp bytesToTimestamp(byte[] bytes) {
        long timeOfDayNanos = ByteBuffer.wrap(new byte[]{
                bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]}).getLong();
        int julianDays = (ByteBuffer.wrap(new byte[]{bytes[11], bytes[10], bytes[9], bytes[8]})).getInt();
        long unixTimeMs = (julianDays - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY + timeOfDayNanos / 1000000;
        return new Timestamp(unixTimeMs);
    }

    // Convert epoch timestamp to byte array (INT96)
    // Inverse of the function above
    public static Binary getBinary(long timeMillis) {
        long daysSinceEpoch = timeMillis / MILLIS_IN_DAY;
        int julianDays = JULIAN_EPOCH_OFFSET_DAYS + (int) daysSinceEpoch;
        long timeOfDayNanos = (timeMillis % MILLIS_IN_DAY) * 1000000;
        return new NanoTime(julianDays, timeOfDayNanos).toBinary();
    }

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final long MILLIS_IN_DAY = 24 * 3600 * 1000;
}
