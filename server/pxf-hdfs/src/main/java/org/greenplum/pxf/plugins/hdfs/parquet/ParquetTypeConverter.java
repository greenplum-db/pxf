package org.greenplum.pxf.plugins.hdfs.parquet;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.io.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;

import static org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;

/**
 * Converter for Parquet types and values into PXF data types and values.
 */
public enum ParquetTypeConverter {

    BINARY {
        @Override
        public DataType getDataType(Type type) {
            LogicalTypeAnnotation originalType = type.getLogicalTypeAnnotation();
            if (originalType == null) {
                return DataType.BYTEA;
            } else if (originalType instanceof DateLogicalTypeAnnotation) {
                return DataType.DATE;
            } else if (originalType instanceof TimestampLogicalTypeAnnotation) {
                return DataType.TIMESTAMP;
            } else {
                return DataType.TEXT;
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
            LogicalTypeAnnotation originalType = type.getLogicalTypeAnnotation();
            if (originalType instanceof DateLogicalTypeAnnotation) {
                return DataType.DATE;
            } else if (originalType instanceof DecimalLogicalTypeAnnotation) {
                return DataType.NUMERIC;
            } else if (originalType instanceof IntLogicalTypeAnnotation) {
                IntLogicalTypeAnnotation intType = (IntLogicalTypeAnnotation) originalType;
                if (intType.getBitWidth() == 8 || intType.getBitWidth() == 16) {
                    return DataType.SMALLINT;
                }
            }
            return DataType.INTEGER;
        }

        @Override
        @SuppressWarnings("deprecation")
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            int result = group.getInteger(columnIndex, repeatIndex);
            LogicalTypeAnnotation originalType = type.getLogicalTypeAnnotation();
            if (originalType instanceof DateLogicalTypeAnnotation) {
                return new org.apache.hadoop.hive.serde2.io.DateWritable(result).get(true);
            } else if (originalType instanceof DecimalLogicalTypeAnnotation) {
                return ParquetTypeConverter.bigDecimalFromLong((DecimalLogicalTypeAnnotation) originalType, result);
            } else if (originalType instanceof IntLogicalTypeAnnotation) {
                IntLogicalTypeAnnotation intType = (IntLogicalTypeAnnotation) originalType;
                if (intType.getBitWidth() == 8 || intType.getBitWidth() == 16) {
                    return (short) result;
                }
            }
            return result;
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
            jsonNode.add(group.getInteger(columnIndex, repeatIndex));
        }
    },

    INT64 {
        @Override
        public DataType getDataType(Type type) {
            if (type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation) {
                return DataType.NUMERIC;
            }
            return DataType.BIGINT;
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            long value = group.getLong(columnIndex, repeatIndex);
            if (type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation) {
                return ParquetTypeConverter
                        .bigDecimalFromLong((DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation(), value);
            }
            return value;
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
            String timestamp = (String) getValue(group, columnIndex, repeatIndex, type);
            jsonNode.add(timestamp);
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
            int scale = ((DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getScale();
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
    },

    LIST {
        @Override
        public DataType getDataType(Type type) {
            try {
                PrimitiveType elementType = type.asGroupType().getType(0).asGroupType().getType(0).asPrimitiveType();
                LogicalTypeAnnotation logicalTypeAnnotation = elementType.getLogicalTypeAnnotation();
                switch (elementType.getPrimitiveTypeName()) {
                    case INT64:
                        return DataType.INT8ARRAY;
                    case INT32:
                        if (logicalTypeAnnotation instanceof DateLogicalTypeAnnotation) {
                            return DataType.DATEARRAY;
                        } else if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation) {
                            return DataType.NUMERICARRAY;
                        } else if (logicalTypeAnnotation instanceof IntLogicalTypeAnnotation) {
                            IntLogicalTypeAnnotation intLogicalTypeAnnotation = (IntLogicalTypeAnnotation) logicalTypeAnnotation;
                            if (intLogicalTypeAnnotation.getBitWidth() == 8 || intLogicalTypeAnnotation.getBitWidth() == 16) {
                                return DataType.INT2ARRAY;
                            }
                        }
                        return DataType.INT4ARRAY;
                    case BOOLEAN:
                        return DataType.BOOLARRAY;
                    case BINARY:
                        if (logicalTypeAnnotation == null) {
                            return DataType.BYTEAARRAY;
                        } else if (logicalTypeAnnotation instanceof DateLogicalTypeAnnotation) {
                            return DataType.DATEARRAY;
                        } else if (logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation) {
                            return DataType.TIMESTAMPARRAY;
                        } else {
                            return DataType.TEXTARRAY;
                        }
                    case FLOAT:
                        return DataType.FLOAT4ARRAY;
                    case DOUBLE:
                        return DataType.FLOAT8ARRAY;
                    case INT96:
                        return DataType.TIMESTAMPARRAY;
                    case FIXED_LEN_BYTE_ARRAY:
                        return DataType.NUMERICARRAY;
                    default:
                        throw new IOException("Not supported type " + elementType.getPrimitiveTypeName());
                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }

            return null;
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
            return group.getGroup(columnIndex, repeatIndex);
        }

        //todo: what should be the format of converting a List to json array?
        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
//            jsonNode.add(group.Group(columnIndex, repeatIndex));
        }
    };


    private static final int SECOND_IN_MICROS = 1000 * 1000;
    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588L;
    private static final long MILLIS_IN_DAY = 24 * 3600 * 1000;
    private static final long MICROS_IN_DAY = 24 * 3600 * 1000 * 1000L;
    private static final long NANOS_IN_MICROS = 1000;
    private static final Logger LOG = LoggerFactory.getLogger(ParquetTypeConverter.class);

    public static ParquetTypeConverter from(PrimitiveType primitiveType) {
        return valueOf(primitiveType.getPrimitiveTypeName().name());
    }

    public static ParquetTypeConverter from(GroupType type) {
        return valueOf(type.getOriginalType().name());
    }

    // Convert parquet byte array to java timestamp IN LOCAL SERVER'S TIME ZONE
    public static String bytesToTimestamp(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        long timeOfDayNanos = byteBuffer.getLong();
        long julianDay = byteBuffer.getInt();
        long unixTimeMs = (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;

        Instant instant = Instant.ofEpochMilli(unixTimeMs); // time read from Parquet is in UTC
        instant = instant.plusNanos(timeOfDayNanos);
        String timestamp = instant.atZone(ZoneId.systemDefault()).format(GreenplumDateTime.DATETIME_FORMATTER);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Converted bytes: {} to date: {} from: julianDays {}, timeOfDayNanos {}, unixTimeMs {}",
                    Base64.getEncoder().encodeToString(bytes),
                    timestamp, julianDay, timeOfDayNanos, unixTimeMs);
        }
        return timestamp;
    }

    /**
     * Parses the dateString and returns the number of days between the unix
     * epoch (1 January 1970) until the given date in the dateString
     */
    public static int getDaysFromEpochFromDateString(String dateString) {
        LocalDate date = LocalDate.parse(dateString, GreenplumDateTime.DATE_FORMATTER);
        return (int) date.toEpochDay();
    }

    /**
     * Converts a timestamp string to a INT96 byte array.
     * Supports microseconds for timestamps
     */
    public static Binary getBinaryFromTimestamp(String timestampString) {
        // We receive a timestamp string from GPDB in the server timezone
        // We convert it to an instant of the current server timezone
        LocalDateTime date = LocalDateTime.parse(timestampString, GreenplumDateTime.DATETIME_FORMATTER);
        ZonedDateTime zdt = ZonedDateTime.of(date, ZoneId.systemDefault());
        return getBinaryFromZonedDateTime(timestampString, zdt);
    }

    /**
     * Converts a "timestamp with time zone" string to a INT96 byte array.
     * Supports microseconds for timestamps
     *
     * @param timestampWithTimeZoneString the greenplum string of the timestamp with the time zone
     * @return Binary format of the timestamp with time zone string
     */
    public static Binary getBinaryFromTimestampWithTimeZone(String timestampWithTimeZoneString) {
        OffsetDateTime date = OffsetDateTime.parse(timestampWithTimeZoneString, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
        ZonedDateTime zdt = date.toZonedDateTime();
        return getBinaryFromZonedDateTime(timestampWithTimeZoneString, zdt);
    }

    // Helper method that takes a ZonedDateTime object and return it as nano time in binary form (UTC)
    private static Binary getBinaryFromZonedDateTime(String timestampString, ZonedDateTime zdt) {
        long timeMicros = (zdt.toEpochSecond() * SECOND_IN_MICROS) + zdt.getNano() / NANOS_IN_MICROS;
        long daysSinceEpoch = timeMicros / MICROS_IN_DAY;
        int julianDays = (int) (JULIAN_EPOCH_OFFSET_DAYS + daysSinceEpoch);
        long timeOfDayNanos = (timeMicros % MICROS_IN_DAY) * NANOS_IN_MICROS;
        LOG.debug("Converted timestamp: {} to julianDays: {}, timeOfDayNanos: {}", timestampString, julianDays, timeOfDayNanos);
        return new NanoTime(julianDays, timeOfDayNanos).toBinary();
    }

    // Helper method that returns a BigDecimal from the long value
    private static BigDecimal bigDecimalFromLong(DecimalLogicalTypeAnnotation decimalType, long value) {
        return new BigDecimal(BigInteger.valueOf(value), decimalType.getScale());
    }

    // ********** PUBLIC INTERFACE **********
    public abstract DataType getDataType(Type type);

    public abstract Object getValue(Group group, int columnIndex, int repeatIndex, Type type);

    public abstract void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode);
}
