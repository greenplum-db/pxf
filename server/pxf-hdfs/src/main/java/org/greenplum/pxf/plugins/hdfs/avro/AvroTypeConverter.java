package org.greenplum.pxf.plugins.hdfs.avro;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericFixed;
import org.greenplum.pxf.api.GreenplumDateTime;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

public class AvroTypeConverter {
    private final TimeConversions.DateConversion dateConversion;
    private final TimeConversions.LocalTimestampMicrosConversion localTimestampMicrosConversion;
    private final TimeConversions.LocalTimestampMillisConversion localTimestampMillisConversion;
    private final TimeConversions.TimeMicrosConversion timeMicrosConversion;
    private final TimeConversions.TimeMillisConversion timeMillisConversion;
    private final TimeConversions.TimestampMicrosConversion timestampMicrosConversion;
    private final TimeConversions.TimestampMillisConversion timestampMillisConversion;
    private final Conversions.DecimalConversion decimalConversion;

    private AvroTypeConverter() {
        // private constructor
        dateConversion = new TimeConversions.DateConversion();
        localTimestampMicrosConversion = new TimeConversions.LocalTimestampMicrosConversion();
        localTimestampMillisConversion = new TimeConversions.LocalTimestampMillisConversion();
        timeMicrosConversion = new TimeConversions.TimeMicrosConversion();
        timeMillisConversion = new TimeConversions.TimeMillisConversion();
        timestampMicrosConversion = new TimeConversions.TimestampMicrosConversion();
        timestampMillisConversion = new TimeConversions.TimestampMillisConversion();
        decimalConversion = new Conversions.DecimalConversion();
    }

    public static AvroTypeConverter getInstance() {
        return AvroTypeConverterHolder.INSTANCE;
    }

    /**
     * Convert an Avro timestamp-millis to GPDB timestamp with time zone value
     *
     * @param epochMillis the number of milliseconds from the unix epoch
     * @return GPDB timestamp with time zone formatted value
     */
    public String timestampMillis(Long epochMillis, Schema schema, LogicalType type) {
        Instant instant = timestampMillisConversion.fromLong(epochMillis, schema, type);
        return instant.atZone(ZoneId.systemDefault()).format(GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
    }

    /**
     * Convert an Avro timestamp-micros to GPDB timestamp with time zone value
     *
     * @param epochMicros the number of microseconds from the unix epoch
     * @return GPDB timestamp with time zone formatted value
     */
    public String timestampMicros(Long epochMicros, Schema schema, LogicalType type) {
        Instant instant = timestampMicrosConversion.fromLong(epochMicros, schema, type);
        return instant.atZone(ZoneId.systemDefault()).format(GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
    }

    /**
     * Convert an Avro local-timestamp-millis to GPDB timestamp without time zone
     *
     * @param epochMillis the number of milliseconds from the unix epoch
     * @return GPDB timestamp without time zone formatted value
     */
    public String localTimestampMillis(Long epochMillis, Schema schema, LogicalType type) {
        LocalDateTime localDateTime = localTimestampMillisConversion.fromLong(epochMillis, schema, type);
        return localDateTime.format(GreenplumDateTime.DATETIME_FORMATTER);
    }

    /**
     * Convert an Avro local-timestamp-micros to GPDB timestamp without time zone
     *
     * @param epochMicros the number of microseconds from the unix epoch
     * @return GPDB timestamp without time zone formatted value
     */
    public String localTimestampMicros(Long epochMicros, Schema schema, LogicalType type) {
        LocalDateTime localDateTime = localTimestampMicrosConversion.fromLong(epochMicros, schema, type);
        return localDateTime.format(GreenplumDateTime.DATETIME_FORMATTER);
    }

    /**
     * Convert an Avro time-micros to String without the timezone
     *
     * @param microsFromMidnight stores the number of microseconds after midnight, 00:00:00.000000
     * @return String representation of the time
     */
    public String timeMicros(Long microsFromMidnight, Schema schema, LogicalType type) {
        return timeMicrosConversion.fromLong(microsFromMidnight, schema, type).toString();
    }

    /**
     * Convert an Avro time-millis to String without the timezone
     *
     * @param millisFromMidnight stores the number of milliseconds after midnight, 00:00:00.000
     * @return String representation of the time
     */
    public String timeMillis(Integer millisFromMidnight, Schema schema, LogicalType type) {
        LocalTime localTime = timeMillisConversion.fromInt(millisFromMidnight, schema, type);
        return localTime.toString();
    }

    /**
     * Converts an Avro date to String
     *
     * @param daysFromEpoch the number of days from the unix epoch, 1 January 1970 (ISO calendar).
     * @return String representation of the date
     */
    public String dateFromInt(Integer daysFromEpoch, Schema schema, LogicalType type) {
        LocalDate localDate = dateConversion.fromInt(daysFromEpoch, schema, type);
        return localDate.toString();
    }

    /**
     * Converts to a BigDecimal value
     *
     * @param val    Object containing ByteBuffer or Fixed type data
     * @param schema Schema of the Record
     * @return BigDecimal value
     */
    public BigDecimal convertToDecimal(Object val, Schema schema, LogicalType logicalType) {
        if (schema.getType() == Schema.Type.BYTES) {
            ByteBuffer value = (ByteBuffer) val;
            return decimalConversion.fromBytes(value, schema, logicalType);
        } else {
            GenericFixed value = (GenericFixed) val;
            return decimalConversion.fromFixed(value, schema, logicalType);
        }
    }

    private static class AvroTypeConverterHolder {
        private static final AvroTypeConverter INSTANCE = new AvroTypeConverter();
    }
}
