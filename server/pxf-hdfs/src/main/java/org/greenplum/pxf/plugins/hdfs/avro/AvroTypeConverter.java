package org.greenplum.pxf.plugins.hdfs.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.greenplum.pxf.api.GreenplumDateTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class AvroTypeConverter {
    /**
     * Convert an Avro timestamp-millis to GPDB timestamp with time zone value
     *
     * @param epochMillis the number of milliseconds from the unix epoch
     * @return GPDB timestamp with time zone formatted value
     */
    public static String timestampMillis(Long epochMillis) {
        return Instant.ofEpochMilli(epochMillis)
                .atZone(ZoneId.systemDefault())
                .format(GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
    }

    /**
     * Convert an Avro timestamp-micros to GPDB timestamp with time zone value
     *
     * @param epochMicros the number of microseconds from the unix epoch
     * @return GPDB timestamp with time zone formatted value
     */
    public static String timestampMicros(Long epochMicros) {
        long epochSeconds = epochMicros / 1_000_000L;
        long epochNanos = (epochMicros % 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(epochSeconds, epochNanos)
                .atZone(ZoneId.systemDefault())
                .format(GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
    }

    /**
     * Convert an Avro local-timestamp-millis to GPDB timestamp without time zone
     *
     * @param epochMillis the number of milliseconds from the unix epoch
     * @return GPDB timestamp without time zone formatted value
     */
    public static String localTimestampMillis(Long epochMillis) {
        Instant instant = Instant.ofEpochMilli(epochMillis);
        return LocalDateTime
                .ofInstant(instant, ZoneOffset.UTC)
                .format(GreenplumDateTime.DATETIME_FORMATTER);
    }

    /**
     * Convert an Avro local-timestamp-micros to GPDB timestamp without time zone
     *
     * @param epochMicros the number of microseconds from the unix epoch
     * @return GPDB timestamp without time zone formatted value
     */
    public static String localTimestampMicros(Long epochMicros) {
        long epochSeconds = epochMicros / 1_000_000L;
        long epochNanos = (epochMicros % 1_000_000L) * 1_000L;
        Instant instant = Instant.ofEpochSecond(epochSeconds, epochNanos);
        return LocalDateTime
                .ofInstant(instant, ZoneOffset.UTC)
                .format(GreenplumDateTime.DATETIME_FORMATTER);
    }

    /**
     * Convert an Avro time-micros to String without the timezone
     *
     * @param microsFromMidnight stores the number of microseconds after midnight, 00:00:00.000000
     * @return String representation of the time
     */
    public static String timeMicros(Long microsFromMidnight) {
        return LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(microsFromMidnight)).toString();
    }


    /**
     * Convert an Avro time-millis to String without the timezone
     *
     * @param millisFromMidnight stores the number of milliseconds after midnight, 00:00:00.000
     * @return String representation of the time
     */
    public static String timeMillis(Integer millisFromMidnight) {
        return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(millisFromMidnight)).toString();
    }

    /**
     * Converts an Avro date to String
     *
     * @param daysFromEpoch the number of days from the unix epoch, 1 January 1970 (ISO calendar).
     * @return String representation of the date
     */
    public static String dateFromInt(Integer daysFromEpoch) {
        return LocalDate.ofEpochDay(daysFromEpoch).toString();
    }


    /**
     * Converts to a BigDecimal value
     *
     * @param val    Object containing ByteBuffer or Fixed type data
     * @param schema Schema of the Record
     * @return BigDecimal value
     */
    public static BigDecimal convertToDecimal(Object val, Schema schema) {
        int scale = ((LogicalTypes.Decimal) schema.getLogicalType()).getScale();
        if (schema.getType() == Schema.Type.BYTES) {
            ByteBuffer value = (ByteBuffer) val;
            // always copy the bytes out because BigInteger has no offset/length ctor
            byte[] bytes = new byte[value.remaining()];
            value.duplicate().get(bytes);
            return new BigDecimal(new BigInteger(bytes), scale);
        } else {
            byte[] value = ((GenericData.Fixed) val).bytes();
            return new BigDecimal(new BigInteger(value), scale);
        }
    }
}
