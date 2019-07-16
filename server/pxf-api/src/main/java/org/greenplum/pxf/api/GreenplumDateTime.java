package org.greenplum.pxf.api;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Provides formatters for Greenplum DateTime
 */
public class GreenplumDateTime {

    public static final String DATETIME_FORMATTER_BASE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    /**
     * Supports date times with the format yyyy-MM-dd HH:mm:ss and
     * optional microsecond
     */
    public static final DateTimeFormatter DATETIME_FORMATTER =
            new DateTimeFormatterBuilder().appendPattern(DATETIME_FORMATTER_BASE_PATTERN)
                    // Parsing nanos in strict mode, the number of parsed digits must be between 0 and 6 (microsecond support)
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true).toFormatter();

    /**
     * Supports date times with timezone and optional microsecond
     */
    private static DateTimeFormatter optionalFormatter = new DateTimeFormatterBuilder().appendOffset("+HH:mm", "Z").toFormatter();
    public static final DateTimeFormatter DATETIME_WITH_TIMEZONE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DATETIME_FORMATTER)
                    // Make the mm optional since Greenplum will only send HH if mm == 00
                    .appendOptional(optionalFormatter)
                    .toFormatter();
}
