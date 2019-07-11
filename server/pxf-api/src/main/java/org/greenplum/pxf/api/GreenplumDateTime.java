package org.greenplum.pxf.api;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class GreenplumDateTime {

    public static final String DATETIME_FORMATTER_BASE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DATETIME_FORMATTER =
            new DateTimeFormatterBuilder().appendPattern(DATETIME_FORMATTER_BASE_PATTERN)
                    // Parsing nanos in strict mode, the number of parsed digits must be between 0 and 6 (millisecond support)
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true).toFormatter();
}
