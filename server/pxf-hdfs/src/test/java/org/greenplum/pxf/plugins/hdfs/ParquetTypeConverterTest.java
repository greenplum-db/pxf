package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.io.api.Binary;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ParquetTypeConverterTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testStringConversionRoundTrip() {
        String timestamp = "2019-03-14 20:52:48.123456";
        Binary binary = ParquetTypeConverter.getBinaryFromTimestamp(timestamp);
        String convertedTimestamp = ParquetTypeConverter.bytesToTimestamp(binary.getBytes());

        assertEquals(timestamp, convertedTimestamp);
    }

    @Test
    public void testBinaryConversionRoundTrip() {
        // 2019-03-14 21:22:05.987654
        byte[] source = new byte[]{112, 105, -24, 125, 77, 14, 0, 0, -66, -125, 37, 0};
        String timestamp = ParquetTypeConverter.bytesToTimestamp(source);
        Binary binary = ParquetTypeConverter.getBinaryFromTimestamp(timestamp);

        assertArrayEquals(source, binary.getBytes());
    }

    @Test
    public void testUnsupportedNanoSeconds() {
        thrown.expect(DateTimeParseException.class);
        thrown.expectMessage("Text '2019-03-14 20:52:48.1234567' could not be parsed, unparsed text found at index 26");
        String timestamp = "2019-03-14 20:52:48.1234567";
        ParquetTypeConverter.getBinaryFromTimestamp(timestamp);
    }

    @Test
    public void testBinaryWithNanos() {
        Instant instant = Instant.parse("2019-03-15T03:52:48.123456Z"); // UTC
        ZonedDateTime localTime = instant.atZone(ZoneId.systemDefault());
        String expected = localTime.format(GreenplumDateTime.DATETIME_FORMATTER); // should be "2019-03-14 20:52:48.123456" in PST

        byte[] source = new byte[]{0, 106, 9, 53, -76, 12, 0, 0, -66, -125, 37, 0}; // represents 2019-03-14 20:52:48.1234567
        String timestamp = ParquetTypeConverter.bytesToTimestamp(source); // nanos get dropped
        assertEquals(expected, timestamp);
    }

    @Test
    public void testTimestampWithTimezoneStringConversionRoundTrip() {
        String expectedTimestampInUTC = "2016-06-22 02:06:25";
        String expectedTimestampInSystemTimeZone = convertUTCToCurrentSystemTimeZone(expectedTimestampInUTC, GreenplumDateTime.DATETIME_FORMATTER);

        // Conversion roundtrip for test input (timestamp)
        String timestamp = "2016-06-21 22:06:25-04";
        Binary binary = ParquetTypeConverter.getBinaryFromTimestampWithTimeZone(timestamp);
        String convertedTimestamp = ParquetTypeConverter.bytesToTimestamp(binary.getBytes());

        assertEquals(expectedTimestampInSystemTimeZone, convertedTimestamp);
    }

    @Test
    public void testTimestampWithTimezoneWithMicrosecondsStringConversionRoundTrip() {
        // Case 1
        String expectedTimestampInUTC = "2019-07-11 01:54:53.523485";
        // We're using expectedTimestampInSystemTimeZone as expected string for testing as the timestamp is expected to be converted to system's local time
        String expectedTimestampInSystemTimeZone = convertUTCToCurrentSystemTimeZone(expectedTimestampInUTC, GreenplumDateTime.DATETIME_FORMATTER);

        // Conversion roundtrip for test input (timestamp); (test input will lose time zone information but remain correct value, and test against expectedTimestampInSystemTimeZone)
        String timestamp = "2019-07-10 21:54:53.523485-04";
        Binary binary = ParquetTypeConverter.getBinaryFromTimestampWithTimeZone(timestamp);
        String convertedTimestamp = ParquetTypeConverter.bytesToTimestamp(binary.getBytes());

        assertEquals(expectedTimestampInSystemTimeZone, convertedTimestamp);

        // Case 2
        String expectedTimestampInUTC2 = "2019-07-10 18:54:47.354795";
        String expectedTimestampInSystemTimeZone2 = convertUTCToCurrentSystemTimeZone(expectedTimestampInUTC2, GreenplumDateTime.DATETIME_FORMATTER);

        // Conversion roundtrip for test input (timestamp)
        String timestamp2 = "2019-07-11 07:39:47.354795+12:45";
        Binary binary2 = ParquetTypeConverter.getBinaryFromTimestampWithTimeZone(timestamp2);
        String convertedTimestamp2 = ParquetTypeConverter.bytesToTimestamp(binary2.getBytes());

        assertEquals(expectedTimestampInSystemTimeZone2, convertedTimestamp2);
    }

    // Helper function
    private String convertUTCToCurrentSystemTimeZone(String expectedUTC, DateTimeFormatter formatter) {
        // convert expectedUTC string to ZonedDateTime zdt
        LocalDateTime date = LocalDateTime.parse(expectedUTC, formatter);
        ZonedDateTime zdt = ZonedDateTime.of(date, ZoneOffset.UTC);
        // convert zdt to Current Zone ID
        ZonedDateTime systemZdt = zdt.withZoneSameInstant(ZoneId.systemDefault());
        // convert date to string representation
        return systemZdt.format(GreenplumDateTime.DATETIME_FORMATTER);
    }

}
