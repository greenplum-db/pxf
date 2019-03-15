package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.io.api.Binary;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

}