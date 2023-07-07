package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DecimalUtilitiesTest {
    private static final int precision = 38;
    private static final int scale = 18;
    private static final String columnName = "dec";
    private static final String decimalStringOverflowsPrecision = "1234567890123456789012345678901234567890.12345";
    private static final String decimalStringOverflowsScale = "123456789012345.12345678901234567890";
    private static final String decimalStringOverflowsPrecisionMinusScale = "123456789012345678901234567890.12345678";

    private DecimalUtilities decimalUtilities;

    @Test
    public void testParseDecimalNoOverflow() {
        String decimalString = "123.12345";
        HiveDecimal expectedHiveDecimal = HiveDecimalWritable.enforcePrecisionScale(new HiveDecimalWritable(decimalString), precision, scale).getHiveDecimal();

        // results should be the same no matter enforcePrecisionAndScaleOnIgnore is true or false
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ROUND, true);
        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalString, precision, scale, columnName);
        assertEquals(expectedHiveDecimal, hiveDecimal);

        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ROUND, false);
        hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalString, precision, scale, columnName);
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionOptionError() {
        // results should be the same no matter enforcePrecisionAndScaleOnIgnore is true or false
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ERROR, true);
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision %d.",
                decimalStringOverflowsPrecision, columnName, precision), e.getMessage());

        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ERROR, false);
        e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision %d.",
                decimalStringOverflowsPrecision, columnName, precision), e.getMessage());
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionOptionRound() {
        // results should be the same no matter enforcePrecisionAndScaleOnIgnore is true or false
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ROUND, true);
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision %d.",
                decimalStringOverflowsPrecision, columnName, precision), e.getMessage());

        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ROUND, false);
        e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision %d.",
                decimalStringOverflowsPrecision, columnName, precision), e.getMessage());
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionOptionIgnore() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.IGNORE, true);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName);
        assertNull(hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionOptionIgnoreWithoutEnforcing() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.IGNORE, false);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName);
        assertNull(hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionMinusScaleOptionError() {
        // results should be the same no matter enforcePrecisionAndScaleOnIgnore is true or false
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ERROR, true);
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision and scale (%d,%d).",
                decimalStringOverflowsPrecisionMinusScale, columnName, precision, scale), e.getMessage());

        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ERROR, false);
        e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision and scale (%d,%d).",
                decimalStringOverflowsPrecisionMinusScale, columnName, precision, scale), e.getMessage());
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionMinusScaleOptionRound() {
        // results should be the same no matter enforcePrecisionAndScaleOnIgnore is true or false
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ROUND, true);
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision and scale (%d,%d).",
                decimalStringOverflowsPrecisionMinusScale, columnName, precision, scale), e.getMessage());

        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ROUND, false);
        e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision and scale (%d,%d).",
                decimalStringOverflowsPrecisionMinusScale, columnName, precision, scale), e.getMessage());
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionMinusScaleOptionIgnore() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.IGNORE, true);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName);
        assertNull(hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionMinusScaleOptionIgnoreWithoutEnforcing() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.IGNORE, false);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName);
        HiveDecimal expectedHiveDecimal = (new HiveDecimalWritable(decimalStringOverflowsPrecisionMinusScale)).getHiveDecimal();
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalOverflowsScaleOptionError() {
        // results should be the same no matter enforcePrecisionAndScaleOnIgnore is true or false
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ERROR, true);
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported scale %s, and cannot be stored without precision loss.",
                decimalStringOverflowsScale, columnName, scale), e.getMessage());

        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ERROR, false);
        e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported scale %s, and cannot be stored without precision loss.",
                decimalStringOverflowsScale, columnName, scale), e.getMessage());
    }

    @Test
    public void testParseDecimalOverflowsScaleOptionRound() {
        HiveDecimal expectedHiveDecimal = HiveDecimalWritable.enforcePrecisionScale(new HiveDecimalWritable(decimalStringOverflowsScale), precision, scale).getHiveDecimal();

        // results should be the same no matter enforcePrecisionAndScaleOnIgnore is true or false
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ROUND, true);
        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName);
        assertEquals(expectedHiveDecimal, hiveDecimal);

        // results should be the same no matter enforcePrecisionAndScaleOnIgnore is true or false
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.ROUND, false);
        hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName);
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalOverflowsScaleOptionIgnore() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.IGNORE, true);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName);
        HiveDecimal expectedHiveDecimal = HiveDecimalWritable.enforcePrecisionScale(new HiveDecimalWritable(decimalStringOverflowsScale), precision, scale).getHiveDecimal();
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalOverflowsScaleOptionIgnoreWithoutEnforcing() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.IGNORE, false);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName);
        HiveDecimal expectedHiveDecimal = (new HiveDecimalWritable(decimalStringOverflowsScale)).getHiveDecimal();
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalOverflowsScaleOptionIgnoreWithoutEnforcing_decimalPartFailedToBorrowDigits() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.IGNORE, false);
        // integer digit count == precision - scale && decimal count > scale
        String decimalString = "12345678901234567890.12345678901234567890";

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalString, precision, scale, columnName);
        // Although we don't enforce precision and scale when parsing the decimal, decimal part failed to borrow digits from
        // integer part when decimal part overflows. Then the behavior would be the same as parsing the value with precision and scale enforced.
        HiveDecimal expectedHiveDecimal = HiveDecimalWritable.enforcePrecisionScale(new HiveDecimalWritable(decimalString), precision, scale).getHiveDecimal();
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }
}
