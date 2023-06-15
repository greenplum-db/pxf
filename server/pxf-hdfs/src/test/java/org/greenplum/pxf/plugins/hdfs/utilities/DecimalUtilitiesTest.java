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
    private static final String decimalStringOverflowsPrecisionMinusScale = "123456789012345678901234567890.12345678901234567890";
    private static final String decimalStringOverflowsScale = "123456789012345.12345678901234567890";
    private DecimalUtilities decimalUtilities;

    @Test
    public void testParseDecimalNoOverflow() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND);
        String decimalString = "123.12345";

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalString, precision, scale, columnName);
        HiveDecimal expectedHiveDecimal = HiveDecimalWritable.enforcePrecisionScale(new HiveDecimalWritable(hiveDecimal), precision, scale).getHiveDecimal();

        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionOptionError() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR);

        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds maximum precision %d.",
                decimalStringOverflowsPrecision, columnName, precision), e.getMessage());
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionOptionRound() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND);

        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds maximum precision %d.",
                decimalStringOverflowsPrecision, columnName, precision), e.getMessage());
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionOptionIgnore() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName);
        assertNull(hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionOptionIgnoreWithoutEnforcing() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecision, precision, scale, columnName);
        assertNull(hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionMinusScaleOptionError() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR);

        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds maximum precision and scale (%d,%d).",
                decimalStringOverflowsPrecisionMinusScale, columnName, precision, scale), e.getMessage());
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionMinusScaleOptionRound() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND);

        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds maximum precision and scale (%d,%d).",
                decimalStringOverflowsPrecisionMinusScale, columnName, precision, scale), e.getMessage());
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionMinusScaleOptionIgnore() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName);
        assertNull(hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsPrecisionMinusScaleOptionIgnoreWithoutEnforcing() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsPrecisionMinusScale, precision, scale, columnName);
        HiveDecimal expectedHiveDecimal = (new HiveDecimalWritable(decimalStringOverflowsPrecisionMinusScale)).getHiveDecimal();
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsScaleOptionError() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR);

        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName));
        assertEquals(String.format("The value %s for the NUMERIC column %s exceeds maximum scale %s, and cannot be stored without precision loss.",
                decimalStringOverflowsScale, columnName, scale), e.getMessage());
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsScaleOptionRound() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName);
        HiveDecimal expectedHiveDecimal = HiveDecimalWritable.enforcePrecisionScale(new HiveDecimalWritable(decimalStringOverflowsScale), precision, scale).getHiveDecimal();
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsScaleOptionIgnore() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName);
        HiveDecimal expectedHiveDecimal = HiveDecimalWritable.enforcePrecisionScale(new HiveDecimalWritable(decimalStringOverflowsScale), precision, scale).getHiveDecimal();
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsScaleOptionIgnoreWithoutEnforcing() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING);

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalStringOverflowsScale, precision, scale, columnName);
        HiveDecimal expectedHiveDecimal = (new HiveDecimalWritable(decimalStringOverflowsScale)).getHiveDecimal();
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }

    @Test
    public void testParseDecimalIntegerDigitCountOverflowsScaleOptionIgnoreWithoutEnforcing_decimalPartFailedToBorrowDigits() {
        decimalUtilities = new DecimalUtilities(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING);
        String decimalString = "12345678901234567890.12345678901234567890";

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(decimalString, precision, scale, columnName);
        HiveDecimal expectedHiveDecimal = HiveDecimalWritable.enforcePrecisionScale(new HiveDecimalWritable(decimalString), precision, scale).getHiveDecimal();
        assertEquals(expectedHiveDecimal, hiveDecimal);
    }
}
