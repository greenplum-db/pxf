package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * DecimalUtilities is used for parsing decimal values for PXF Parquet profile and PXF ORC profile.
 * Parsing behaviors are depending on the decimal overflow option values.
 * Warning logs for different types of overflows will be logged only once if overflows happen.
 */
public class DecimalUtilities {
    private static final Logger LOG = LoggerFactory.getLogger(DecimalUtilities.class);

    private final DecimalOverflowOption decimalOverflowOption;
    private boolean isPrecisionOverflowWarningLogged;
    private boolean isIntegerDigitCountOverflowWarningLogged;
    private boolean isScaleOverflowWarningLogged;

    /**
     * Construct a DecimalUtilities object with a DecimalOverflowOption object
     *
     * @param decimalOverflowOption is parsed by DecimalOverflowOption.parseDecimalOverflowOption
     */
    public DecimalUtilities(DecimalOverflowOption decimalOverflowOption) {
        this.decimalOverflowOption = decimalOverflowOption;
        this.isPrecisionOverflowWarningLogged = false;
        this.isIntegerDigitCountOverflowWarningLogged = false;
        this.isScaleOverflowWarningLogged = false;
    }

    /**
     * Parse the incoming decimal string into a decimal number for ORC or Parquet profiles according to the decimal overflow options
     *
     * @param value      is the decimal string going to be parsed
     * @param precision  is the decimal precision defined in the schema
     * @param scale      is the decimal scale defined in the schema
     * @param columnName is the name of the current column
     * @return null or a HiveDecimal number
     */
    public HiveDecimal parseDecimalStringWithHiveDecimal(String value, int precision, int scale, String columnName) {
        /*
         Greenplum will handle the overflow which integer digit count (# digits on the left side of the decimal point) of
         the incoming value is greater than the precision defined in the column e.g., NUMERIC(precision).
         Therefore, the following logic is to deal with the integer digit count overflow when no precision is defined in
         the column e.g., NUMERIC.
         By default, NUMERIC will be stored as DECIMAL(38,18) in PXF Parquet's schema, or DECIMAL(38,10) in PXF ORC's schema.

         HiveDecimal.create has different behaviors given the integer digit count and the total digits of the incoming value.

         (1) integer digit count > precision defined in schema
         HiveDecimal.create will return a NULL value. To make the behavior consistent with Hive's behavior
         when storing on a Parquet/ORC-backed table, we store the value as null when the decimal overflow option is set to 'ignore',
         and we throw out an error when the decimal overflow option is set to 'error' or 'round'.
         For example, the integer digit count of 1234567890123456789012345678901234567890.123 is 40,
         which is greater than 38. HiveDecimal.create returns null.

         (2) (Integer digit count <= precision defined in schema) && (total digits of the value > precision defined in schema)
         HiveDecimal.create will return a rounded-off value to fit in the Hive maximum supported precision 38.
         For example, the integer digit count of 1234567890123456789012345.12345678901234567890 is 25 which is less than 38,
         but its overall precision is 45 which is greater than 38.
         Then data will be created as a rounded value 1234567890123456789012345.1234567890123

         (3) (Integer digit count <= precision defined in schema) && (total digits of the value <= precision defined in schema)
         HiveDecimal.create will return the decimal value as-is.
         For example, 123456.123456 can fit in DECIMAL(38,18) without any data loss.
         */

        // HiveDecimal.create will return a decimal value which can fit in DECIMAL(38)
        HiveDecimal hiveDecimal = HiveDecimal.create(value);
        if (hiveDecimal == null) {
            if (!decimalOverflowOption.isOptionIgnore()) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision %d.",
                        value, columnName, precision));
            }

            LOG.trace("The value {} for the NUMERIC column {} exceeds the maximum supported precision {} and has been stored as NULL.",
                    value, columnName, precision);

            if (!isPrecisionOverflowWarningLogged) {
                LOG.warn("There are rows where for the NUMERIC column {} the values exceed the maximum supported precision {} " +
                                "and have been stored as NULL. Enable TRACE log level for row-level details.",
                        columnName, precision);
                isPrecisionOverflowWarningLogged = true;
            }
            return null;
        }

        /*
        At this point, the integer digit count must less than or equal to the precision, but the total digits may still
        be greater than the precision value.
        HiveDecimal.enforcePrecisionScale has different behaviors given the integer digit count and the max integer digit count (precision - scale).

        (1) integer digit count > precision - scale
        HiveDecimal.enforcePrecisionScale returns NULL. In previous version of PXF, writing data with ORC profile
        did not enforce precision and scale whereas Parquet did. For backward compatibility,
        when storing on a Parquet-backed table, we store the value as null when the decimal overflow option is set to 'ignore';
        when storing on a ORC-backed table, we store a rounded-off value without enforcing precision and scale when the decimal overflow option is set to 'ignore';
        when the decimal overflow option is set to 'error' or 'round', PXF will throw out an error.
        For example, the column will be stored as DECIMAL(38,18). The integer digit count of 1234567890123456789012345.12345678901234567890
        is 25 which is less than 38, but it's greater than the max integer digit count 20. A null value is returned.
        For Parquet-backed table, we store NULL; for ORC-backed table, we store 1234567890123456789012345.1234567890123.

        (2) (integer digit count <= precision - scale) && (total digits of the value > precision defined in schema)
        HiveDecimal.enforcePrecisionScale will return a rounded-off value to fit in the precision and scale.
        For example, the column will be stored as DECIMAL(38,18). The integer digit count of 1234567890.123456789012345678901234567890
        is 10 which is less than 20, but total digits is 40 which is greater than 38. The value will be stored as a rounded one
        1234567890.1234567890123456789012345679

        (3) (integer digit count <= precision - scale) && (total digits of the value <= precision defined in schema)
        HiveDecimal.enforcePrecisionScale will return exactly the decimal value as-is.
        For example, 123456.123456 can fit in DECIMAL
        (38,18) without any data loss.

        Current logic for ORC doesn't call HiveDecimal.enforcePrecisionScale but Parquet does. That will bring in the
        inconsistency in the error messages in the third check.
         */

        // HiveDecimal.enforcePrecisionScale will return a decimal value which can fit in DECIMAL(38,18) for Parquet profile,
        // or DECIMAL(38,10) for ORC profile
        HiveDecimal hiveDecimalEnforcedPrecisionAndScale = HiveDecimal.enforcePrecisionScale(
                hiveDecimal,
                precision,
                scale);

        if (hiveDecimalEnforcedPrecisionAndScale == null) {
            if (!decimalOverflowOption.isOptionIgnore()) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision and scale (%d,%d).",
                        value, columnName, precision, scale));
            }

            LOG.trace("The value {} for the NUMERIC column {} exceeds the maximum supported precision and scale ({},{}) and has been stored as NULL.",
                    value, columnName, precision, scale);

            if (!isIntegerDigitCountOverflowWarningLogged) {
                LOG.warn("There are rows where for the NUMERIC column {} the values exceed the maximum supported precision and scale ({},{}) " +
                        "and have been stored as NULL. Enable TRACE log level for row-level details.", columnName, precision, scale);
                isIntegerDigitCountOverflowWarningLogged = true;
            }
            /*
             if we are here, that means we are using 'ignore' option
             if previous behavior was enforcing precision and scale, we stored the value as NULL,
             otherwise store the unenforced value
             */
            return decimalOverflowOption.wasEnforcedPrecisionAndScale() ? null : hiveDecimal;
        }

        /*
        At this point, the integer digit count must less than or equal to (precision - scale),
        but the total digits may still be greater than precision.
        Here, check whether the value has been rounded off. If the decimal overflow option is set to 'error', an exception will be thrown.
         */
        BigDecimal accurateDecimal = new BigDecimal(value);
        if (!decimalOverflowOption.isOptionIgnore() && accurateDecimal.compareTo(hiveDecimalEnforcedPrecisionAndScale.bigDecimalValue()) != 0) {
            if (decimalOverflowOption.isOptionError()) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported scale %s, and cannot be stored without precision loss.",
                        value, columnName, scale));
            }

            LOG.trace("The value {} for the NUMERIC column {} exceeds the maximum supported scale {} and has been rounded off.",
                    value, columnName, scale);

            if (!isScaleOverflowWarningLogged) {
                LOG.warn("There are rows where for the NUMERIC column {} the values exceed the maximum supported scale {} " +
                        "and have been rounded off. Enable TRACE log level for row-level details.", columnName, scale);
                isScaleOverflowWarningLogged = true;
            }
        }
        /*
        If we are here, that means we are using 'round' or 'ignore' option.
        If the previous behavior of the current profile enforced precision and scale, when scale overflow happens, the decimal part will be rounded
        If the previous behavior of the current profile did not enforce precision and scale,
        when scale overflow happens, if the decimal part can borrow digit slots from the integer part, the decimal part will not be rounded.
        For example, the column will be stored as DECIMAL(38,18). The integer digit count of 123456789012345.1234567890123456789
        is 15 which is less than 20, the decimal digit count is 19 which is greater than 18.
        If precision and scale are enforced, the value should be rounded as 123456789012345.123456789012345679,
        otherwise the decimal part can borrow 1 digit slot from the integer part, and the value will not be rounded.
         */
        return decimalOverflowOption.wasEnforcedPrecisionAndScale() ? hiveDecimalEnforcedPrecisionAndScale : hiveDecimal;
    }
}
