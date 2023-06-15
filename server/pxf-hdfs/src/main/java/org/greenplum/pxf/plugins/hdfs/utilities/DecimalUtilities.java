package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * DecimalUtilities is used for parsing decimal values for PXF Parquet profile and PXF ORC profile.
 * Parsing behaviors are different based on the decimal overflow option values.
 * Warning logs for different types of overflow will be logged once if overflow happens.
 */
public class DecimalUtilities {
    private static final Logger LOG = LoggerFactory.getLogger(DecimalUtilities.class);

    private final DecimalOverflowOption decimalOverflowOption;
    private boolean isPrecisionOverflowWarningLogged;
    private boolean isIntegerDigitCountOverflowWarningLogged;
    private boolean isScaleOverflowWarningLogged;

    /**
     * Construct a DecimalUtilities object with DecimalOverflowOption information
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
     * @param value      incoming decimal string
     * @param precision  is the decimal precision defined in the schema
     * @param scale      is the decimal scale defined in the schema
     * @param columnName is the name of the current column
     * @return null or a BigDecimal number meets all the requirements
     */
    public HiveDecimal parseDecimalStringWithHiveDecimal(String value, int precision, int scale, String columnName) {
        /*
         Greenplum will handle the overflow which integer digit count (# digits on the left side of the decimal point) of
         the incoming value is greater than the precision defined in the column e.g., NUMERIC(precision).
         Therefore, the following logic is to deal with the integer digit count overflow when no precision is defined in
         the column e.g., NUMERIC.
         By default, NUMERIC will be stored as DECIMAL(38,18) in Parquet's schema, or DECIMAL(38,10) in ORC's schema.

         HiveDecimal.create has different behaviors given the integer digit count and the total digits of the incoming value.

         (1) integer digit count > precision defined in schema
         HiveDecimal.create will return a NULL value. To make the behavior consistent with Hive's behavior
         when storing on a Parquet/ORC-backed table, we store the value as null when the decimal overflow option is set to 'ignore'.
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
        // also there is Decimal and Decimal64 column vectors for ORC, see TypeUtils.createColumn
        HiveDecimal hiveDecimal = HiveDecimal.create(value);
        if (hiveDecimal == null) {
            if (decimalOverflowOption.isOptionError() || decimalOverflowOption.isOptionRound()) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds maximum precision %d.",
                        value, columnName, precision));
            }

            LOG.trace("The value {} for the NUMERIC column {} exceeds maximum precision {} and has been stored as NULL.",
                    value, columnName, precision);

            if (!isPrecisionOverflowWarningLogged) {
                LOG.warn("There are rows where for the NUMERIC column {} the values exceed maximum precision {} " +
                                "and have been stored as NULL. Enable TRACE log level for row-level details.",
                        columnName, precision);
                isPrecisionOverflowWarningLogged = true;
            }
            return null;
        }

        /*
        At this point, the integer digit count must less than or equal to the precision, but the total digits may still greater than the precision.
        HiveDecimal.enforcePrecisionScale has different behaviors given the integer digit count and the max integer digit count (precision - scale).

        (1) integer digit count > precision - scale
        HiveDecimal.enforcePrecisionScale returns NULL. To make the behavior consistent with Hive's behavior
        when storing on a Parquet/ORC-backed table, we store the value as null when the decimal overflow option is set to 'ignore'.
        For example, the column will be stored as DECIMAL(38,18). The integer digit count of 1234567890123456789012345.12345678901234567890
        is 25 which is less than 38, but it's greater than the max integer digit count 20. A null value is returned.

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

        // At this point data can fit in precision 38, but still need enforcePrecisionScale to check whether it can fit in scale 18
        HiveDecimal hiveDecimalEnforcedPrecisionAndScale = HiveDecimal.enforcePrecisionScale(
                hiveDecimal,
                precision,
                scale);

        if (hiveDecimalEnforcedPrecisionAndScale == null) {
            if (decimalOverflowOption.isOptionError() || decimalOverflowOption.isOptionRound()) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds maximum precision and scale (%d,%d).",
                        value, columnName, precision, scale));
            }

            LOG.trace("The value {} for the NUMERIC column {} exceeds maximum precision and scale ({},{}) and has been stored as NULL.",
                    value, columnName, precision, scale);

            if (!isIntegerDigitCountOverflowWarningLogged) {
                LOG.warn("There are rows where for the NUMERIC column {} the values exceed maximum precision and scale ({},{}) " +
                        "and have been stored as NULL. Enable TRACE log level for row-level details.", columnName, precision, scale);
                isIntegerDigitCountOverflowWarningLogged = true;
            }
            // if we are here, that means we are using 'ignore' option
            // if old behavior was not enforcing precision and scale, we stored the unenforced value,
            // otherwise store NULL
            return decimalOverflowOption.isStoredAsNull() ? null : hiveDecimal;
        }

        // At this point, the integer digit count must less than or equal to (precision - scale) for Parquet,
        // or less than or equal to the precision for ORC, but the total digits may still greater than precision.
        // Here is to check whether the value has been rounded off. If the decimal overflow option is set to 'error',
        // an exception will be thrown.
        BigDecimal accurateDecimal = new BigDecimal(value);
        if ((decimalOverflowOption.isOptionError() || decimalOverflowOption.isOptionRound()) && accurateDecimal.compareTo(hiveDecimalEnforcedPrecisionAndScale.bigDecimalValue()) != 0) {
            if (decimalOverflowOption.isOptionError()) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds maximum scale %s, and cannot be stored without precision loss.",
                        value, columnName, scale));
            }

            LOG.trace("The value {} for the NUMERIC column {} exceeds maximum scale {} and has been rounded off.",
                    value, columnName, scale);

            if (!isScaleOverflowWarningLogged) {
                LOG.warn("There are rows where for the NUMERIC column {} the values exceed maximum scale {} " +
                        "and have been rounded off. Enable TRACE log level for row-level details.", columnName, scale);
                isScaleOverflowWarningLogged = true;
            }
        }
        return hiveDecimalEnforcedPrecisionAndScale;
    }
}
