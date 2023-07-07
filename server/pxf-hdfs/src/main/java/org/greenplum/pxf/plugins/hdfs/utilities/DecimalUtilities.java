package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * DecimalUtilities is used for parsing decimal values according to the decimal schema and the decimal overflow options.
 * Precision and scale enforcement is dictated by the boolean 'enforcePrecisionAndScaleOnIgnore' when the decimal overflow option is 'ignore'.
 * Warning logs for different types of overflow will be logged only once if overflow happens.
 */
public class DecimalUtilities {
    private static final Logger LOG = LoggerFactory.getLogger(DecimalUtilities.class);

    private final DecimalOverflowOption decimalOverflowOption;
    private final boolean enforcePrecisionAndScaleOnIgnore;
    private boolean isPrecisionOverflowWarningLogged;
    private boolean isPrecisionAndScaleOverflowWarningLogged;

    /**
     * Construct a DecimalUtilities object carrying the decimal overflow option value,
     * and a boolean for the current profile to determine if precision and scale need to be enforced when parsing the decimal
     * if the overflow option is 'ignore'
     *
     * @param decimalOverflowOption            one of the decimal overflow options. Supported values are 'error', 'round' and 'ignore'
     * @param enforcePrecisionAndScaleOnIgnore true if current profile should enforce precision and scale when parsing the decimal
     *                                         with decimal overflow option set to 'ignore'
     */
    public DecimalUtilities(DecimalOverflowOption decimalOverflowOption, boolean enforcePrecisionAndScaleOnIgnore) {
        this.decimalOverflowOption = decimalOverflowOption;
        this.enforcePrecisionAndScaleOnIgnore = enforcePrecisionAndScaleOnIgnore;
        this.isPrecisionOverflowWarningLogged = false;
        this.isPrecisionAndScaleOverflowWarningLogged = false;
    }

    /**
     * Parse the incoming decimal string into a decimal number according to the decimal schema and the decimal overflow options.
     * There are 3 different types of overflow that PXF needs to handle when the column is defined as NUMERIC without precision and scale provided.
     * Decimal overflows on NUMERIC(precision,scale) have already been handled by GPDB and FileAccessor.
     * Case 1:
     *      Integer digit count > precision
     * Case 2:
     *      precision >= Integer digit count > precision - scale
     * Case 3:
     *      (Integer digit count <= precision - scale) && (decimal part > scale)
     *
     * @param value      is the decimal string to be parsed
     * @param precision  is the decimal precision defined in the schema
     * @param scale      is the decimal scale defined in the schema
     * @param columnName is the name of the current column
     * @return NULL or a HiveDecimal number
     */
    public HiveDecimal parseDecimalStringWithHiveDecimal(String value, int precision, int scale, String columnName) {
        switch (decimalOverflowOption) {
            case ERROR:
                return parseDecimalStringWithHiveDecimalOnError(value, precision, scale, columnName);
            case ROUND:
                return parseDecimalStringWithHiveDecimalOnRound(value, precision, scale, columnName);
            case IGNORE:
                return parseDecimalStringWithHiveDecimalOnIgnore(value, precision, scale, columnName);
            default:
                throw new UnsupportedTypeException(String.format("Unsupported decimal overflow option %s", decimalOverflowOption));
        }
    }

    /**
     * Parse the incoming decimal string into a decimal number according to the decimal schema when the decimal overflow option is 'error'.
     * PXF should error out on all the 3 overflow cases, or return a not rounded HiveDecimal which can fit within dictated precision and scale.
     *
     * @param value      is the decimal string to be parsed
     * @param precision  is the decimal precision defined in the schema
     * @param scale      is the decimal scale defined in the schema
     * @param columnName is the name of the current column
     * @return a non rounded HiveDecimal which can fit within dictated precision and scale
     */
    private HiveDecimal parseDecimalStringWithHiveDecimalOnError(String value, int precision, int scale, String columnName) {
        BigDecimal bigDecimal = new BigDecimal(value);
        // precision of the bigDecimal cannot be above designated precision
        if (bigDecimal.precision() > precision) {
            throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision %d.",
                    value, columnName, precision));
        }
        // integer digit count of the bigDecimal cannot be above designated precision and scale
        if (bigDecimal.precision() - bigDecimal.scale() > precision - scale) {
            throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision and scale (%d,%d).",
                    value, columnName, precision, scale));
        }
        // scale of the bigDecimal cannot be above designated scale
        if (bigDecimal.scale() > scale) {
            throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported scale %s, and cannot be stored without precision loss.",
                    value, columnName, scale));
        }

        // if the bigDecimal fits, then create a HiveDecimal without rounding
        HiveDecimal hiveDecimal = HiveDecimal.create(bigDecimal, false);
        if (hiveDecimal == null) {
            throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s is not supported",
                    value, columnName));
        }
        return hiveDecimal;
    }

    /**
     * Parse the incoming decimal string into a decimal number according to the decimal schema when the decimal overflow option is 'round'.
     * PXF should error out on the overflow case 1 and 2, or return a rounded HiveDecimal which can fit within dictated precision and scale.
     * Previous (before release 6.7.0) decimal parsing logic enforced precision and scale only for the PXF Parquet profile and not for the PXF ORC profile.
     * PXF now enforces precision and scale for both profiles.
     *
     * @param value      is the decimal string to be parsed
     * @param precision  is the decimal precision defined in the schema
     * @param scale      is the decimal scale defined in the schema
     * @param columnName is the name of the current column
     * @return a rounded HiveDecimal which can fit within dictated precision and scale
     */
    private HiveDecimal parseDecimalStringWithHiveDecimalOnRound(String value, int precision, int scale, String columnName) {
        // HiveDecimal.create returns a rounded value if the integer digit count of the decimal string
        // is less than or equal to Hive's maximum supported precision 38.
        // Otherwise, it returns NULL. (Overflow case 1)
        HiveDecimal hiveDecimal = HiveDecimal.create(value);
        if (hiveDecimal == null) {
            throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision %d.",
                    value, columnName, precision));
        }
        // HiveDecimal.enforcePrecisionScale returns a rounded value if the integer digit count of the decimal string
        // is less than or equal to Hive's maximum supported (precision - scale).
        // Otherwise, it returns NULL. (Overflow case 2)
        HiveDecimal hiveDecimalEnforcedPrecisionAndScale = HiveDecimal.enforcePrecisionScale(
                hiveDecimal,
                precision,
                scale);
        if (hiveDecimalEnforcedPrecisionAndScale == null) {
            throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s exceeds the maximum supported precision and scale (%d,%d).",
                    value, columnName, precision, scale));
        }
        return hiveDecimalEnforcedPrecisionAndScale;
    }

    /**
     * Parse the incoming decimal string into a decimal number according to the decimal schema when the decimal overflow option is 'ignore'.
     * PXF should error out on the overflow case 1, or return either NULL or a rounded HiveDecimal for backwards compatibilities.
     * Precision and scale will be enforced on the HiveDecimal if the previous decimal parsing logic of the current profile did so.
     * Previous (before release 6.7.0) decimal parsing logic enforced precision and scale only for the PXF Parquet profile and not for the PXF ORC profile.
     * Now the enforcement is dictated by the boolean 'enforcePrecisionAndScaleOnIgnore' and is set to true for the PXF Parquet profile, and false for the PXF ORC profile.
     *
     * @param value      is the decimal string to be parsed
     * @param precision  is the decimal precision defined in the schema
     * @param scale      is the decimal scale defined in the schema
     * @param columnName is the name of the current column
     * @return NULL or a rounded HiveDecimal with or without precision and scale enforced
     */
    private HiveDecimal parseDecimalStringWithHiveDecimalOnIgnore(String value, int precision, int scale, String columnName) {
        // HiveDecimal.create returns a rounded value if the integer digit count of the decimal string
        // is less than or equal to Hive's maximum supported precision 38.
        // Otherwise, it returns NULL. (Overflow case 1)
        HiveDecimal hiveDecimal = HiveDecimal.create(value);
        if (hiveDecimal == null) {
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
        if (enforcePrecisionAndScaleOnIgnore) {
            // HiveDecimal.enforcePrecisionScale returns a rounded value if the integer digit count of the decimal string
            // is less than or equal to Hive's maximum supported (precision - scale).
            // Otherwise, it returns NULL. (Overflow case 2)
            HiveDecimal hiveDecimalEnforcedPrecisionAndScale = HiveDecimal.enforcePrecisionScale(
                    hiveDecimal,
                    precision,
                    scale);
            if (hiveDecimalEnforcedPrecisionAndScale == null) {
                LOG.trace("The value {} for the NUMERIC column {} exceeds the maximum supported precision and scale ({},{}) and has been stored as NULL.",
                        value, columnName, precision, scale);

                if (!isPrecisionAndScaleOverflowWarningLogged) {
                    LOG.warn("There are rows where for the NUMERIC column {} the values exceed the maximum supported precision and scale ({},{}) " +
                            "and have been stored as NULL. Enable TRACE log level for row-level details.", columnName, precision, scale);
                    isPrecisionAndScaleOverflowWarningLogged = true;
                }
                return null;
            }
            // return the rounded value with precision and scale enforced.
            return hiveDecimalEnforcedPrecisionAndScale;
        }
        // return the rounded value without precision and scale enforced.
        return hiveDecimal;
    }
}
