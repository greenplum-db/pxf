package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class DecimalUtilities {
    private static final String PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ERROR = "error";
    private static final String PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ROUND = "round";
    private static final String PXF_WRITE_DECIMAL_OVERFLOW_OPTION_IGNORE = "ignore";
    private static final Logger LOG = LoggerFactory.getLogger(DecimalUtilities.class);

    private final String profile;
    private boolean isDecimalOverflowOptionError;
    private boolean isDecimalOverflowOptionRound;
    private boolean isPrecisionOverflowWarningLogged;
    private boolean isIntegerDigitCountOverflowWarningLogged;
    private boolean isScaleOverflowWarningLogged;

    /**
     *
     * @param profile can be ORC or Parquet
     */
    public DecimalUtilities(String profile) {
        this.profile = profile;
        this.isDecimalOverflowOptionError = false;
        this.isDecimalOverflowOptionRound = false;
        this.isPrecisionOverflowWarningLogged = false;
        this.isIntegerDigitCountOverflowWarningLogged = false;
        this.isScaleOverflowWarningLogged = false;
    }

    /**
     * Sets configuration variables based on server configuration properties of pxf.parquet.write.decimal.overflow.
     * *
     * * @param configuration
     * * @param pxfWriteDecimalOverflowPropertyName
     *
     * @param configuration                       contains server configuration properties
     * @param profile                             can be ORC or Parquet
     * @param pxfWriteDecimalOverflowPropertyName is the property name in pxf-site.xml
     */
    public void parseDecimalOverflowOption(Configuration configuration, String profile, String pxfWriteDecimalOverflowPropertyName) {
        String defaultOption = pxfWriteDecimalOverflowPropertyName.equalsIgnoreCase("orc") ? PXF_WRITE_DECIMAL_OVERFLOW_OPTION_IGNORE : PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ROUND;
        String decimalOverflowOption = configuration.get(pxfWriteDecimalOverflowPropertyName, defaultOption).toLowerCase();
        switch (decimalOverflowOption) {
            case PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ERROR:
                isDecimalOverflowOptionError = true;
                break;
            case PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ROUND:
                isDecimalOverflowOptionRound = true;
                break;
            case PXF_WRITE_DECIMAL_OVERFLOW_OPTION_IGNORE:
                break;
            default:
                throw new UnsupportedTypeException(String.format("Invalid configuration value %s for " +
                        "pxf.%s.write.decimal.overflow. Valid values are error, round, and ignore.", decimalOverflowOption, profile.toLowerCase()));
        }
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
    public BigDecimal parseDecimalString(String value, int precision, int scale, String columnName) {
        /*
         When this column is defined as NUMERIC(precision,scale) in Greenplum, Greenplum will handle writing values with overflow.
         When this column is defined as NUMERIC in Greenplum, the column type will be treated as DECIMAL(38,18) for Parquet, or DECIMAL(38,10) for ORC.

         For ORC, in ORC code base the max precision for decimal is set to 38 which is consistent with Hive,
         (https://github.com/apache/orc/blob/de5831ffbc72e872f61b0afd9683b77bd09c6c61/java/core/src/java/org/apache/orc/TypeDescription.java#L39)
         so we parse the value into ORC format with HiveDecimal.

         For Parquet, there is no decimal precision limitation, and the maximum supported decimal precision and scale are pretty large in postgres,
         (https://www.postgresql.org/docs/9.3/datatype-numeric.html#DATATYPE-NUMERIC-TABLE)
         so we parse the value into Parquet with BigDecimal instead of HiveDecimal when precision is greater than Hive's max precision 38
         */

        if (StringUtils.equalsIgnoreCase(profile, "parquet")) {
            //TODO: uncomment this part when working on the complete numeric fix for Parquet
//            if (precision > HiveDecimal.MAX_PRECISION) {
//                return parseDecimalStringWithBigDecimal();
//            }
            return parseDecimalStringWithHiveDecimal(value, precision, scale, true, columnName);
        }
        return parseDecimalStringWithHiveDecimal(value, precision, scale, false, columnName);
    }

    /**
     * Parse the incoming decimal string into a decimal number for ORC or Parquet profiles according to the decimal overflow options
     *
     * @param value                    incoming decimal string
     * @param precision                is the decimal precision defined in the schema
     * @param scale                    is the decimal scale defined in the schema
     * @param enforcePrecisionAndScale decides whether to enforce the decimal with the given precision and scale
     * @param columnName               is the name of the current column
     * @return null or a BigDecimal number meets all the requirements
     */
    private BigDecimal parseDecimalStringWithHiveDecimal(String value, int precision, int scale, boolean enforcePrecisionAndScale, String columnName) {
        /*
         Greenplum will handle the overflow which integer digit count (# digits to the left side of the decimal point) of
         the incoming value is greater than the precision defined in the column e.g., NUMERIC(precision).
         Therefore, the following part is to deal with the integer digit count overflow when no precision is defined in
         the column e.g., NUMERIC.
         By default, NUMERIC will be stored as DECIMAL(38,18) in Parquet's schema, or DECIMAL(38,10) in ORC's schema.

         HiveDecimal.create has different behaviors depends on whether there is an integer digit count overflow and the
         total digits of the incoming value.

         (1) integer digit count > precision defined in schema
         HiveDecimal.create will return a NULL value. To make the behavior consistent with Hive's behavior
         when storing on a Parquet/ORC-backed table, we store the value as null.
         For example, the integer digit count of 1234567890123456789012345678901234567890.123 is 40,
         which is greater than 38. HiveDecimal.create returns null.

         (2) (Integer digit count <= precision defined in schema) && (total digits of the value > precision defined in schema)
         HiveDecimal.create will return a rounded-off value to fit in the Hive maximum supported precision 38.
         For example, the integer digit count of 1234567890123456789012345.12345678901234567890 is 25 which is less than 38,
         but its overall precision is 45 which is greater than 38.
         Then data will be created as a rounded value 1234567890123456789012345.1234567890123

         (3) (Integer digit count <= precision defined in schema) && (total digits of the value <= precision defined in schema)
         HiveDecimal.create will return exactly the same decimal value as provided.
         For example, 123456.123456 can fit in DECIMAL(38,18) without any data loss.
         */

        // HiveDecimal.create will return a decimal value which can fit in DECIMAL(38)
        // also there is Decimal and Decimal64 column vectors for ORC, see TypeUtils.createColumn
        HiveDecimal hiveDecimal = HiveDecimal.create(value);
        if (hiveDecimal == null) {
            if (isDecimalOverflowOptionError || isDecimalOverflowOptionRound) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s using %s profile exceeds maximum precision %d.",
                        value, columnName, profile, precision));
            }

            LOG.trace("The value {} for the NUMERIC column {} using {} profile exceeds maximum precision {} and has been stored as NULL.",
                    value, columnName, profile, precision);

            if (!isPrecisionOverflowWarningLogged) {
                LOG.warn("There are rows where for the NUMERIC column {} using {} profile the values exceed maximum precision {} " +
                                "and have been stored as NULL. Enable TRACE log level for row-level details.",
                        columnName, profile, precision);
                isPrecisionOverflowWarningLogged = true;
            }
            return null;
        }

        /*
        At this point, the integer digit count must less than precision, but the total digits may still greater than precision.
        HiveDecimal.enforcePrecisionScale has different behaviors depends on the integer digit count and the max integer digit count (precision - scale).

        (1) integer digit count > precision - scale
        HiveDecimal.enforcePrecisionScale returns NULL. For example, the column will be stored as DECIMAL(38,18).
        The integer digit count of 1234567890123456789012345.12345678901234567890 is 25 which is less than 38, but it's greater
        than the max integer digit count 20. A null value is returned.

        (2) (integer digit count <= precision - scale) && (total digits of the value > precision defined in schema)
        HiveDecimal.enforcePrecisionScale will return a rounded-off value to fit in the precision and scale.
        For example, the column will be stored as DECIMAL(38,18). The integer digit count of 1234567890.123456789012345678901234567890
        is 10 which is less than 20, but total digits is 40 which is greater than 38. The value will be stored as a rounded one
        1234567890.1234567890123456789012345679

        (3) (integer digit count <= precision - scale) && (total digits of the value <= precision defined in schema)
        HiveDecimal.enforcePrecisionScale will return exactly the same decimal value as provided.
        For example, 123456.123456 can fit in DECIMAL(38,18) without any data loss.

        Current logic for ORC doesn't call HiveDecimal.enforcePrecisionScale but Parquet does. That will bring in the
        inconsistency in the error messages in the third check.
         */
        String limitationForAccurateValue = String.format("maximum precision %s", precision);
        if (enforcePrecisionAndScale) {
            // At this point data can fit in precision 38, but still need enforcePrecisionScale to check whether it can fit in scale 18
            hiveDecimal = HiveDecimal.enforcePrecisionScale(
                    hiveDecimal,
                    precision,
                    scale);

            if (hiveDecimal == null) {
                if (isDecimalOverflowOptionError || isDecimalOverflowOptionRound) {
                    throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s using %s profile exceeds maximum precision and scale (%d,%d).",
                            value, columnName, profile, precision, scale));
                }

                LOG.trace("The value {} for the NUMERIC column {} using {} profile exceeds maximum precision and scale ({},{}) and has been stored as NULL.",
                        value, columnName, profile, precision, scale);

                if (!isIntegerDigitCountOverflowWarningLogged) {
                    LOG.warn("There are rows where for the NUMERIC column {} using {} profile the values exceed maximum precision and scale ({},{}) " +
                                    "and have been stored as NULL. Enable TRACE log level for row-level details.",
                            profile, columnName, precision, scale);
                    isIntegerDigitCountOverflowWarningLogged = true;
                }
                return null;
            }

            limitationForAccurateValue = String.format("maximum scale %s", scale);
        }

        // At this point, the integer digit count must less than (precision - scale) for Parquet or less than the precision for ORC,
        // but the total digits may still greater than precision.
        // Here is to check whether there is a precision loss.
        BigDecimal accurateDecimal = new BigDecimal(value);
        if ((isDecimalOverflowOptionError || isDecimalOverflowOptionRound) && accurateDecimal.compareTo(hiveDecimal.bigDecimalValue()) != 0) {
            if (isDecimalOverflowOptionError) {
                throw new UnsupportedTypeException(String.format("The value %s for the NUMERIC column %s using %s profile exceeds %s, and cannot be stored without precision loss.",
                        value, columnName, profile, limitationForAccurateValue));
            }

            LOG.trace("The value {} for the NUMERIC column {} using {} profile exceeds {} and has been rounded off.",
                    value, columnName, profile, limitationForAccurateValue);

            if (!isScaleOverflowWarningLogged) {
                LOG.warn("There are rows where for the NUMERIC column {} using {} profile the values exceed {} " +
                                "and have been rounded off. Enable TRACE log level for row-level details.",
                        profile, columnName, limitationForAccurateValue);
                isScaleOverflowWarningLogged = true;
            }
        }
        return hiveDecimal.bigDecimalValue();
    }

    // TODO: implement parsing value with BigDecimal
    private BigDecimal parseDecimalStringWithBigDecimal() {
        return null;
    }
}
