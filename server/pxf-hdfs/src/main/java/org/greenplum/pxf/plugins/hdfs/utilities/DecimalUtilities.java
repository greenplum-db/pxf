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
    private boolean isDecimalOverflowOptionError;
    private boolean isDecimalOverflowOptionRound;
    private boolean isPrecisionOverflowWarningLogged;
    private boolean isIntegerDigitCountOverflowWarningLogged;
    private boolean isScaleOverflowWarningLogged;

    public DecimalUtilities() {
        isDecimalOverflowOptionError = false;
        isDecimalOverflowOptionRound = false;
        isPrecisionOverflowWarningLogged = false;
        isIntegerDigitCountOverflowWarningLogged = false;
        isScaleOverflowWarningLogged = false;
    }

    /**
     * Sets configuration variables based on server configuration properties of pxf.parquet.write.decimal.overflow.
     *
     * @param configuration                       contains server configuration properties
     * @param profile                             may have decimal overflow. Profiles can be ORC or Parquet
     * @param pxfWriteDecimalOverflowPropertyName is the property name in pxf-site.xml
     */
    public void parseDecimalOverflowOption(Configuration configuration, String profile, String pxfWriteDecimalOverflowPropertyName) {
        String decimalOverflowOption = configuration.get(pxfWriteDecimalOverflowPropertyName, PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ROUND).toLowerCase();
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
                        "pxf.%s.write.decimal.overflow. Valid values are error, round, and ignore.", decimalOverflowOption, profile));
        }
    }

    /**
     * Parse the incoming decimal string into a decimal number for ORC or Parquet profiles according to the decimal overflow options
     *
     * @param profile    may have decimal overflow. Profiles can be ORC or Parquet
     * @param value      incoming decimal string
     * @param precision  is the decimal precision defined in the schema
     * @param scale      is the decimal scale defined in the schema
     * @param columnName is the name of the current column
     * @return null or a BigDecimal number meets all the requirements
     */
    public BigDecimal parseDecimalString(String profile, String value, int precision, int scale, String columnName) {
        /*
         When this column is defined as NUMERIC(precision,scale) in Greenplum, Greenplum will handle writing values with overflow.
         When this column is defined as NUMERIC in Greenplum, the column type will be treated as DECIMAL(38,18) for Parquet, or DECIMAL(38,10) for ORC.

         For ORC, in ORC code base the max precision for decimal is set to 38 which is consistent with Hive,
         (https://github.com/apache/orc/blob/de5831ffbc72e872f61b0afd9683b77bd09c6c61/java/core/src/java/org/apache/orc/TypeDescription.java#L39)
         so we parse the value into ORC format with HiveDecimal.

         For Parquet, there is no decimal precision limitation, and the decimal precision and scale is pretty large in postgres,
         (https://www.postgresql.org/docs/9.3/datatype-numeric.html#DATATYPE-NUMERIC-TABLE)
         so we parse the value into Parquet with BigDecimal instead of HiveDecimal when precision is greater than Hive's max precision 38
         */

        if (StringUtils.equalsIgnoreCase(profile, "parquet")) {
            if (precision > HiveDecimal.MAX_PRECISION) {
                return parseDecimalStringWithBigDecimal();
            }
            return parseDecimalStringWithHiveDecimal(profile, value, precision, scale, true, columnName);
        }
        return parseDecimalStringWithHiveDecimal(profile, value, precision, scale, false, columnName);
    }

    /**
     * Parse the incoming decimal string into a decimal number for ORC or Parquet profiles according to the decimal overflow options
     *
     * @param profile                  may have decimal overflow. Profiles can be ORC, or Parquet with decimal precision not greater than Hive's max precision 38
     * @param value                    incoming decimal string
     * @param precision                is the decimal precision defined in the schema
     * @param scale                    is the decimal scale defined in the schema
     * @param enforcePrecisionAndScale decides whether to enforce the decimal with the given precision and scale
     * @param columnName               is the name of the current column
     * @return null or a BigDecimal number meets all the requirements
     */
    private BigDecimal parseDecimalStringWithHiveDecimal(String profile, String value, int precision, int scale, boolean enforcePrecisionAndScale, String columnName) {
        /*
         HiveDecimal.create has different behaviors for different types of overflow:

         (1) Precision overflow
         When the data integer digit count (data precision - data scale) is greater than 38,
         HiveDecimal.create will return a null value. To make the behavior consistent with Hive's behavior
         when storing on a Parquet-backed table, we store the value as null.
         For example, the integer digit count of 1234567890123456789012345678901234567890.123 is 40,
         which is greater than 38. HiveDecimal.create returns null.

         (2) Integer digit count overflow
         When data integer digit count is not greater than 38,
         and the overall data precision is greater than 38,
         HiveDecimal.create will return a rounded-off value to fit in the Hive maximum supported precision 38.
         For example, the integer digit count of 1234567890123456789012345.12345678901234567890 is 25 which is less than 38,
         but its overall precision is 45 which is greater than 38.
         Then data will be created as a rounded value 1234567890123456789012345.1234567890123

         (3) Scale overflow
         When data integer digit count is not greater than 38,
         and the overall data precision is not greater than 38,
         HiveDecimal.create will return the same decimal value as provided.
         For example, 123456.123456 can fit in DECIMAL(38,18) without any data loss, so the data will be created as the same
         */

        // HiveDecimal.create will return a decimal value which can fit in DECIMAL(38)
        // also there is Decimal and Decimal64 column vectors for ORC, see TypeUtils.createColumn
        HiveDecimal hiveDecimal = HiveDecimal.create(value);
        if (hiveDecimal == null) {
            if (isDecimalOverflowOptionError || isDecimalOverflowOptionRound) {
                throw new UnsupportedTypeException(String.format("The value %s for the %s NUMERIC column %s exceeds maximum precision %d.",
                        value, profile, columnName, precision));
            }

            LOG.trace("The value {} for the {} NUMERIC column {} exceeds maximum precision {} and has been stored as NULL.",
                    value, profile, columnName, precision);

            if (!isPrecisionOverflowWarningLogged) {
                LOG.warn("There are rows where for the {} NUMERIC column {} the values exceed maximum precision {} " +
                                "and have been stored as NULL. Enable TRACE log level for row-level details.",
                        profile, columnName, precision);
                isPrecisionOverflowWarningLogged = true;
            }
            return null;
        }

        // When enforcePrecisionAndScale = true, it will have a check on integer digit count (HiveDecimal.enforcePrecisionScale),
        // and values will be rounded with the integer digit count not greater than the max integer digit count
        // When enforcePrecisionAndScale = false, no check on integer digit count,
        // and values will be rounded with the integer digit count not greater than the max precision
        // so there will be different error hints in the scale check
        String limitationForAccurateValue = String.format("maximum precision %s", precision);
        if (enforcePrecisionAndScale) {
            // At this point data can fit in precision 38, but still need enforcePrecisionScale to check whether it can fit in scale 18
            hiveDecimal = HiveDecimal.enforcePrecisionScale(
                    hiveDecimal,
                    precision,
                    scale);

            /*
            When data integer digit count is greater than the maximum supported integer digit count 20 (38 - 18),
            enforcePrecisionScale will return null, it means we cannot store the value in Parquet because we have
            exceeded the maximum integer digit count. To make the behavior consistent with Hive's behavior
            when storing on a Parquet-backed table, we store the value as null.
            For example, in the case 2 in the previous HiveDecimal.create example,
            we got a rounded value 1234567890123456789012345.1234567890123.
            Its integer digit count is 25 which exceeds the maximum integer digit count 20.
            So it cannot fit in DECIMAL(38,18) and Null is returned.
             */
            if (hiveDecimal == null) {
                if (isDecimalOverflowOptionError || isDecimalOverflowOptionRound) {
                    throw new UnsupportedTypeException(String.format("The value %s for the %s NUMERIC column %s exceeds maximum precision and scale (%d,%d).",
                            value, profile, columnName, precision, scale));
                }

                LOG.trace("The value {} for the {} NUMERIC column {} exceeds maximum precision and scale ({},{}) and has been stored as NULL.",
                        value, profile, columnName, precision, scale);

                if (!isIntegerDigitCountOverflowWarningLogged) {
                    LOG.warn("There are rows where for the {} NUMERIC column {} the values exceed maximum precision and scale ({},{}) " +
                                    "and have been stored as NULL. Enable TRACE log level for row-level details.",
                            profile, columnName, precision, scale);
                    isIntegerDigitCountOverflowWarningLogged = true;
                }
                return null;
            }

            limitationForAccurateValue = String.format("maximum scale %s", scale);
        }

        BigDecimal accurateDecimal = new BigDecimal(value);
        // At this point data can fit in DECIMAL(38,18), but may have been rounded off
        if ((isDecimalOverflowOptionError || isDecimalOverflowOptionRound) && accurateDecimal.compareTo(hiveDecimal.bigDecimalValue()) != 0) {
            if (isDecimalOverflowOptionError) {
                throw new UnsupportedTypeException(String.format("The value %s for the %s NUMERIC column %s exceeds %s, and cannot be stored without precision loss.",
                        value, profile, columnName, limitationForAccurateValue));
            }

            LOG.trace("The value {} for the {} NUMERIC column {} exceeds {} and has been rounded off.",
                    value, profile, columnName, limitationForAccurateValue);

            if (!isScaleOverflowWarningLogged) {
                LOG.warn("There are rows where for the {} NUMERIC column {} the values exceed {} " +
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
