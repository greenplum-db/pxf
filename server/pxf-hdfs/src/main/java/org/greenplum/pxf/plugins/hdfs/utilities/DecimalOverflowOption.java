package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.error.UnsupportedTypeException;

/**
 * There are 3 different types of overflow that PXF needs to handle when the column is defined as NUMERIC
 * without precision and scale provided. Decimal overflows on NUMERIC(precision,scale) have already been handled by GPDB.
 *    Case 1:
 *        Integer digit count > precision
 *    Case 2:
 *        precision >= Integer digit count > precision - scale
 *    Case 3:
 *        (Integer digit count <= precision - scale) && (decimal part > scale)
 *
 * Previous decimal parsing logic enforced precision and scale only for the PXF Parquet profile and not for the PXF ORC profile.
 * PXF now enforces precision and scale for both profiles.
 *
 * To maintain backwards compatibility, if the decimal overflow option is set to 'ignore',
 * PXF uses `wasEnforcedPrecisionAndScale` to determine the decimal overflow behavior.
 * Previous decimal parsing logic for Parquet profile and ORC profile are:
 *    For the Parquet profile, PXF returns NULL for cases 1 and 2. PXF returns the rounded value for case 3.
 *    For the ORC profile, PXF returns NULL for case 1 and the rounded value for case 2 and 3.
 */
public enum DecimalOverflowOption {
    DECIMAL_OVERFLOW_ERROR("error", true),

    DECIMAL_OVERFLOW_ROUND("round", true),

    DECIMAL_OVERFLOW_IGNORE("ignore", true),

    DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING("ignore", false);

    private static final String PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ERROR = "error";
    private static final String PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ROUND = "round";
    private static final String PXF_WRITE_DECIMAL_OVERFLOW_OPTION_IGNORE = "ignore";
    private final String decimalOverflowOption;
    // this boolean is used when the overflow option is set to ignore and is meant to help with backwards compatibility
    private final boolean wasEnforcedPrecisionAndScale;

    /**
     * Construct an enum object containing a decimal overflow configuration value
     * and a profile-dependent boolean value. This boolean captures if the current profile was previously enforcing
     * precision and scale when parsing decimal values. It is used to help maintain backwards compatibility.
     * @param decimalOverflowValue        the decimal overflow configuration value for the profile. Supported values are 'error', 'round' and 'ignore'.
     * @param wasEnforcedPrecisionAndScale true if the previous decimal parsing behavior of the current profile enforced precision and scale
     */
    DecimalOverflowOption(String decimalOverflowValue, boolean wasEnforcedPrecisionAndScale) {
        this.decimalOverflowOption = decimalOverflowValue;
        this.wasEnforcedPrecisionAndScale = wasEnforcedPrecisionAndScale;
    }

    /**
     * Parse the configuration value of the configurationPropertyName defined in 'pxf-site.xml' into a DecimalOverflowOption enum object
     *
     * @param configuration               contains all the configuration properties of the current server
     * @param configurationPropertyName   name of decimal overflow configuration property to be parsed
     * @param wasEnforcedPrecisionAndScale true if the previous decimal parsing behavior of the current profile enforced precision and scale
     * @return a DecimalOverflowOption enum object
     */
    public static DecimalOverflowOption parseDecimalOverflowOption(Configuration configuration, String configurationPropertyName, boolean wasEnforcedPrecisionAndScale) {
        String decimalOverflowOption = configuration.get(configurationPropertyName, DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND.getDecimalOverflowOption()).toLowerCase();
        switch (decimalOverflowOption) {
            case PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ERROR:
                return DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR;
            case PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ROUND:
                return DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND;
            case PXF_WRITE_DECIMAL_OVERFLOW_OPTION_IGNORE:
                if (wasEnforcedPrecisionAndScale) {
                    return DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE;
                }
                return DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING;
            default:
                throw new UnsupportedTypeException(String.format("Invalid configuration value %s for configuration property %s. " +
                        "Valid values are 'error', 'round', and 'ignore'.", decimalOverflowOption, configurationPropertyName));
        }
    }

    /**
     * @return decimal overflow option of the current profile
     */
    public String getDecimalOverflowOption() {
        return decimalOverflowOption;
    }

    /**
     * @return true if the current profile enforced precision and scale in previous decimal parsing behavior
     */
    public boolean wasEnforcedPrecisionAndScale() {
        return wasEnforcedPrecisionAndScale;
    }

    /**
     * @return true if the current option is the 'error' option
     */
    public boolean isOptionError() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_ERROR.getDecimalOverflowOption());
    }

    /**
     * @return true if the current option is the 'round' option
     */
    public boolean isOptionRound() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_ROUND.getDecimalOverflowOption());
    }

    /**
     * @return true if the current option is the 'ignore' option
     */
    public boolean isOptionIgnore() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_IGNORE.getDecimalOverflowOption())
                || decimalOverflowOption.equals(DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING.getDecimalOverflowOption());
    }
}
