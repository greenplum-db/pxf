package org.greenplum.pxf.plugins.hdfs.utilities;


import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.error.UnsupportedTypeException;

/**
 * Supported decimal overflow options for ORC and Parquet profile.
 * Since the old decimal parsing logic for PXF ORC profile didn't enforce precision and scale, whereas PXF Parquet profile did,
 * 'wasEnforcedPrecisionAndScale' is used to differentiated ORC profile and Parquet profile's old behavior,
 * and return the value getting from the old logic when decimal option is set as 'ignore'
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
    private final boolean wasEnforcedPrecisionAndScale;

    /**
     * Construct an enum object containing a decimal overflow configuration value
     * and info about whether the current profile was parsing a decimal value with precision and scale enforced in the old behavior
     *
     * @param decimalOverflowValue        is one of the supported the decimal overflow configuration value. Supported values are 'error', 'round' and 'ignore'.
     * @param wasEnforcePrecisionAndScale tells whether using this option should enforce precision and scale when parsing decimal strings
     */
    DecimalOverflowOption(String decimalOverflowValue, boolean wasEnforcePrecisionAndScale) {
        this.decimalOverflowOption = decimalOverflowValue;
        this.wasEnforcedPrecisionAndScale = wasEnforcePrecisionAndScale;
    }

    /**
     * Parse the configuration value of the configurationPropertyName defined in 'pxf-site.xml' into a DecimalOverflowOption enum object
     *
     * @param configuration               contains all the configuration properties of the current server
     * @param configurationPropertyName   is the decimal overflow configuration property going to be parsed
     * @param wasEnforcePrecisionAndScale tells whether the old decimal parsing behavior of the current profile enforced precision and scale
     * @return a DecimalOverflowOption enum object
     */
    public static DecimalOverflowOption parseDecimalOverflowOption(Configuration configuration, String configurationPropertyName, boolean wasEnforcePrecisionAndScale) {
        String decimalOverflowOption = configuration.get(configurationPropertyName, DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR.getDecimalOverflowOption()).toLowerCase();
        switch (decimalOverflowOption) {
            case PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ERROR:
                return DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR;
            case PXF_WRITE_DECIMAL_OVERFLOW_OPTION_ROUND:
                return DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND;
            case PXF_WRITE_DECIMAL_OVERFLOW_OPTION_IGNORE:
                if (wasEnforcePrecisionAndScale) {
                    return DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE;
                }
                return DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING;
            default:
                throw new UnsupportedTypeException(String.format("Invalid configuration value %s for configuration property %s. " +
                        "Valid values are 'error', 'round', and 'ignore'.", decimalOverflowOption, configurationPropertyName));
        }
    }

    /**
     * Return the decimal overflow option of the current profile.
     *
     * @return decimal overflow option of the current profile
     */
    public String getDecimalOverflowOption() {
        return decimalOverflowOption;
    }

    /**
     * Return the info whether the old behavior of the current profile enforced precision and scale when parsing decimal strings
     *
     * @return whether the old behavior of the current profile enforced precision and scale when parsing decimal strings
     */
    public boolean wasEnforcedPrecisionAndScale() {
        return wasEnforcedPrecisionAndScale;
    }

    /**
     * @return whether the current option is 'error' option
     */
    public boolean isOptionError() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_ERROR.getDecimalOverflowOption());
    }

    /**
     * @return whether the current option is 'round' option
     */
    public boolean isOptionRound() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_ROUND.getDecimalOverflowOption());
    }

    /**
     * @return whether the current option is 'ignore' option
     */
    public boolean isOptionIgnore() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_IGNORE.getDecimalOverflowOption())
                || decimalOverflowOption.equals(DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING.getDecimalOverflowOption());
    }
}
