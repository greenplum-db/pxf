package org.greenplum.pxf.plugins.hdfs.utilities;


/**
 * Supported decimal overflow options for ORC and Parquet profile.
 * Since decimal overflow handling for ORC profile and Parquet profile are slightly different,
 * the decimal overflow configuration name is also stored for differentiation.
 * An instance should be constructed in a profile-specific class.
 */
public enum DecimalOverflowOption {
    DECIMAL_OVERFLOW_ERROR_WITH_ENFORCING( "error", true),
    DECIMAL_OVERFLOW_ERROR_WITHOUT_ENFORCING( "error", false),
    DECIMAL_OVERFLOW_ROUND_WITH_ENFORCING("round", true),
    DECIMAL_OVERFLOW_ROUND_WITHOUT_ENFORCING("round", false),
    DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING("ignore", false),
    DECIMAL_OVERFLOW_IGNORE_WITH_ENFORCING("ignore", true);

    private final String decimalOverflowOption;

    private final boolean enforcePrecisionAndScale;

    DecimalOverflowOption(String decimalOverflowValue, boolean enforcePrecisionAndScale) {
        this.decimalOverflowOption = decimalOverflowValue;
        this.enforcePrecisionAndScale = enforcePrecisionAndScale;
    }

    /**
     * Return the decimal overflow option of the current profile.
     * @return decimal overflow option of the current profile
     */
    public String getDecimalOverflowOption() {
        return decimalOverflowOption;
    }

    /**
     *
     */
    public boolean isEnforcePrecisionAndScale() {
        return enforcePrecisionAndScale;
    }

    /**
     *
     * @return whether the current option is 'error' option
     */
    public boolean isOptionError() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_ERROR_WITH_ENFORCING.getDecimalOverflowOption()) || decimalOverflowOption.equals(DECIMAL_OVERFLOW_ERROR_WITHOUT_ENFORCING.getDecimalOverflowOption());
    }

    /**
     *
     * @return whether the current option is 'round' option
     */
    public boolean isOptionRound() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_ROUND_WITH_ENFORCING.getDecimalOverflowOption()) || decimalOverflowOption.equals(DECIMAL_OVERFLOW_ROUND_WITHOUT_ENFORCING.getDecimalOverflowOption());
    }

    /**
     *
     * @return whether the current option is 'ignore' option with overflowing value not set to NULL
     */
    public boolean isOptionIgnore() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_IGNORE_WITH_ENFORCING.getDecimalOverflowOption()) || decimalOverflowOption.equals(DECIMAL_OVERFLOW_ERROR_WITHOUT_ENFORCING.getDecimalOverflowOption());
    }

}
