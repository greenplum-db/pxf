package org.greenplum.pxf.plugins.hdfs.utilities;


/**
 * Supported decimal overflow options for ORC and Parquet profile.
 * Since decimal overflow handling for ORC profile and Parquet profile are slightly different,
 * the decimal overflow configuration name is also stored for differentiation.
 * An instance should be constructed in a profile-specific class.
 */
public enum DecimalOverflowOption {
    DECIMAL_OVERFLOW_ERROR( "error", true),

    DECIMAL_OVERFLOW_ROUND("round", true),

    DECIMAL_OVERFLOW_IGNORE("ignore", true),

    DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING("ignore", false);

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
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_ERROR.getDecimalOverflowOption());
    }

    /**
     *
     * @return whether the current option is 'round' option
     */
    public boolean isOptionRound() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_ROUND.getDecimalOverflowOption());
    }

    /**
     *
     * @return whether the current option is 'ignore' option
     */
    public boolean isOptionIgnore() {
        return decimalOverflowOption.equals(DECIMAL_OVERFLOW_IGNORE.getDecimalOverflowOption()) || decimalOverflowOption.equals(DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING);
    }

}
