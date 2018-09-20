package org.greenplum.pxf.automation.utils.system;

/***
 * Utility for working with system-wide parameters
 */
public abstract class SystemUtils {

    public static final String PGMODE_PROPERTY_NAME = "PG_MODE";
    public static PGModeEnum getPGMode() {

        PGModeEnum result;
        try {
            result = PGModeEnum.valueOf(System.getProperty(PGMODE_PROPERTY_NAME, PGModeEnum.GPDB.name()).toUpperCase());
        } catch (Exception e) {
            result = PGModeEnum.GPDB; // use GPDB as default mode
        }

        return result;
    }
}
