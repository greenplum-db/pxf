package org.greenplum.pxf.automation.utils.system;

/***
 * Utility for working with system-wide parameters
 */
public class ProtocolUtils {

    public final static String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    public final static String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";
    public static final String PROTOCOL_KEY = "PROTOCOL";
    public static final String PXF_TEST_KEEP_DATA = "PXF_TEST_KEEP_DATA";

    public static ProtocolEnum getProtocol() {

        ProtocolEnum result;
        try {
            result = ProtocolEnum.valueOf(System.getProperty(PROTOCOL_KEY, ProtocolEnum.HDFS.name()).toUpperCase());
        } catch (Exception e) {
            result = ProtocolEnum.HDFS; // use HDFS as default mode
        }

        return result;
    }

    public static String getSecret() {
        return System.getProperty(AWS_SECRET_ACCESS_KEY);
    }

    public static String getAccess() {
        return System.getProperty(AWS_ACCESS_KEY_ID);
    }

    public static String getPxfTestKeepData() {
        return System.getProperty(PXF_TEST_KEEP_DATA, "false");
    }


}
