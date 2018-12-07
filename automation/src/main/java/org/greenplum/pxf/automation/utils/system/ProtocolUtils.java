package org.greenplum.pxf.automation.utils.system;

/***
 * Utility for working with system-wide parameters
 */
public class ProtocolUtils {

    public static final String PROTOCOL_KEY = "PROTOCOL";
    public static ProtocolEnum getProtocol() {

        ProtocolEnum result;
        try {
            result = ProtocolEnum.valueOf(System.getProperty(PROTOCOL_KEY, ProtocolEnum.HDFS.name()).toUpperCase());
        } catch (Exception e) {
            result = ProtocolEnum.HDFS; // use HDFS as default mode
        }

        return result;
    }
}
