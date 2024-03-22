package org.greenplum.pxf.automation.utils.system;

import org.apache.commons.lang.StringUtils;

/**
 * Enum to reflect the protocols supported
 */
public enum ProtocolEnum {
    ABFSS("abfss"),
    GS("gs"),
    HDFS("hdfs"),
    FILE("file") {
        @Override
        public String getExternalTablePath(String basePath, String path) {
            // Remove the basePath from the location URI for the table definition
            return StringUtils.removeStart(path, basePath);
        }
    },
    S3("s3"),
    WASBS("wasbs");

    private final String value;

    ProtocolEnum(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /**
     * Returns the path for the given protocol
     *
     * @param basePath the basePath for the HCFS profile
     * @param path the HCFS path
     * @return the path to be used for the table definition
     */
    public String getExternalTablePath(String basePath, String path) {
        return path;
    }
}
