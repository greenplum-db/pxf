package org.greenplum.pxf.plugins.hdfs.parquet;

/**
 * Parquet writer server configuration for switching between error out and ignore values when writing decimal values with overflow
 */
public enum ParquetWriteDecimalOverflowOption {
    ERROR("error"),
    IGNORE("ignore");

    private String decimalOverflowOption = "";

    private ParquetWriteDecimalOverflowOption(String decimalOverflowOption) {
        this.decimalOverflowOption = decimalOverflowOption;
    }

    public String getValue() {
        return decimalOverflowOption;
    }
}

