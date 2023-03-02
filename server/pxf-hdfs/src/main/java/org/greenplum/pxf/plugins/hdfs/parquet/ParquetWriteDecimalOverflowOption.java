package org.greenplum.pxf.plugins.hdfs.parquet;

/**
 * Parquet server configuration options for property pxf.parquet.write.decimal.overflow
 */
public enum ParquetWriteDecimalOverflowOption {
    ERROR("error"),
    IGNORE("ignore");

    private String value = "";

    private ParquetWriteDecimalOverflowOption(String decimalOverflowOption) {
        this.value = decimalOverflowOption;
    }

    public String getValue() {
        return value;
    }
}

