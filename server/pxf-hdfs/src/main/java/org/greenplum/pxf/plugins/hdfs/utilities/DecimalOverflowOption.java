package org.greenplum.pxf.plugins.hdfs.utilities;

/**
 * Supported values for PXF server configurations 'pxf.orc.write.decimal.overflow' and 'pxf.parquet.write.decimal.overflow'.
 * Default value is ROUND
 */
public enum DecimalOverflowOption {
    ERROR,
    ROUND,
    IGNORE
}
