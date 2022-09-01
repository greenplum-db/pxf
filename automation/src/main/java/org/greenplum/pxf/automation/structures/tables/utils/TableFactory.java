package org.greenplum.pxf.automation.structures.tables.utils;

import java.util.ArrayList;
import java.util.List;

import org.greenplum.pxf.automation.enums.EnumPartitionType;

import org.greenplum.pxf.automation.enums.EnumPxfDefaultProfiles;
import org.greenplum.pxf.automation.structures.tables.hbase.HBaseTable;
import org.greenplum.pxf.automation.structures.tables.hive.HiveExternalTable;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ForeignTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;

/**
 * Factory class for preparing different kind of Tables setting.
 */
public abstract class TableFactory {

    /**
     * Prepares PXF Readable External Table for Hive data
     *
     * @param tableName
     * @param fields
     * @param hiveTable
     * @param useProfile true to use Profile or false to use Fragmenter Accessor
     *            Resolver
     * @return PXF Readable External Table using "Hive" profile
     */
    public static ReadableExternalTable getPxfHiveReadableTable(String tableName,
                                                                String[] fields,
                                                                HiveTable hiveTable,
                                                                boolean useProfile) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(tableName,
                fields, hiveTable.getName(), "CUSTOM");

        if (useProfile) {
            exTable.setProfile("hive");
        } else {
            exTable.setFragmenter("org.greenplum.pxf.plugins.hive.HiveDataFragmenter");
            exTable.setAccessor("org.greenplum.pxf.plugins.hive.HiveAccessor");
            exTable.setResolver("org.greenplum.pxf.plugins.hive.HiveResolver");
        }

        exTable.setFormatter("pxfwritable_import");

        return exTable;
    }

    /**
     * Prepares PXF Readable External Table for Hive RC data
     *
     * @param tableName external table name
     * @param fields for external table
     * @param hiveTable to direct to
     * @param useProfile true to use Profile or false to use Fragmenter Accessor
     *            Resolver
     * @return PXF Readable External Table using "HiveRC" profile
     */
    public static ReadableExternalTable getPxfHiveRcReadableTable(String tableName,
                                                                  String[] fields,
                                                                  HiveTable hiveTable,
                                                                  boolean useProfile) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(tableName,
                fields, hiveTable.getName(), "TEXT");

        if (useProfile) {
            exTable.setProfile(EnumPxfDefaultProfiles.HiveRC.toString());
        } else {
            exTable.setFragmenter("org.greenplum.pxf.plugins.hive.HiveInputFormatFragmenter");
            exTable.setAccessor("org.greenplum.pxf.plugins.hive.HiveRCFileAccessor");
            exTable.setResolver("org.greenplum.pxf.plugins.hive.HiveColumnarSerdeResolver");
        }
        exTable.setDelimiter("E'\\x01'");

        return exTable;
    }

    /**
     * Prepares PXF Readable External Table for Hive ORC data
     *
     * @param tableName external table name
     * @param fields for external table
     * @param hiveTable to direct to
     * @param useProfile true to use Profile or false to use Fragmenter Accessor
     *            Resolver
     * @return PXF Readable External Table using "HiveORC" profile
     */
    public static ReadableExternalTable getPxfHiveOrcReadableTable(String tableName,
                                                                  String[] fields,
                                                                  HiveTable hiveTable,
                                                                  boolean useProfile) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(tableName,
                fields, hiveTable.getName(), "CUSTOM");

        if (useProfile) {
            exTable.setProfile(EnumPxfDefaultProfiles.HiveORC.toString());
        } else {
            exTable.setFragmenter("org.greenplum.pxf.plugins.hive.HiveInputFormatFragmenter");
            exTable.setAccessor("org.greenplum.pxf.plugins.hive.HiveORCFileAccessor");
            exTable.setResolver("org.greenplum.pxf.plugins.hive.HiveORCSerdeResolver");
        }
        exTable.setFormatter("pxfwritable_import");

        return exTable;
    }

    /**
     * Prepares PXF Readable External Table for Hive ORC data using vectorized profile
     *
     * @param tableName external table name
     * @param fields for external table
     * @param hiveTable to direct to
     * @param useProfile true to use Profile or false to use Fragmenter Accessor
     *            Resolver
     * @return PXF Readable External Table using "HiveVectorizedORC" profile
     */
    public static ReadableExternalTable getPxfHiveVectorizedOrcReadableTable(String tableName,
                                                                   String[] fields,
                                                                   HiveTable hiveTable,
                                                                   boolean useProfile) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(tableName,
                fields, hiveTable.getName(), "CUSTOM");

        if (useProfile) {
            exTable.setProfile("HiveVectorizedORC");
        } else {
            exTable.setFragmenter("org.greenplum.pxf.plugins.hive.HiveInputFormatFragmenter");
            exTable.setAccessor("org.greenplum.pxf.plugins.hive.HiveORCVectorizedAccessor");
            exTable.setResolver("org.greenplum.pxf.plugins.hive.HiveORCVectorizedResolver");
        }
        exTable.setFormatter("pxfwritable_import");

        return exTable;
    }

    public static void main(String[] args) {
        HiveTable t = new HiveTable("bin_data", null);
        ReadableExternalTable a = getPxfHiveRcReadableTable("hive_bin",
                new String[] { "bin BYTEA" }, t, true);

        System.out.println(a.constructCreateStmt());
    }

    /**
     * Prepares PXF Readable External Table for Hive Text data
     *
     * @param tableName external table name
     * @param fields for external table
     * @param hiveTable to direct to
     * @param useProfile true to use Profile or false to use Fragmenter Accessor
     *            Resolver
     * @return PXF Readable External Table using "HiveText" profile
     */
    public static ReadableExternalTable getPxfHiveTextReadableTable(String tableName,
                                                                    String[] fields,
                                                                    HiveTable hiveTable,
                                                                    boolean useProfile) {
        ReadableExternalTable exTable = getReadableExternalOrForeignTable(tableName,
                fields, hiveTable.getName(), "TEXT");
        if (useProfile) {
            exTable.setProfile(EnumPxfDefaultProfiles.HiveText.toString());
        } else {
            exTable.setFragmenter("org.greenplum.pxf.plugins.hive.HiveInputFormatFragmenter");
            exTable.setAccessor("org.greenplum.pxf.plugins.hive.HiveLineBreakAccessor");
            exTable.setResolver("org.greenplum.pxf.plugins.hive.HiveStringPassResolver");
        }
        exTable.setDelimiter("E'\\x01'");

        return exTable;
    }

    /**
     * Prepares PXF Readable External Table for HBase data
     *
     * @param tableName
     * @param fields
     * @param hbaseTable for external table path
     * @return PXF Readable external table using HBase Profile
     */
    public static ReadableExternalTable getPxfHBaseReadableTable(String tableName,
                                                                 String[] fields,
                                                                 HBaseTable hbaseTable) {
        ReadableExternalTable exTable = getReadableExternalOrForeignTable(tableName,
                fields, hbaseTable.getName(), "CUSTOM");
        exTable.setProfile("hbase");
        exTable.setFormatter("pxfwritable_import");
        return exTable;
    }

    /**
     * Prepares PXF Readable External Table for Simple Text data, using
     * "HdfsTextSimple" profile
     *
     * @param name
     * @param fields
     * @param path
     * @param delimiter
     * @return PXF Readable External Table using "HdfsTextSimple" profile and
     *         "Text" format.
     */
    public static ReadableExternalTable getPxfReadableTextTable(String name,
                                                                String[] fields,
                                                                String path,
                                                                String delimiter) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name, fields, path, "Text");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(delimiter);
        return exTable;
    }

    /**
     * Prepares PXF Writable External Table for Simple Text data, using
     * "HdfsTextSimple" profile
     *
     * @param name
     * @param fields
     * @param path
     * @param delimiter
     * @return PXF Writable External Table using "HdfsTextSimple" profile and
     *         "Text" format.
     */
    public static WritableExternalTable getPxfWritableTextTable(String name,
                                                                String[] fields,
                                                                String path,
                                                                String delimiter) {
        WritableExternalTable exTable = new WritableExternalTable(name, fields,
                path, "Text");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(delimiter);
        return exTable;
    }

    /**
     * Prepares PXF Writable External Table for Simple Text data, using
     * "HdfsTextSimple" profile and Gzip Codec
     *
     * @param name
     * @param fields
     * @param path
     * @return PXF Writable External Table using "HdfsTextSimple" profile and
     *         "GZipCodec" compression.
     */
    public static WritableExternalTable getPxfWritableGzipTable(String name,
                                                                String[] fields,
                                                                String path,
                                                                String delimiter) {
        WritableExternalTable exTable = new WritableExternalTable(name, fields,
                path, "Text");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(delimiter);
        exTable.setCompressionCodec("org.apache.hadoop.io.compress.GzipCodec");
        return exTable;
    }

    /**
     * Prepares PXF Writable External Table for Simple Text data, using
     * "HdfsTextSimple" profile and BZip2 Codec
     *
     * @param name
     * @param fields
     * @param path
     * @return PXF Writable External Table using "HdfsTextSimple" profile and
     *         "BZip2" compression.
     */
    public static WritableExternalTable getPxfWritableBZip2Table(String name,
                                                                 String[] fields,
                                                                 String path,
                                                                 String delimiter) {
        WritableExternalTable exTable = new WritableExternalTable(name, fields,
                path, "Text");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(delimiter);
        exTable.setCompressionCodec("org.apache.hadoop.io.compress.BZip2Codec");
        return exTable;
    }

    /**
     * Prepares PXF Readable External Table using "SequenceWritable" profile,
     * CUSTOM format with "pxfwritable_export" formatter and data schema.
     *
     * @param name
     * @param fields
     * @param path
     * @param schema data schema
     * @return PXF Readable External Table using "SequenceWritable" profile,
     *         CUSTOM format with "pxfwritable_export" formatter and data
     *         schema.
     */
    public static ReadableExternalTable getPxfReadableSequenceTable(String name,
                                                                    String[] fields,
                                                                    String path,
                                                                    String schema) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name, fields,
                path, "CUSTOM");

        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":SequenceFile");
        exTable.setDataSchema(schema);
        exTable.setFormatter("pxfwritable_import");

        return exTable;
    }

    /**
     * Prepares PXF Writable External Table using "SequenceWritable" profile,
     * CUSTOM format with "pxfwritable_export" formatter and data schema.
     *
     * @param name
     * @param fields
     * @param path
     * @param schema data schema
     * @return PXF Writable External Table using "SequenceWritable" profile,
     *         CUSTOM format with "pxfwritable_export" formatter and data
     *         schema.
     */
    public static WritableExternalTable getPxfWritableSequenceTable(String name,
                                                                    String[] fields,
                                                                    String path,
                                                                    String schema) {

        WritableExternalTable exTable = new WritableExternalTable(name, fields,
                path, "CUSTOM");

        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":SequenceFile");
        exTable.setDataSchema(schema);
        exTable.setFormatter("pxfwritable_export");

        return exTable;
    }

    /**
     * Generates Hive Table in specified Hive schema using row format and comma delimiter
     *
     * @param name table name
     * @param schema Hive schema name
     * @param fields table fields
     * @return {@link HiveTable} with "row" format and comma Delimiter.
     */
    public static HiveTable getHiveByRowCommaTable(String name, String schema, String[] fields) {

    	HiveTable table;

    	if (schema != null)
    		table = new HiveTable(name, schema, fields);
    	else
    		table = new HiveTable(name, fields);

    	table.setFormat("ROW");
    	table.setDelimiterFieldsBy(",");

    	return table;
    }

    /**
     * Generates Hive Table using row format and comma delimiter
     *
     * @param name table name
     * @param fields table fields
     * @return {@link HiveTable} with "row" format and comma Delimiter.
     */
    public static HiveTable getHiveByRowCommaTable(String name, String[] fields) {

    	HiveTable table = getHiveByRowCommaTable(name, null, fields);
        return table;
    }


    /**
     * Generates Hive External Table using row format and comma delimiter
     *
     * @param name
     * @param fields
     * @return {@link HiveExternalTable} with "row" format and comma Delimiter.
     */
    public static HiveExternalTable getHiveByRowCommaExternalTable(String name,
                                                                   String[] fields) {

        HiveExternalTable table = new HiveExternalTable(name, fields);

        table.setFormat("ROW");
        table.setDelimiterFieldsBy(",");

        return table;
    }

    private static ExternalTable getPxfJdbcReadableTable(String tableName,
                                                         String[] fields, String dataSourcePath, String driver,
                                                         String dbUrl, boolean isPartitioned,
                                                         Integer partitionByColumnIndex, String rangeExpression,
                                                         String interval, String user, EnumPartitionType partitionType,
                                                         String server, String customParameters) {
        ExternalTable exTable = getReadableExternalOrForeignTable(tableName, fields,
                dataSourcePath, "CUSTOM");
        List<String> userParameters = new ArrayList<String>();
        if (driver != null) {
            userParameters.add("JDBC_DRIVER=" + driver);
        }
        if (dbUrl != null) {
            userParameters.add("DB_URL=" + dbUrl);
        }
        if (isPartitioned) {
            if (fields.length <= partitionByColumnIndex) {
                throw new IllegalArgumentException(
                        "Partition by column doesn't not exists.");
            }
            String partitionByColumn = fields[partitionByColumnIndex];
            String[] tokens = partitionByColumn.split("\\s+");
            userParameters.add("PARTITION_BY=" + tokens[0] + ":" + partitionType.name().toLowerCase());
            userParameters.add("RANGE=" + rangeExpression);
            userParameters.add("INTERVAL=" + interval);
        }

        if (user != null) {
            userParameters.add("USER=" + user);
        }
        if (server != null) {
            userParameters.add("SERVER=" + server);
        }
        if (customParameters != null) {
            userParameters.add(customParameters);
        }
        exTable.setUserParameters(userParameters.toArray(new String[userParameters.size()]));
        exTable.setProfile("jdbc");
        exTable.setFormatter("pxfwritable_import");

        return exTable;
    }

    /**
     * Generates an External Writable Table using JDBC profile.
     *
     * @param tableName name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the target table to be written to i.e. schema_name.table_name
     * @param driver full class name of the JDBC driver
     * @param dbUrl JDBC URL
     * @param user database user
     * @return External Writable Table
     */
    public static ExternalTable getPxfJdbcWritableTable(String tableName,
            String[] fields, String dataSourcePath, String driver,
            String dbUrl, String user, String customParameters) {

        ExternalTable exTable = new WritableExternalTable(tableName, fields, dataSourcePath, "CUSTOM");
        List<String> userParameters = new ArrayList<String>();
        if (driver != null) {
            userParameters.add("JDBC_DRIVER=" + driver);
        }
        if (dbUrl != null) {
            userParameters.add("DB_URL=" + dbUrl);
        }
        if (user != null) {
            userParameters.add("USER=" + user);
        }
        if (customParameters != null) {
            userParameters.add(customParameters);
        }
        exTable.setUserParameters(userParameters.toArray(new String[userParameters.size()]));
        exTable.setProfile("jdbc");
        exTable.setFormatter("pxfwritable_export");

        return exTable;
    }

    /**
     * Generates an External Readable Table using JDBC profile, partitioned by given column
     * on a given range with a given interval.
     * Recommended to use for large tables.
     *
     * @param tableName name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param driver full class name of the JDBC driver
     * @param dbUrl JDBC URL
     * @param partitionByColumnIndex index of column which table is partitioned/fragmented by
     * @param rangeExpression partition range expression
     * @param interval interval expression
     * @param user database user
     * @param partitionType partition type used to get fragments
     * @return External Readable Table
     */
    public static ExternalTable getPxfJdbcReadablePartitionedTable(
            String tableName,
            String[] fields, String dataSourcePath, String driver,
            String dbUrl, Integer partitionByColumnIndex,
            String rangeExpression, String interval, String user, EnumPartitionType partitionType, String server) {

        return getPxfJdbcReadableTable(tableName, fields, dataSourcePath, driver,
            dbUrl, true, partitionByColumnIndex, rangeExpression,
            interval, user, partitionType, server, null);
    }

    /**
     * Generates an External Readable Table using JDBC profile.
     * It's not recommended for large tables.
     *
     * @param tableName name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param driver full class name of the JDBC driver
     * @param dbUrl JDBC url
     * @param user databases user name
     * @return External Readable Table
     */
    public static ExternalTable getPxfJdbcReadableTable(String tableName,
            String[] fields, String dataSourcePath, String driver, String dbUrl, String user) {

        return getPxfJdbcReadableTable(tableName, fields, dataSourcePath, driver,
            dbUrl, false, null, null, null, user, null, null, null);
    }

    /**
     * Generates an External Readable Table using JDBC profile.
     * It's not recommended for large tables.
     *
     * @param tableName name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param driver full class name of the JDBC driver
     * @param dbUrl JDBC url
     * @param user databases user name
     * @param customParameters additional user parameters
     * @param
     * @return External Readable Table
     */
    public static ExternalTable getPxfJdbcReadableTable(String tableName,
            String[] fields, String dataSourcePath, String driver, String dbUrl, String user, String customParameters) {

        return getPxfJdbcReadableTable(tableName, fields, dataSourcePath, driver,
                dbUrl, false, null, null, null, user, null, null, customParameters);
    }

    /**
     * Generates an External Readable Table using JDBC profile.
     *
     * @param tableName name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param server name of configuration server
     * @return External Readable Table
     */
    public static ExternalTable getPxfJdbcReadableTable(String tableName, String[] fields, String dataSourcePath, String server) {
        return getPxfJdbcReadableTable(tableName, fields, dataSourcePath, null,
                null, false, null, null, null, null, null, server, null);
    }

    /**
     * Generates an External Readable Table using JDBC profile.
     *
     * @param tableName name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param dbUrl JDBC url
     * @param server name of configuration server
     * @return External Readable Table
     */
    public static ExternalTable getPxfJdbcReadableTable(String tableName, String[] fields, String dataSourcePath, String dbUrl, String server) {
        return getPxfJdbcReadableTable(tableName, fields, dataSourcePath, null,
                dbUrl, false, null, null, null, null, null, server, null);
    }

    /**
     * Generate an External Writable Table using JDBC profile.
     *
     * @param tableName name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param server name of configuration server
     * @return External Writable Table
     */
    public static ExternalTable getPxfJdbcWritableTable(String tableName, String[] fields, String dataSourcePath, String server) {
        String customParameter = server != null ? "SERVER=" + server : null;
        return getPxfJdbcWritableTable(tableName, fields, dataSourcePath, null, null, null, customParameter);
    }

    // ============ FDW Adapter ============
    private static ReadableExternalTable getReadableExternalOrForeignTable (String name, String[] fields, String path, String format) {
        return FDWUtils.useFDW ?
               new ForeignTable(name, fields, path, "Text") :
               new ReadableExternalTable(name, fields, path, "Text");
    }

}
