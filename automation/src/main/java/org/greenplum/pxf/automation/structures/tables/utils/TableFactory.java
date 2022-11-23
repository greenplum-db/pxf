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
     * Prepares PXF Readable External or Foreign Table for Hive data, using CUSTOM format and either
     * "hive" profile or Hive fragmenter, accessor and resolver.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param hiveTable Hive table object
     * @param useProfile true to use Profile or false to use Fragmenter, Accessor and Resolver
     * @return PXF Readable External or Foreign table
     */
    public static ReadableExternalTable getPxfHiveReadableTable(String name,
                                                                String[] fields,
                                                                HiveTable hiveTable,
                                                                boolean useProfile) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name,
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
     * Prepares PXF Readable External or Foreign Table for Hive RC data, using TEXT format and either
     * "HiveRC" profile or Hive fragmenter, accessor and resolver.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param hiveTable Hive table object
     * @param useProfile true to use Profile or false to use Fragmenter, Accessor and Resolver
     * @return PXF Readable External or Foreign table
     */
    public static ReadableExternalTable getPxfHiveRcReadableTable(String name,
                                                                  String[] fields,
                                                                  HiveTable hiveTable,
                                                                  boolean useProfile) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name,
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
     * Prepares PXF Readable External or Foreign Table for Hive ORC data, using CUSTOM format and either
     * "HiveORC" profile or Hive fragmenter, accessor and resolver.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param hiveTable Hive table object
     * @param useProfile true to use Profile or false to use Fragmenter, Accessor and Resolver
     * @return PXF Readable External or Foreign table
     */
    public static ReadableExternalTable getPxfHiveOrcReadableTable(String name,
                                                                   String[] fields,
                                                                   HiveTable hiveTable,
                                                                   boolean useProfile) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name,
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
     * Prepares PXF Readable External or Foreign Table for Hive data, using CUSTOM format and either
     * "HiveVectorizedORC" profile or Hive fragmenter, accessor and resolver.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param hiveTable Hive table object
     * @param useProfile true to use Profile or false to use Fragmenter, Accessor and Resolver
     * @return PXF Readable External or Foreign table
     */
    public static ReadableExternalTable getPxfHiveVectorizedOrcReadableTable(String name,
                                                                             String[] fields,
                                                                             HiveTable hiveTable,
                                                                             boolean useProfile) {

        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name,
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

    /**
     * Prepares PXF Readable External or Foreign Table for Hive data, using TEXT format and either "hive:text" profile
     * or Hive fragmenter, accessor and resolver.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param hiveTable Hive table object
     * @param useProfile true to use Profile or false to use Fragmenter, Accessor and Resolver
     * @return PXF Readable External or Foreign table
     */
    public static ReadableExternalTable getPxfHiveTextReadableTable(String name,
                                                                    String[] fields,
                                                                    HiveTable hiveTable,
                                                                    boolean useProfile) {
        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name,
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
     * Prepares PXF Readable External or Foreign Table for HBase data, using CUSTOM format and "hbase" profile.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param hbaseTable HBase table object
     * @return PXF Readable External or Foreign table
     */
    public static ReadableExternalTable getPxfHBaseReadableTable(String name,
                                                                 String[] fields,
                                                                 HBaseTable hbaseTable) {
        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name,
                fields, hbaseTable.getName(), "CUSTOM");
        exTable.setProfile("hbase");
        exTable.setFormatter("pxfwritable_import");
        return exTable;
    }

    /**
     * Prepares PXF Readable External or Foreign Table for test data, using TEXT format and "test:text" profile.
     * Since "test:*" profiles are ephemeral, it should be used when testing with custom Fragmenter, Accessor or Resolver.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param delimiter delimiter used in the external data
     * @return PXF Readable External or Foreign table
     */
    public static ReadableExternalTable getPxfReadableTestTextTable(String name,
                                                                    String[] fields,
                                                                    String path,
                                                                    String delimiter) {
        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name, fields, path, "Text");
        exTable.setProfile("test:text");
        exTable.setDelimiter(delimiter);
        return exTable;
    }

    /**
     * Prepares PXF Readable External or Foreign Table for TEXT data, using TEXT format and "<protocol>:text" profile.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param delimiter delimiter used in the external data
     * @return PXF Readable External or Foreign table
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
     * Prepares PXF Readable External or Foreign Table for CSV data, using CSV format and "<protocol>:csv" profile.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param delimiter delimiter used in the external data
     * @return PXF Readable External or Foreign table
     */
    public static ReadableExternalTable getPxfReadableCSVTable(String name,
                                                               String[] fields,
                                                               String path,
                                                               String delimiter) {
        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name, fields, path, "CSV");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":csv");
        exTable.setDelimiter(delimiter);
        return exTable;
    }

    /**
     * Prepares PXF Writable External or Foreign Table for TEXT data, using TEXT format and "<protocol>:text" profile.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param delimiter delimiter used in the external data
     * @return PXF Writable External or Foreign table
     */
    public static WritableExternalTable getPxfWritableTextTable(String name,
                                                                String[] fields,
                                                                String path,
                                                                String delimiter) {
        WritableExternalTable exTable = getWritableExternalOrForeignTable(name, fields, path, "Text");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(delimiter);
        return exTable;
    }

    /**
     * Prepares PXF Readable External or Foreign Table for HCFS-compatible file systems with a given file format
     * using CUSTOM table format and "<protocol>:<fileFormat>" profile.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param hcfsBasePath base path on HCFS file system
     * @param fileFormat format of the file being read
     * @return PXF Readable External or Foreign table
     */
    public static ReadableExternalTable getPxfHcfsReadableTable(String name,
                                                                String[] fields,
                                                                String path,
                                                                String hcfsBasePath,
                                                                String fileFormat) {
        String effectivePath = ProtocolUtils.getProtocol().getExternalTablePath(hcfsBasePath, path);
        ReadableExternalTable exTable = getReadableExternalOrForeignTable(name, fields, effectivePath, "custom");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":" + fileFormat);
        exTable.setFormatter("pxfwritable_import");
        return exTable;
    }

    /**
     * Prepares PXF Writable External or Foreign Table for HCFS-compatible file systems with a given file format
     * using CUSTOM table format and "<protocol>:<fileFormat>" profile.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param hcfsBasePath base path on HCFS file system
     * @param fileFormat format of the file being written
     * @return PXF Writable External or Foreign table
     */
    public static ReadableExternalTable getPxfHcfsWritableTable(String name,
                                                                String[] fields,
                                                                String path,
                                                                String hcfsBasePath,
                                                                String fileFormat) {
        String effectivePath = ProtocolUtils.getProtocol().getExternalTablePath(hcfsBasePath, path);
        ReadableExternalTable exTable = getWritableExternalOrForeignTable(name, fields, effectivePath, "custom");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":" + fileFormat);
        exTable.setFormatter("pxfwritable_export");
        return exTable;
    }

    /**
     * Prepares PXF Writable External or Foreign Table for TEXT data, using TEXT format and "<protocol>:text" profile
     * with Gzip compression codec.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param delimiter delimiter used in the external data
     * @return PXF Writable External or Foreign table
     */
    public static WritableExternalTable getPxfWritableGzipTable(String name,
                                                                String[] fields,
                                                                String path,
                                                                String delimiter) {
        WritableExternalTable exTable = getWritableExternalOrForeignTable(name, fields, path, "Text");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(delimiter);
        exTable.setCompressionCodec("org.apache.hadoop.io.compress.GzipCodec");
        return exTable;
    }

    /**
     * Prepares PXF Writable External or Foreign Table for TEXT data, using TEXT format and "<protocol>:text" profile
     * with BZip2 compression codec.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param delimiter delimiter used in the external data
     * @return PXF Writable External or Foreign table
     */
    public static WritableExternalTable getPxfWritableBZip2Table(String name,
                                                                 String[] fields,
                                                                 String path,
                                                                 String delimiter) {
        WritableExternalTable exTable = getWritableExternalOrForeignTable(name, fields, path, "Text");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(delimiter);
        exTable.setCompressionCodec("org.apache.hadoop.io.compress.BZip2Codec");
        return exTable;
    }

    /**
     * Prepares PXF Readable External or Foreign Table for SEQUENCE data, using CUSTOM format and "<protocol>:SequenceFile" profile.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param schema data schema
     * @return PXF Readable External or Foreign table
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
     * Prepares PXF Writable External or Foreign Table for SEQUENCE data, using CUSTOM format and "<protocol>:SequenceFile" profile.
     *
     * @param name name of the table
     * @param fields fields of the table
     * @param path for external table path
     * @param schema data schema
     * @return PXF Writable External or Foreign table
     */
    public static WritableExternalTable getPxfWritableSequenceTable(String name,
                                                                    String[] fields,
                                                                    String path,
                                                                    String schema) {

        WritableExternalTable exTable = getWritableExternalOrForeignTable(name, fields, path, "CUSTOM");

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
     * @param name table name
     * @param fields table fields
     * @return {@link HiveExternalTable} with "row" format and comma Delimiter.
     */
    public static HiveExternalTable getHiveByRowCommaExternalTable(String name,
                                                                   String[] fields) {

        HiveExternalTable table = new HiveExternalTable(name, fields);

        table.setFormat("ROW");
        table.setDelimiterFieldsBy(",");

        return table;
    }

    /**
     * Generates a PXF External Readable or Foreign Table using JDBC profile.
     *
     * @param name name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the target table to be written to i.e. schema_name.table_name
     * @param driver full class name of the JDBC driver
     * @param dbUrl JDBC URL
     * @param user database user
     * @return PXF Writable External or Foreign table
     */
    private static ExternalTable getPxfJdbcReadableTable(String name,
                                                         String[] fields, String dataSourcePath, String driver,
                                                         String dbUrl, boolean isPartitioned,
                                                         Integer partitionByColumnIndex, String rangeExpression,
                                                         String interval, String user, EnumPartitionType partitionType,
                                                         String server, String customParameters) {
        ExternalTable exTable = getReadableExternalOrForeignTable(name, fields,
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
     * Generates a PXF External Writable or Foreign Table using JDBC profile.
     *
     * @param name name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the target table to be written to i.e. schema_name.table_name
     * @param driver full class name of the JDBC driver
     * @param dbUrl JDBC URL
     * @param user database user
     * @return PXF Writable External or Foreign table
     */
    public static ExternalTable getPxfJdbcWritableTable(String name,
            String[] fields, String dataSourcePath, String driver,
            String dbUrl, String user, String customParameters) {

        ExternalTable exTable = getWritableExternalOrForeignTable(name, fields, dataSourcePath, "CUSTOM");
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
     * Generates a PXF External Readable or Foreign Table using JDBC profile, partitioned by given column
     * on a given range with a given interval.
     * Recommended to use for large tables.
     *
     * @param name name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param driver full class name of the JDBC driver
     * @param dbUrl JDBC URL
     * @param partitionByColumnIndex index of column which table is partitioned/fragmented by
     * @param rangeExpression partition range expression
     * @param interval interval expression
     * @param user database user
     * @param partitionType partition type used to get fragments
     * @return PXF Readable External or Foreign table
     */
    public static ExternalTable getPxfJdbcReadablePartitionedTable(
            String name,
            String[] fields, String dataSourcePath, String driver,
            String dbUrl, Integer partitionByColumnIndex,
            String rangeExpression, String interval, String user, EnumPartitionType partitionType, String server) {

        return getPxfJdbcReadableTable(name, fields, dataSourcePath, driver,
            dbUrl, true, partitionByColumnIndex, rangeExpression,
            interval, user, partitionType, server, null);
    }

    /**
     * Generates a PXF External Readable or Foreign Table using JDBC profile.
     * It's not recommended for large tables.
     *
     * @param name name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param driver full class name of the JDBC driver
     * @param dbUrl JDBC url
     * @param user databases user name
     * @return PXF Readable External or Foreign table
     */
    public static ExternalTable getPxfJdbcReadableTable(String name,
            String[] fields, String dataSourcePath, String driver, String dbUrl, String user) {

        return getPxfJdbcReadableTable(name, fields, dataSourcePath, driver,
            dbUrl, false, null, null, null, user, null, null, null);
    }

    /**
     * Generates a PXF External Readable or Foreign Table using JDBC profile.
     * It's not recommended for large tables.
     *
     * @param name name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param driver full class name of the JDBC driver
     * @param dbUrl JDBC url
     * @param user databases user name
     * @param customParameters additional user parameters
     * @return PXF Readable External or Foreign table
     */
    public static ExternalTable getPxfJdbcReadableTable(String name,
            String[] fields, String dataSourcePath, String driver, String dbUrl, String user, String customParameters) {

        return getPxfJdbcReadableTable(name, fields, dataSourcePath, driver,
                dbUrl, false, null, null, null, user, null, null, customParameters);
    }

    /**
     * Generates a PXF External Readable or Foreign Table using JDBC profile.
     *
     * @param name name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param server name of configuration server
     * @return PXF Readable External or Foreign table
     */
    public static ExternalTable getPxfJdbcReadableTable(String name, String[] fields, String dataSourcePath, String server) {
        return getPxfJdbcReadableTable(name, fields, dataSourcePath, null,
                null, false, null, null, null, null, null, server, null);
    }

    /**
     * Generates a PXF External Readable or Foreign Table using JDBC profile.
     *
     * @param name name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param dbUrl JDBC url
     * @param server name of configuration server
     * @return PXF Readable External or Foreign table
     */
    public static ExternalTable getPxfJdbcReadableTable(String name, String[] fields, String dataSourcePath, String dbUrl, String server) {
        return getPxfJdbcReadableTable(name, fields, dataSourcePath, null,
                dbUrl, false, null, null, null, null, null, server, null);
    }

    /**
     * Generate a PXF External Writable or Foreign Table using JDBC profile.
     *
     * @param name name of the external table which will be generated
     * @param fields fields of the external table
     * @param dataSourcePath path to the data object i.e. schema_name.table_name
     * @param server name of configuration server
     * @return PXF Writable External or Foreign table
     */
    public static ExternalTable getPxfJdbcWritableTable(String name, String[] fields, String dataSourcePath, String server) {
        String customParameter = server != null ? "SERVER=" + server : null;
        return getPxfJdbcWritableTable(name, fields, dataSourcePath, null, null, null, customParameter);
    }

    // ============ FDW Adapter ============
    private static ReadableExternalTable getReadableExternalOrForeignTable (String name, String[] fields, String path, String format) {
        return FDWUtils.useFDW ?
                new ForeignTable(name, fields, path, format) :
                new ReadableExternalTable(name, fields, path, format);
    }

    private static WritableExternalTable getWritableExternalOrForeignTable (String name, String[] fields, String path, String format) {
        return FDWUtils.useFDW ?
                new ForeignTable(name, fields, path, format) :
                new WritableExternalTable(name, fields, path, format);
    }

}
