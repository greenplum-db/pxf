package org.greenplum.pxf.automation.features.hive;

import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveExternalTable;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.features.BaseFeature;
import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HiveBaseTest extends BaseFeature {

    static final String[] PXF_HIVE_TYPES_COLS = {
            "t1    TEXT",
            "t2    TEXT",
            "num1  INTEGER",
            "dub1  DOUBLE PRECISION",
            "dec1  NUMERIC",
            "tm    TIMESTAMP",
            "r     REAL",
            "bg    BIGINT",
            "b     BOOLEAN",
            "tn    SMALLINT",
            "sml   SMALLINT",
            "dt    DATE",
            "vc1   VARCHAR(5)",
            "c1    CHAR(3)",
            "bin   BYTEA"
    };
    static final String[] PXF_HIVE_TYPES_LIMITED_COLS = {
            "key   TEXT",
            "t1    TEXT",
            "num1  INTEGER",
            "dub1  FLOAT8",
            "tm    TIMESTAMP",
            "r     REAL",
            "bg    BIGINT",
            "b     BOOLEAN",
            "sml   SMALLINT",
            "bin   BYTEA"
    };
    static final String[] HIVE_TYPES_COLS = {
            "t1    STRING",
            "t2    STRING",
            "num1  INT",
            "dub1  DOUBLE",
            "dec1  DECIMAL(38,18)",
            "tm    TIMESTAMP",
            "r     FLOAT",
            "bg    BIGINT",
            "b     BOOLEAN",
            "tn    TINYINT",
            "sml   SMALLINT",
            "dt    DATE",
            "vc1   VARCHAR(5)",
            "c1    CHAR(3)",
            "bin   BINARY"
    };
    static final String[] HIVE_TYPES_LIMITED_COLS = {
            "key   STRING",
            "t1    STRING",
            "num1  INT",
            "dub1  DOUBLE",
            "tm    TIMESTAMP",
            "r     FLOAT",
            "bg    BIGINT",
            "b     BOOLEAN",
            "sml   SMALLINT",
            "bin   BINARY"
    };
    static final String[] HIVE_SMALLDATA_COLS = {
            "t1    STRING",
            "t2    STRING",
            "num1  INT",
            "dub1  DOUBLE"
    };
    static final String[] HIVE_RC_COLS = { // TODO: dedup with HIVE_SMALLDATA_COLS above
            "t1    STRING",
            "t2    STRING",
            "num1  INT",
            "dub1  DOUBLE"
    };
    // Hive Table columns for Predicate pushdown test on partitioned hive table
    static final String[] HIVE_SMALLDATA_PPD_COLS = {
            "t1    STRING",
            "dub1  DOUBLE",
    };
    static final String[] PXF_HIVE_SMALLDATA_COLS = {
            "t1    TEXT",
            "t2    TEXT",
            "num1  INTEGER",
            "dub1  DOUBLE PRECISION"
    };
    static final String[] PXF_HIVE_SMALLDATA_AS_TEXT_COLS = {
            "t1    TEXT",
            "t2    TEXT",
            "num1  TEXT",
            "dub1  TEXT"
    };
    static final String[] PXF_HIVE_SUBSET_COLS = {
            "dub1  DOUBLE PRECISION",
            "t2    TEXT"
    };
    static final String[] PXF_HIVE_SMALLDATA_FMT_COLS = {
            "t1    TEXT",
            "t2    TEXT",
            "num1  INTEGER",
            "dub1  DOUBLE PRECISION",
            "fmt   TEXT"
    };
    static final String[] PXF_HIVE_SUBSET_FMT_COLS = {
            "fmt   TEXT",
            "num1  INTEGER",
            "t2    TEXT"
    };
    static final String[] PXF_HIVE_SMALLDATA_PRT_COLS = {
            "t1    TEXT",
            "t2    TEXT",
            "num1  INTEGER",
            "dub1  DOUBLE PRECISION",
            "fmt   TEXT",
            "prt   TEXT"
    };
    // PXF Table columns for Predicate pushdown test on partitioned hive table
    static final String[] PXF_HIVE_SMALLDATA_PPD_COLS = {
            "t1    TEXT",
            "dub1  DOUBLE PRECISION",
            "t2    TEXT",
            "num1  INTEGER"
    };
    static final String[] PXF_HIVE_COLLECTION_COLS = {
            "t1    TEXT",
            "f1    REAL",
            "t2    TEXT",
            "t3    TEXT",
            "t4    TEXT",
            "t5    TEXT"
    };
    static final String[] HIVE_COLLECTION_COLS = {
            "t1    STRING",
            "f1    FLOAT",
            "t2    ARRAY<STRING>",
            "t3    MAP<STRING, FLOAT>",
            "t4    STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>",
            "t5    UNIONTYPE<STRING, INT, ARRAY<INT>, ARRAY<STRING>>"
    };
    static final String[] HIVE_TYPES_BINARY = {
            "b1    BINARY"
    };
    static final String[] HIVE_PARTITION_COLUMN = {
            "fmt   STRING"
    };
    static final String[] HIVE_PARTITION_COLS = {
            "fmt   STRING",
            "prt   STRING"
    };
    // Hive table partition columns for Predicate pushdown test
    static final String[] HIVE_PARTITION_PPD_COLS = {
            "t2    STRING",
            "num1  INT"
    };

    static final String[] PARQUET_TIMESTAMP_COLS ={
            "tm    TIMESTAMP"
    };

    // PXF Table columns for Parquet Mismatch test on partitioned hive table
    // column order in the table definition is different from the references Hive table
    static final String[] PXF_HIVE_PARQUET_MISMATCH_COLS = {
            "num   INTEGER",        // 10
            "s2    TEXT",           // s_10
            "dcm   NUMERIC(38,12)", // 1.0
            "part  TEXT",           // a
            "s1    TEXT"            // a_row0
    };

    static final String[] HIVE_PARQUET_MISMATCH_TABLE_COLS = {
            "s1           STRING",         // a_row0
            "dcm          DECIMAL(38,12)", // 1.0
            "num          INT",            // 10
            "s2           STRING",         // s_10
            "ext_hive_par STRING"          // ext_hive_par_10
    };

    static final String[] HIVE_PARQUET_MISMATCH_SOURCE_TABLE_COLS = {
            "part         STRING",         // a
            "s1           STRING",         // a_row0
            "s2           STRING",         // s_10
            "num          INT",            // 10
            "dcm          DECIMAL(38,12)", // 1.0
            "ext_par      STRING",         // ext_par_10
            "ext_hive_par STRING"          // ext_hive_par_10
    };

    // - partition A: has 3 GP columns, 'extra_hive_par', --> missing a column present in Hive and GP tables
    static final String[] HIVE_PARQUET_MISMATCH_WRITE_PART_A_COL_NAMES = {"dcm", "num", "s1", "ext_hive_par", "part"};
    static final String[] HIVE_PARQUET_MISMATCH_WRITE_PART_A_COLS = {
            "dcm          DECIMAL(38,12)", // 1.0
            "num          INT",            // 10
            "s1           STRING",         // a_row0
            "ext_hive_par STRING"          // ext_hive_par_10
    };

    // - partition B: has 4 GP columns, 'extra_hive_par'
    static final String[] HIVE_PARQUET_MISMATCH_WRITE_PART_B_COL_NAMES = {"s2", "s1", "dcm", "ext_hive_par","num", "part"};
    static final String[] HIVE_PARQUET_MISMATCH_WRITE_PART_B_COLS = {
            "s2           STRING",         // s_10
            "s1           STRING",         // a_row0
            "dcm          DECIMAL(38,12)", // 1.0
            "ext_hive_par STRING",         // ext_hive_par_10
            "num          INT"             // 10
    };

    // - partition C: has 4 GP columns, 'extra_hive_par', --> and 'extra_par' column (not present in Hive or GP)
    static final String[] HIVE_PARQUET_MISMATCH_WRITE_PART_C_COL_NAMES = {"ext_par", "num", "s1", "s2", "dcm", "ext_hive_par", "part"};
    static final String[] HIVE_PARQUET_MISMATCH_WRITE_PART_C_COLS = {
            "ext_par      STRING",         // ext_par_10
            "num          INT",            // 10
            "s1           STRING",         // a_row0
            "s2           STRING",         // s_10
            "dcm          DECIMAL(38,12)", // 1.0
            "ext_hive_par STRING"          // ext_hive_par_10
    };
    // PXF Table columns for testing string escaping of complex types with nulls
    static final String[] PXF_HIVE_NESTED_STRUCT_COLS = {
            "t1    TEXT"
    };
    static final String[] HIVE_NESTED_STRUCT_COLS = {
            "t1    STRUCT<field1:STRUCT<subfield1:STRING, subfield2:STRING>, field2:INT>"
    };

    static final String HIVE_TYPES_DATA_FILE_NAME = "hive_types.txt";
    static final String HIVE_COLLECTIONS_FILE_NAME = "hive_collections.txt";
    static final String HIVE_DATA_FILE_NAME = "hive_small_data.txt";
    static final String HIVE_TYPES_LIMITED_FILE_NAME = "hive_types_limited.txt";
    static final String HIVE_PARQUET_MISMATCH_DATA_FILE_NAME = "hive_parquet_mismatch_data.txt";

    static final String HIVE_SMALL_DATA_TABLE = "hive_small_data";
    static final String HIVE_TYPES_TABLE = "hive_types";
    static final String HIVE_TEXT_TABLE = "hive_text_table";
    static final String HIVE_RC_TABLE = "hive_rc_table";
    static final String HIVE_RC_FOR_ALTER_TABLE = "hive_rc_alter_table";
    static final String HIVE_ORC_TABLE = "hive_orc_table";
    static final String HIVE_ORC_SNAPPY_TABLE = "hive_orc_snappy";
    static final String HIVE_ORC_ZLIB_TABLE = "hive_orc_zlib";
    static final String HIVE_PARQUET_TIMESTAMP_TABLE = "hive_parquet_timestamp";
    static final String HIVE_BINARY_TABLE = "hive_binary";
    static final String HIVE_COLLECTIONS_TABLE = "hive_collections_table";
    static final String HIVE_AVRO_TABLE = "hive_avro_table";
    static final String HIVE_PARQUET_TABLE = "hive_parquet_table";
    static final String HIVE_PARQUET_FOR_ALTER_TABLE = "hive_parquet_alter_table";
    static final String HIVE_PARQUET_MISMATCH_SOURCE_TABLE = "hive_parquet_mismatch_source_table";
    static final String HIVE_PARQUET_MISMATCH_TABLE = "hive_parquet_mismatch_table";
    static final String HIVE_SEQUENCE_TABLE = "hive_sequence_table";
    static final String HIVE_OPEN_CSV_TABLE = "hive_open_csv_table";
    static final String HIVE_PARTITIONED_TABLE = "hive_partitioned_table";
    static final String HIVE_PARTITIONED_PPD_TABLE = "hive_partitioned_ppd_table";
    static final String HIVE_REG_HETEROGEN_TABLE = "reg_heterogen";
    static final String PXF_HIVE_SMALL_DATA_TABLE = "pxf_hive_small_data";
    static final String PXF_HIVE_COLLECTIONS_TABLE = "pxf_hive_collections";
    static final String PXF_HIVE_BINARY_TABLE = "pxf_hive_binary";
    static final String PXF_HIVE_PARTITIONED_TABLE = "pxf_hive_partitioned_table";
    static final String PXF_HIVE_PARTITIONED_PPD_TABLE = "pxf_hive_partitioned_ppd_table";
    static final String PXF_HIVE_TEXT_TABLE = "pxf_hive_text";
    static final String PXF_HIVE_ORC_TABLE = "pxf_hive_orc_types";
    static final String PXF_HIVE_ORC_ZLIB_TABLE = "pxf_hive_orc_zlib";
    static final String PXF_HIVE_PARQUET_TIMESTAMP_TABLE = "pxf_hive_parquet_timestamp";
    static final String PXF_HIVE_HETEROGEN_TABLE = "pxf_hive_heterogen";
    static final String GPDB_SMALL_DATA_TABLE = "gpdb_small_data";
    static final String GPDB_HIVE_TYPES_TABLE = "gpdb_hive_types";
    static final String HIVE_NESTED_STRUCT_TABLE = "hive_nested_struct";
    static final String PXF_HIVE_NESTED_STRUCT_TABLE = "pxf_hive_nested_struct";

    static final String HIVE_SCHEMA = "userdb";
    static final String AVRO = "AVRO";
    static final String OPEN_CSV_INPUT_OUTPUT_FORMAT = "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'";
    static final String ORC = "ORC";
    static final String PARQUET = "PARQUET";
    static final String RCFILE = "RCFILE";
    static final String SEQUENCEFILE = "SEQUENCEFILE";
    static final String TEXTFILE = "TEXTFILE";
    static final String FORMAT_ROW = "ROW";
    static final String TEST_PACKAGE = "org.greenplum.pxf.automation.testplugin.";
    static final String TEST_PACKAGE_LOCATION = "/org/greenplum/pxf/automation/testplugin/";
    static final String ORC_COMPRESSION = "orc.compression";
    static final String COLUMNAR_SERDE = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";

    Hive hive;
    HiveExternalTable hiveTable;
    HiveTable hiveSmallDataTable;
    HiveTable hiveTypesTable;
    HiveTable hiveOrcTable;
    HiveTable hiveOrcAllTypes;
    HiveTable hiveOrcSnappyTable;
    HiveTable hiveOrcZlibTable;
    HiveTable hiveRcTable;
    HiveTable hiveRcForAlterTable;
    HiveTable hiveSequenceTable;
    HiveTable hiveParquetTable;
    HiveTable hiveParquetForAlterTable;
    HiveTable hiveParquetMismatchSourceTable; // intermediate table holding all data for parquet file creation process
    HiveTable hiveParquetMismatchTable;       // partitioned table on top of parquet files with mismatched columns
    HiveTable hiveAvroTable;
    HiveTable hiveBinaryTable;
    HiveTable hiveCollectionTable;
    HiveTable hiveNonDefaultSchemaTable;
    HiveTable hiveOpenCsvTable;
    Table comparisonDataTable;
    HiveTable hiveNestedStructTable;

    String configuredNameNodeAddress;
    String hdfsBaseDir;

    protected boolean isRestartAllowed() {
        return true;
    }

    @Override
    protected void beforeClass() throws Exception {

        if (isRestartAllowed()) {
            // copy additional plugins classes to cluster nodes, used for filter pushdown cases
            String oldPath = "target/classes" + TEST_PACKAGE_LOCATION;
            String newPath = "/tmp/publicstage/pxf";
            cluster.copyFileToNodes(new File(oldPath + "HiveDataFragmenterWithFilter.class")
                    .getAbsolutePath(), newPath + TEST_PACKAGE_LOCATION, true, false);
            cluster.copyFileToNodes(new File(oldPath + "MultipleHiveFragmentsPerFileFragmenter.class")
                    .getAbsolutePath(), newPath + TEST_PACKAGE_LOCATION, true, false);
            cluster.copyFileToNodes(new File(oldPath + "HiveInputFormatFragmenterWithFilter.class")
                    .getAbsolutePath(), newPath + TEST_PACKAGE_LOCATION, true, false);

            // add new path to classpath file and restart PXF service
            cluster.addPathToPxfClassPath(newPath);
            cluster.restart(PhdCluster.EnumClusterServices.pxf);
        }

        hdfsBaseDir = cluster.getHiveBaseHdfsDirectory();

        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");

        // get configured Name Node
        configuredNameNodeAddress = hdfs.getConfiguredNameNodeAddress();

        prepareData();
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();

        // close hive connection
        if (hive != null)
            hive.close();
    }

    void loadDataIntoHive(String fileName, HiveTable tableName) throws Exception {
        loadDataIntoHive(hdfs, hive, fileName, tableName);
    }

    void loadDataIntoHive(Hdfs hdfs, Hive hive, String fileName, HiveTable tableName) throws Exception {

        // copy data to hdfs
        hdfs.copyFromLocal(localDataResourcesFolder + "/hive/" + fileName,
                hdfs.getWorkingDirectory() + "/" + fileName);
        // load to hive table
        hive.loadData(tableName, hdfs.getWorkingDirectory() + "/" + fileName, false);
    }

    String[] hiveTestFilter(String filterString) {
        return new String[]{"TEST-HIVE-FILTER=" + filterString};
    }

    private HiveTable prepareData(String tableName, String format) throws Exception {

        HiveTable hiveTable = new HiveTable(tableName, HIVE_RC_COLS);
        hiveTable.setStoredAs(format);
        hive.createTableAndVerify(hiveTable);
        hive.insertData(hiveSmallDataTable, hiveTable);
        return hiveTable;
    }

    void prepareData() throws Exception {

        prepareSmallData();
        prepareTypesData();
    }

    void prepareSmallData() throws Exception {

        hiveSmallDataTable = prepareTableData(hdfs, hive, hiveSmallDataTable, HIVE_SMALL_DATA_TABLE, HIVE_SMALLDATA_COLS, HIVE_DATA_FILE_NAME);
    }

    HiveTable prepareTableData(Hdfs hdfs, Hive hive, HiveTable hiveTable, String tableName, String[] tableColumns, String dataFileName) throws Exception {

        if (hiveTable != null)
            return hiveTable;
        hiveTable = TableFactory.getHiveByRowCommaTable(tableName, tableColumns);
        hive.createTableAndVerify(hiveTable);
        loadDataIntoHive(hdfs, hive, dataFileName, hiveTable);
        return hiveTable;
    }

    void prepareTypesData() throws Exception {

        if (hiveTypesTable != null)
            return;
        hiveTypesTable = TableFactory.getHiveByRowCommaTable(HIVE_TYPES_TABLE, HIVE_TYPES_COLS);
        hive.createTableAndVerify(hiveTypesTable);
        loadDataIntoHive(HIVE_TYPES_DATA_FILE_NAME, hiveTypesTable);
    }

    void prepareOrcData() throws Exception {

        if (hiveOrcTable != null)
            return;
        hiveOrcTable = new HiveTable(HIVE_ORC_TABLE, HIVE_RC_COLS);
        hiveOrcTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcTable);
        hive.insertData(hiveSmallDataTable, hiveOrcTable);

        hiveOrcAllTypes = new HiveTable("hive_orc_all_types", HIVE_TYPES_COLS);
        hiveOrcAllTypes.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcAllTypes);
        hive.insertData(hiveTypesTable, hiveOrcAllTypes);
    }

    void prepareOrcSnappyData() throws Exception {

        if (hiveOrcSnappyTable != null)
            return;
        hiveOrcSnappyTable = new HiveTable(HIVE_ORC_SNAPPY_TABLE, HIVE_RC_COLS);
        hiveOrcSnappyTable.setStoredAs(ORC);
        List<List<String>> tableProperties = new ArrayList<>();
        tableProperties.add(new ArrayList<>(Arrays.asList(ORC_COMPRESSION, "snappy")));
        hiveOrcSnappyTable.setTableProperties(tableProperties);
        hive.createTableAndVerify(hiveOrcSnappyTable);
        hive.insertData(hiveSmallDataTable, hiveOrcSnappyTable);
    }

    void prepareOrcZlibData() throws Exception {

        if (hiveOrcZlibTable != null)
            return;
        hiveOrcZlibTable = new HiveTable(HIVE_ORC_ZLIB_TABLE, HIVE_RC_COLS);
        hiveOrcZlibTable.setStoredAs(ORC);
        List<List<String>> tableProperties = new ArrayList<>();
        tableProperties.add(new ArrayList<>(Arrays.asList(ORC_COMPRESSION, "zlib")));
        hiveOrcZlibTable.setTableProperties(tableProperties);
        hive.createTableAndVerify(hiveOrcZlibTable);
        hive.insertData(hiveSmallDataTable, hiveOrcZlibTable);
    }

    void prepareHiveCollection() throws Exception {

        if (hiveCollectionTable != null)
            return;
        hiveCollectionTable = new HiveTable(HIVE_COLLECTIONS_TABLE, HIVE_COLLECTION_COLS);
        hiveCollectionTable.setFormat(FORMAT_ROW);
        hiveCollectionTable.setDelimiterFieldsBy("\\001");
        hiveCollectionTable.setDelimiterCollectionItemsBy("\\002");
        hiveCollectionTable.setDelimiterMapKeysBy("\\003");
        hiveCollectionTable.setDelimiterLinesBy("\\n");
        hiveCollectionTable.setStoredAs(TEXTFILE);
        hive.createTableAndVerify(hiveCollectionTable);
        loadDataIntoHive(HIVE_COLLECTIONS_FILE_NAME, hiveCollectionTable);
    }

    void prepareRCData() throws Exception {

        if (hiveRcTable != null)
            return;
        hiveRcTable = prepareData(HIVE_RC_TABLE, RCFILE);
    }

    void prepareParquetData() throws Exception {

        if (hiveParquetTable != null)
            return;
        hiveParquetTable = prepareData(HIVE_PARQUET_TABLE, PARQUET);
    }

    void prepareParquetForAlterData() throws Exception {

        if (hiveParquetForAlterTable != null)
            return;
        hiveParquetForAlterTable = prepareData(HIVE_PARQUET_FOR_ALTER_TABLE, PARQUET);
    }

    void prepareParquetMismatchSourceTable() throws Exception {
        if (hiveParquetMismatchSourceTable != null)
            return;
        hiveParquetMismatchSourceTable = prepareTableData(hdfs, hive,null,
                HIVE_PARQUET_MISMATCH_SOURCE_TABLE,
                HIVE_PARQUET_MISMATCH_SOURCE_TABLE_COLS,
                HIVE_PARQUET_MISMATCH_DATA_FILE_NAME);
    }

    void prepareAvroData() throws Exception {

        if (hiveAvroTable != null)
            return;
        hiveAvroTable = prepareData(HIVE_AVRO_TABLE, AVRO);
    }

    void prepareSequenceData() throws Exception {

        if (hiveSequenceTable != null)
            return;
        hiveSequenceTable = TableFactory.getHiveByRowCommaTable(HIVE_SEQUENCE_TABLE, HIVE_RC_COLS);
        hiveSequenceTable.setStoredAs(SEQUENCEFILE);
        hive.createTableAndVerify(hiveSequenceTable);
        hive.insertData(hiveSmallDataTable, hiveSequenceTable);
    }

    void prepareBinaryData() throws Exception {

        if (hiveBinaryTable != null)
            return;
        hiveBinaryTable = new HiveTable(HIVE_BINARY_TABLE, HIVE_TYPES_BINARY);
        hive.createTableAndVerify(hiveBinaryTable);
        loadDataIntoHive("hiveBinaryData", hiveBinaryTable);
    }

    void prepareNonDefaultSchemaData() throws Exception {

        if (hiveNonDefaultSchemaTable != null)
            return;
        hiveNonDefaultSchemaTable = TableFactory.getHiveByRowCommaTable(
                HIVE_SMALL_DATA_TABLE, HIVE_SCHEMA, new String[]{"id INT", "name STRING"});
        hive.createDataBase(HIVE_SCHEMA, true);
        hive.createTableAndVerify(hiveNonDefaultSchemaTable);
    }

    void prepareOpenCsvData() throws Exception {

        if (hiveOpenCsvTable != null)
            return;
        hiveOpenCsvTable = new HiveTable(HIVE_OPEN_CSV_TABLE, HIVE_RC_COLS);
        hiveOpenCsvTable.setFormat("ROW");
        hiveOpenCsvTable.setStoredAs(OPEN_CSV_INPUT_OUTPUT_FORMAT);
        hiveOpenCsvTable.setSerde("org.apache.hadoop.hive.serde2.OpenCSVSerde");
        hive.createTableAndVerify(hiveOpenCsvTable);
        hive.insertData(hiveSmallDataTable, hiveOpenCsvTable);
    }

    void addHivePartition(String hiveTable, String partition, String location) throws Exception {

        hive.runQuery("ALTER TABLE " + hiveTable + " ADD PARTITION ("
                + partition + ") LOCATION " + location);
    }

    void setHivePartitionFormat(String hiveTable, String partition, String format) throws Exception {

        hive.runQuery("ALTER TABLE " + hiveTable + " PARTITION ("
                + partition + ") SET FILEFORMAT " + format);
    }

    /**
     * Adds a partition to given hive table, with partition parameter as specified.
     * The partition is another hive table whose location is used.
     */
    void addTableAsPartition(HiveTable hiveTable, String partition, HiveTable partitionTable)
            throws Exception {

        hive.runQuery("ALTER TABLE " + hiveTable.getName()
                + " ADD PARTITION (" + partition + ") " + "LOCATION '"
                + configuredNameNodeAddress + cluster.getHiveBaseHdfsDirectory()
                + partitionTable.getName() + "'");
    }

    /**
     * Creates Hive table with 3 partitions, each with a different storage
     * (text, rc, sequence)
     *
     * @param tableName hive table name
     * @throws Exception if test fails to run
     */
    void createHivePartitionTable(String tableName) throws Exception {

        prepareSmallData();
        prepareRCData();
        prepareSequenceData();
        prepareOrcData();

        addHivePartition(tableName, "fmt = 'txt'",
                "'hdfs:" + hdfsBaseDir + hiveSmallDataTable.getName() + "'");
        addHivePartition(tableName, "fmt = 'rc'",
                "'hdfs:" + hdfsBaseDir + hiveRcTable.getName() + "'");
        addHivePartition(tableName, "fmt = 'seq'",
                "'hdfs:" + hdfsBaseDir + hiveSequenceTable.getName() + "'");
        addHivePartition(tableName, "fmt = 'orc'",
                "'hdfs:" + hdfsBaseDir + hiveOrcTable.getName() + "'");

        setHivePartitionFormat(tableName, "fmt = 'rc'", RCFILE);
        setHivePartitionFormat(tableName, "fmt = 'seq'", SEQUENCEFILE);
        setHivePartitionFormat(tableName, "fmt = 'orc'", ORC);
    }

    /**
     * Creates Hive table with 4 partitions, each with a different storage
     * (text, rc, sequence, orc).
     * Each partitions has different data
     *
     * @param tableName hive table name
     * @return hive table
     * @throws Exception if test fails to run
     */
    HiveExternalTable createGenerateHivePartitionTable(String tableName) throws Exception {

        String smallDataTableName = hiveSmallDataTable.getName();
        HiveExternalTable hiveTable = TableFactory.getHiveByRowCommaExternalTable(tableName, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hive.createTableAndVerify(hiveTable);

        HiveTable hiveRcTablePartition = TableFactory.getHiveByRowCommaTable(
                "hive_rc_table_partition", HIVE_SMALLDATA_COLS);
        hiveRcTablePartition.setStoredAs(RCFILE);
        hive.createTableAndVerify(hiveRcTablePartition);
        hive.runQuery("INSERT INTO " + hiveRcTablePartition.getName() +
                " SELECT CONCAT(t1, '_rc'), CONCAT(t2, '_1'), num1 + 100, dub1 + 1000 FROM " + smallDataTableName);

        HiveTable hiveTxtTablePartition = TableFactory.getHiveByRowCommaTable(
                "hive_txt_table_partition", HIVE_SMALLDATA_COLS);
        hive.createTableAndVerify(hiveTxtTablePartition);
        hive.runQuery("INSERT INTO " + hiveTxtTablePartition.getName() +
                " SELECT CONCAT(t1, '_txt'), CONCAT(t2, '_2'), num1 + 200, dub1 + 2000 FROM " + smallDataTableName);

        HiveTable hiveSeqTablePartition = TableFactory.getHiveByRowCommaTable(
                "hive_seq_table_partition", HIVE_SMALLDATA_COLS);
        hiveSeqTablePartition.setStoredAs(SEQUENCEFILE);
        hive.createTableAndVerify(hiveSeqTablePartition);
        hive.runQuery("INSERT INTO " + hiveSeqTablePartition.getName() +
                " SELECT CONCAT(t1, '_seq'), CONCAT(t2, '_3'), num1 + 300, dub1 + 3000 FROM " + smallDataTableName);

        HiveTable hiveOrcTablePartition = TableFactory.getHiveByRowCommaTable(
                "hive_orc_table_partition", HIVE_SMALLDATA_COLS);
        hiveOrcTablePartition.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcTablePartition);
        hive.runQuery("INSERT INTO " + hiveOrcTablePartition.getName() +
                " SELECT CONCAT(t1, '_orc'), CONCAT(t2, '_4'), num1 + 400, dub1 + 4000 FROM " + smallDataTableName);

        hive.runQuery("ALTER TABLE " + tableName + " ADD PARTITION (fmt = 'txt') LOCATION 'hdfs:"
                + hdfsBaseDir + hiveTxtTablePartition.getName() + "'");
        hive.runQuery("ALTER TABLE " + tableName + " ADD PARTITION (fmt = 'rc') LOCATION 'hdfs:"
                + hdfsBaseDir + hiveRcTablePartition.getName() + "'");
        hive.runQuery("ALTER TABLE " + tableName + " ADD PARTITION (fmt = 'seq') LOCATION 'hdfs:"
                + hdfsBaseDir + hiveSeqTablePartition.getName() + "'");
        hive.runQuery("ALTER TABLE " + tableName + " ADD PARTITION (fmt = 'orc') LOCATION 'hdfs:"
                + hdfsBaseDir + hiveOrcTablePartition.getName() + "'");

        setHivePartitionFormat(tableName, "fmt = 'rc'", RCFILE);
        setHivePartitionFormat(tableName, "fmt = 'seq'", SEQUENCEFILE);
        setHivePartitionFormat(tableName, "fmt = 'orc'", ORC);

        return hiveTable;
    }

    /**
     * First, creates 3 different sets of Parquet files using 3 Hive writable tables.
     * The files from each table will then be used in the readable table partitions:
     * - partition A: has 3 GP columns, 'extra_hive_par', --> missing a column 's2' present in Hive and GP tables
     * - partition B: has 4 GP columns, 'extra_hive_par'
     * - partition C: has 4 GP columns, 'extra_hive_par', --> and 'extra_par' column (not present in Hive or GP)
     * The order of the columns in parquet files for all partitions is different

     * Then, creates Hive readable partitioned table that has the same 4 columns as GP and 2 extra columns:
     * - 'extra_hive_par' -- column that is present in Hive table and in all underlying Parquet files
     * - 'extra_hive'     -- column that is present in Hive table, but not in any underlying Parquet files
     *
     * @throws Exception if test fails to run
     */
    void createHiveParquetColumnMismatchPartitionTable() throws Exception {

        String[] partitionColumns = new String[]{"part"};
        String[] partitionColumnsWithType = new String[]{"part STRING"};
        String location = "hdfs:" + hdfsBaseDir + HIVE_PARQUET_MISMATCH_TABLE;

        hive.runQuery("SET hive.exec.dynamic.partition = true");
        hive.runQuery("SET hive.exec.dynamic.partition.mode = nonstrict");

        // process partition A
        String writeTableName = "hive_parquet_mismatch_write_a";
        HiveTable hiveParquetExtTableA = new HiveExternalTable(writeTableName, HIVE_PARQUET_MISMATCH_WRITE_PART_A_COLS, location);
        hiveParquetExtTableA.setPartitionedBy(partitionColumnsWithType);
        hiveParquetExtTableA.setStoredAs(PARQUET);
        hive.createTableAndVerify(hiveParquetExtTableA);
        hive.insertDataToPartition(hiveParquetMismatchSourceTable, hiveParquetExtTableA,
                partitionColumns, HIVE_PARQUET_MISMATCH_WRITE_PART_A_COL_NAMES, "part='a'");

        // process partition B
        writeTableName = "hive_parquet_mismatch_write_b";
        HiveTable hiveParquetExtTableB = new HiveExternalTable(writeTableName, HIVE_PARQUET_MISMATCH_WRITE_PART_B_COLS, location);
        hiveParquetExtTableB.setPartitionedBy(partitionColumnsWithType);
        hiveParquetExtTableB.setStoredAs(PARQUET);
        hive.createTableAndVerify(hiveParquetExtTableB);
        hive.insertDataToPartition(hiveParquetMismatchSourceTable, hiveParquetExtTableB,
                partitionColumns, HIVE_PARQUET_MISMATCH_WRITE_PART_B_COL_NAMES, "part='b'");

        // process partition B
        writeTableName = "hive_parquet_mismatch_write_c";
        HiveTable hiveParquetExtTableC = new HiveExternalTable(writeTableName, HIVE_PARQUET_MISMATCH_WRITE_PART_C_COLS, location);
        hiveParquetExtTableC.setPartitionedBy(partitionColumnsWithType);
        hiveParquetExtTableC.setStoredAs(PARQUET);
        hive.createTableAndVerify(hiveParquetExtTableC);
        hive.insertDataToPartition(hiveParquetMismatchSourceTable, hiveParquetExtTableC,
                partitionColumns, HIVE_PARQUET_MISMATCH_WRITE_PART_C_COL_NAMES, "part='c'");

        // create readable Hive table with a fixed schema to read all 3 partitions
        hiveParquetMismatchTable = new HiveExternalTable(HIVE_PARQUET_MISMATCH_TABLE, HIVE_PARQUET_MISMATCH_TABLE_COLS, location);
        hiveParquetMismatchTable.setPartitionedBy(partitionColumnsWithType);
        hiveParquetMismatchTable.setStoredAs(PARQUET);
        hive.createTableAndVerify(hiveParquetMismatchTable);
        // add existing data as partitions to the table
        hive.alterTableAddPartition(hiveParquetMismatchTable, new String[]{"part='a'"});
        hive.alterTableAddPartition(hiveParquetMismatchTable, new String[]{"part='b'"});
        hive.alterTableAddPartition(hiveParquetMismatchTable, new String[]{"part='c'"});
    }

    /**
     * Pump up the comparison table data for partitions test case
     *
     * @throws IOException if test fails to run
     */
    void appendToEachRowOfComparisonTable(List<String> values) throws IOException {

        comparisonDataTable.loadDataFromFile(
                localDataResourcesFolder + "/hive/" + HIVE_DATA_FILE_NAME, ",", 0);

        // get original number of line before pump
        int originalNumberOfLines = comparisonDataTable.getData().size();
        for (String value : values) {
            for (int j = 0; j < originalNumberOfLines; j++) {
                comparisonDataTable.getData().get(j).add(value);
            }
        }
    }

    protected void createExternalTable(String tableName, String[] fields,
                                       HiveTable hiveTable, boolean useProfile, String serverName)
            throws Exception {

        exTable = TableFactory.getPxfHiveReadableTable(tableName, fields, hiveTable, useProfile);
        if (serverName != null) {
            exTable.setServer(serverName);
        }
        createTable(exTable);

    }

    protected void createExternalTable(String tableName, String[] fields,
                                       HiveTable hiveTable, boolean useProfile) throws Exception {

        createExternalTable(tableName, fields, hiveTable, useProfile, null);
    }

    protected void createExternalTable(String tableName, String[] fields,
                                       HiveTable hiveTable) throws Exception {

        createExternalTable(tableName, fields, hiveTable, true);
    }
}
