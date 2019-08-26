package org.greenplum.pxf.automation.features.hive;

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
            "si    SMALLINT",
            "ba    BYTEA"
    };
    static final String[] HIVE_TYPES_COLS = {
            "s1    STRING",
            "s2    STRING",
            "n1    INT",
            "d1    DOUBLE",
            "dc1   DECIMAL(38,18)",
            "tm    TIMESTAMP",
            "f     FLOAT",
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
            "s1    STRING",
            "n1    INT",
            "d1    DOUBLE",
            "tm    TIMESTAMP",
            "f     FLOAT",
            "bg    BIGINT",
            "b     BOOLEAN",
            "si    SMALLINT",
            "bin   BINARY"
    };
    static final String[] HIVE_SMALLDATA_COLS = {
            "s1    STRING",
            "s2    STRING",
            "n1    INT",
            "d1    DOUBLE"
    };
    static final String[] HIVE_RC_COLS = { // TODO: dedup with HIVE_SMALLDATA_COLS above
            "t1    STRING",
            "t2    STRING",
            "num1  INT",
            "d1    DOUBLE"
    };
    // Hive Table columns for Predicate pushdown test on partitioned hive table
    static final String[] HIVE_SMALLDATA_PPD_COLS = {
            "s1    STRING",
            "d1    DOUBLE",
    };
    static final String[] PXF_HIVE_SMALLDATA_COLS = {
            "t1    TEXT",
            "t2    TEXT",
            "num1  INTEGER",
            "dub1  DOUBLE PRECISION"
    };
    static final String[] PXF_HIVE_SMALLDATA_FMT_COLS = {
            "t1    TEXT",
            "t2    TEXT",
            "num1  INTEGER",
            "dub1  DOUBLE PRECISION",
            "fmt   TEXT"
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
            "s1    TEXT",
            "d1    DOUBLE PRECISION",
            "s2    TEXT",
            "n1    INTEGER"
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
            "s1    STRING",
            "f1    FLOAT",
            "a1    ARRAY<STRING>",
            "m1    MAP<STRING, FLOAT>",
            "sr1   STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>",
            "ut1   UNIONTYPE<STRING, INT, ARRAY<INT>, ARRAY<STRING>>"
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
            "s2    STRING",
            "n1    INT"
    };

    static final String HIVE_TYPES_DATA_FILE_NAME = "hive_types.txt";
    static final String HIVE_COLLECTIONS_FILE_NAME = "hive_collections.txt";
    static final String HIVE_DATA_FILE_NAME = "hive_small_data.txt";
    static final String HIVE_TYPES_LIMITED_FILE_NAME = "hive_types_limited.txt";

    static final String HIVE_SMALL_DATA_TABLE = "hive_small_data";
    static final String HIVE_TYPES_TABLE = "hive_types";
    static final String HIVE_TEXT_TABLE = "hive_text_table";
    static final String HIVE_RC_TABLE = "hive_rc_table";
    static final String HIVE_ORC_TABLE = "hive_orc_table";
    static final String HIVE_ORC_SNAPPY_TABLE = "hive_orc_snappy";
    static final String HIVE_ORC_ZLIB_TABLE = "hive_orc_zlib";
    static final String HIVE_BINARY_TABLE = "hive_binary";
    static final String HIVE_COLLECTIONS_TABLE = "hive_collections_table";
    static final String HIVE_AVRO_TABLE = "hive_avro_table";
    static final String HIVE_PARQUET_TABLE = "hive_parquet_table";
    static final String HIVE_SEQUENCE_TABLE = "hive_sequence_table";
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
    static final String PXF_HIVE_HETEROGEN_TABLE = "pxf_hive_heterogen";
    static final String GPDB_SMALL_DATA_TABLE = "gpdb_small_data";
    static final String GPDB_HIVE_TYPES_TABLE = "gpdb_hive_types";

    static final String HIVE_SCHEMA = "userdb";
    static final String AVRO = "AVRO";
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
    HiveTable hiveSequenceTable;
    HiveTable hiveParquetTable;
    HiveTable hiveAvroTable;
    HiveTable hiveBinaryTable;
    HiveTable hiveCollectionTable;
    HiveTable hiveNonDefaultSchemaTable;
    Table comparisonDataTable;

    String configuredNameNodeAddress;
    String hdfsBaseDir;

    @Override
    protected void beforeClass() throws Exception {

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
        hdfsBaseDir = cluster.getHiveBaseHdfsDirectory();

        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");

        // get configured Name Node
        configuredNameNodeAddress = hdfs.getConfiguredNameNodeAdress();

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

        // copy data to hdfs
        hdfs.copyFromLocal(localDataResourcesFolder + "/hive/" + fileName,
                hdfs.getWorkingDirectory() + "/" + fileName);
        // load to hive table
        hive.loadData(tableName, hdfs.getWorkingDirectory() + "/" + fileName, false);
    }

    String[] hiveTestFilter(String filterString) {
        return new String[] { "TEST-HIVE-FILTER=" + filterString };
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

        if (hiveSmallDataTable != null)
            return;
        hiveSmallDataTable = TableFactory.getHiveByRowCommaTable(HIVE_SMALL_DATA_TABLE, HIVE_SMALLDATA_COLS);
        hive.createTableAndVerify(hiveSmallDataTable);
        loadDataIntoHive(HIVE_DATA_FILE_NAME, hiveSmallDataTable);
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

    void prepareBinaryData() throws Exception{

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
                HIVE_SMALL_DATA_TABLE, HIVE_SCHEMA, new String[] { "id INT", "name STRING" });
    	hive.createDataBase(HIVE_SCHEMA, true);
    	hive.createTableAndVerify(hiveNonDefaultSchemaTable);
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
     *
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
                " SELECT CONCAT(s1, '_rc'), CONCAT(s2, '_1'), n1 + 100, d1 + 1000 FROM " + smallDataTableName);

        HiveTable hiveTxtTablePartition = TableFactory.getHiveByRowCommaTable(
                "hive_txt_table_partition", HIVE_SMALLDATA_COLS);
        hive.createTableAndVerify(hiveTxtTablePartition);
        hive.runQuery("INSERT INTO " + hiveTxtTablePartition.getName() +
                " SELECT CONCAT(s1, '_txt'), CONCAT(s2, '_2'), n1 + 200, d1 + 2000 FROM " + smallDataTableName);

        HiveTable hiveSeqTablePartition = TableFactory.getHiveByRowCommaTable(
                "hive_seq_table_partition", HIVE_SMALLDATA_COLS);
        hiveSeqTablePartition.setStoredAs(SEQUENCEFILE);
        hive.createTableAndVerify(hiveSeqTablePartition);
        hive.runQuery("INSERT INTO " + hiveSeqTablePartition.getName() +
                " SELECT CONCAT(s1, '_seq'), CONCAT(s2, '_3'), n1 + 300, d1 + 3000 FROM " + smallDataTableName);

        HiveTable hiveOrcTablePartition = TableFactory.getHiveByRowCommaTable(
                "hive_orc_table_partition", HIVE_SMALLDATA_COLS);
        hiveOrcTablePartition.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcTablePartition);
        hive.runQuery("INSERT INTO " + hiveOrcTablePartition.getName() +
                " SELECT CONCAT(s1, '_orc'), CONCAT(s2, '_4'), n1 + 400, d1 + 4000 FROM " + smallDataTableName);

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
     * Pump up the comparison table data for partitions test case
     *
     * @throws IOException if test fails to run
     */
    void appendToEachRowOfComparisonTable(List<String> values) throws IOException {

        comparisonDataTable.loadDataFromFile(
                localDataResourcesFolder + "/hive/" + HIVE_DATA_FILE_NAME,",",0);

        // get original number of line before pump
        int originalNumberOfLines = comparisonDataTable.getData().size();
        for (String value : values) {
            for (int j = 0; j < originalNumberOfLines; j++) {
                comparisonDataTable.getData().get(j).add(value);
            }
        }
    }
}
