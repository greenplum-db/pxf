package org.greenplum.pxf.automation.features.hive;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.ArrayList;

public class HiveVectorizedOrcTest extends HiveBaseTest {

    static final String[] HIVE_TYPES_NO_TIMESTAMP_COLS = {
            "t1    STRING",
            "t2    STRING",
            "num1  INT",
            "dub1  DOUBLE",
            "dec1  DECIMAL(38,18)",
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

    static final String[] PXF_HIVE_TYPES_NO_TIMESTAMP_COLS = {
            "t1    TEXT",
            "t2    TEXT",
            "num1  INTEGER",
            "dub1  DOUBLE PRECISION",
            "dec1  NUMERIC",
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

    private HiveTable hiveRepeatingCsvTable;
    private HiveTable hiveRepeatingNoNullsOrcTable;
    private HiveTable hiveRepeatingNullsOrcTable;

    ArrayList<String> hiveTypesNoTMCols = new ArrayList<>(Arrays.asList(HIVE_TYPES_COLS));
    ArrayList<String> gpdbTypesNoTMCols = new ArrayList<>(Arrays.asList(PXF_HIVE_TYPES_COLS));

    void prepareTypesData() throws Exception {

        hiveTypesTable = TableFactory.getHiveByRowCommaTable(HIVE_TYPES_TABLE,
                hiveTypesNoTMCols.toArray(new String[hiveTypesNoTMCols.size()]));

        hive.createTableAndVerify(hiveTypesTable);
        loadDataIntoHive("hive_types_no_timestamp.txt", hiveTypesTable);
    }

    void prepareOrcData() throws Exception {

        hiveOrcTable = new HiveTable(HIVE_ORC_TABLE, HIVE_RC_COLS);
        hiveOrcTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcTable);
        hive.insertData(hiveSmallDataTable, hiveOrcTable);

        hiveOrcAllTypes = new HiveTable("hive_orc_all_types",
                hiveTypesNoTMCols.toArray(new String[hiveTypesNoTMCols.size()]));
        hiveOrcAllTypes.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcAllTypes);
        hive.insertData(hiveTypesTable, hiveOrcAllTypes);
    }

    private void preparePxfHiveOrcTypes() throws Exception {
        exTable = TableFactory.getPxfHiveOrcReadableTable(PXF_HIVE_ORC_TABLE,
                gpdbTypesNoTMCols.toArray(new String[gpdbTypesNoTMCols.size()]), hiveOrcAllTypes, true);
        // this profile is now deprecated
        exTable.setProfile("HiveVectorizedORC");
        createTable(exTable);
    }

    private void prepareOrcDataWithRepeatingData() throws Exception {
        String dataFileName = "hive_types_all_columns_repeating.txt";
        // timestamp conversion is not supported by HiveORCVectorizedResolver

        hiveRepeatingCsvTable = prepareTableData(hdfs, hive, hiveRepeatingCsvTable, "hive_types_all_columns_repeating_csv", HIVE_TYPES_NO_TIMESTAMP_COLS, "hive_types_all_columns_repeating.txt");

        hiveRepeatingNoNullsOrcTable = new HiveTable("hive_types_all_columns_repeating_no_nulls_orc", HIVE_TYPES_NO_TIMESTAMP_COLS);
        hiveRepeatingNoNullsOrcTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveRepeatingNoNullsOrcTable);
        hive.insertData(hiveRepeatingCsvTable, hiveRepeatingNoNullsOrcTable);

        hiveRepeatingCsvTable = prepareTableData(hdfs, hive, null, "hive_types_all_columns_repeating_csv", HIVE_TYPES_NO_TIMESTAMP_COLS, "hive_types_all_columns_repeating_nulls.txt");

        hiveRepeatingNullsOrcTable = new HiveTable("hive_types_all_columns_repeating_nulls_orc", HIVE_TYPES_NO_TIMESTAMP_COLS);
        hiveRepeatingNullsOrcTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveRepeatingNullsOrcTable);
        hive.insertData(hiveRepeatingCsvTable, hiveRepeatingNullsOrcTable);

    }

    @Override
    void prepareData() throws Exception {

        // Remove timestamp column
        hiveTypesNoTMCols.remove(5);
        gpdbTypesNoTMCols.remove(5);

        prepareTypesData();
        prepareSmallData();
        prepareOrcData();
        preparePxfHiveOrcTypes();
    }

    /**
     * PXF on Hive ORC format all hive primitive types
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog", "features", "gpdb", "security" })
    public void storeAsOrcAllTypes() throws Exception {

        exTable = TableFactory.getPxfHiveOrcReadableTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTable, true);
        createTable(exTable);

        Table gpdbNativeTable = new Table(GPDB_SMALL_DATA_TABLE, PXF_HIVE_SMALLDATA_COLS);
        gpdbNativeTable.setDistributionFields(new String[] { "t1" });
        gpdb.createTableAndVerify(gpdbNativeTable);
        gpdb.copyData(exTable, gpdbNativeTable);

        runSqlTest("features/hive/orc_primitive_types_no_timestamp");
    }

    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void columsnWithRepeating() throws Exception {
        prepareOrcDataWithRepeatingData();

        exTable = TableFactory.getPxfHiveVectorizedOrcReadableTable("pxf_hivevectorizedorc_repeating_no_nulls", PXF_HIVE_TYPES_NO_TIMESTAMP_COLS, hiveRepeatingNoNullsOrcTable, true);
        createTable(exTable);

        exTable = TableFactory.getPxfHiveVectorizedOrcReadableTable("pxf_hivevectorizedorc_repeating_nulls", PXF_HIVE_TYPES_NO_TIMESTAMP_COLS, hiveRepeatingNullsOrcTable, true);
        createTable(exTable);

        exTable = TableFactory.getPxfHiveVectorizedOrcReadableTable("pxf_hive_orc_vectorize_repeating_no_nulls", PXF_HIVE_TYPES_NO_TIMESTAMP_COLS, hiveRepeatingNoNullsOrcTable, true);
        exTable.setProfile("hive:orc");
        exTable.setUserParameters(new String[] { "VECTORIZE=true" });
        createTable(exTable);

        exTable = TableFactory.getPxfHiveVectorizedOrcReadableTable("pxf_hive_orc_vectorize_repeating_nulls", PXF_HIVE_TYPES_NO_TIMESTAMP_COLS, hiveRepeatingNullsOrcTable, true);
        exTable.setProfile("hive:orc");
        exTable.setUserParameters(new String[] { "VECTORIZE=true" });
        createTable(exTable);

        runSqlTest("features/hive/orc_repeating");
    }

}
