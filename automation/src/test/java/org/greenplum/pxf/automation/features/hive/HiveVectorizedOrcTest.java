package org.greenplum.pxf.automation.features.hive;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.ArrayList;

public class HiveVectorizedOrcTest extends HiveBaseTest {

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
        exTable.setProfile("HiveVectorizedORC");
        createTable(exTable);
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
    @Test(groups = { "hive", "hcatalog" })
    public void storeAsOrcAllTypes() throws Exception {

        exTable = TableFactory.getPxfHiveOrcReadableTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTable, true);
        createTable(exTable);

        Table gpdbNativeTable = new Table(GPDB_SMALL_DATA_TABLE, PXF_HIVE_SMALLDATA_COLS);
        gpdbNativeTable.setDistributionFields(new String[] { "t1" });
        gpdb.createTableAndVerify(gpdbNativeTable);
        gpdb.copyData(exTable, gpdbNativeTable);

        runTincTest("pxf.features.hive.orc_primitive_types_no_timestamp.runTest");
    }

}
