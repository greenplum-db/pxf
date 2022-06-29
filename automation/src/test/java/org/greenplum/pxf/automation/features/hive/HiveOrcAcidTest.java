package org.greenplum.pxf.automation.features.hive;

import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class HiveOrcAcidTest extends HiveBaseTest {

    private HiveTable hiveOrcSmallDataTable;
    private HiveTable hiveOrcPartitionedTable;

    private String ACID_POSTPEND = "_acid";
    private List<String> transactionalTrue = Arrays.asList("transactional", "true");
    private List<String> defaultTransactionalProperties = Arrays.asList("transactional_properties", "default");

    @Override
    protected void createExternalTable(String tableName, String[] fields, HiveTable hiveTable) throws Exception {

        exTable = TableFactory.getPxfHiveOrcReadableTable(tableName, fields, hiveTable, true);
        createTable(exTable);
    }

    private void setAcidHiveSessionProperties() throws Exception {
        hive.runQuery("SET hive.support.concurrency = true");
        hive.runQuery("SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        hive.runQuery("SET hive.enforce.bucketing = true");
        hive.runQuery("SET hive.exec.dynamic.partition.mode = nonstrict");
        hive.runQuery("SET hive.compactor.initiator.on = true");
        hive.runQuery("SET hive.compactor.worker.threads = 1");
    }

    @Override
    void prepareData() throws Exception {

        prepareSmallData();
    }

    @Override
    void prepareSmallData() throws Exception {

        super.prepareSmallData();
        // Create a copy of small data in ORC format on an ACID table
        hiveOrcSmallDataTable = new HiveTable(HIVE_SMALL_DATA_TABLE + "_orc" + ACID_POSTPEND, HIVE_SMALLDATA_COLS);
        hiveOrcSmallDataTable.setStoredAs(ORC);
        // Hive 1.2.1000 (used in singlecluster) requires a table to be bucketed to be considered ACID compliant.
        hiveOrcSmallDataTable.setClusteredBy(new String[]{"num1"});
        hiveOrcSmallDataTable.setClusterBucketCount(1);
        hiveOrcSmallDataTable.setTableProperties(Arrays.asList(transactionalTrue, defaultTransactionalProperties));

        setAcidHiveSessionProperties();

        hive.createTableAndVerify(hiveOrcSmallDataTable);
    }

    private void preparePartitionedData() throws Exception {

        hiveOrcPartitionedTable = new HiveTable(HIVE_PARTITIONED_TABLE + ACID_POSTPEND, HIVE_RC_COLS);
        hiveOrcPartitionedTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hiveOrcPartitionedTable.setStoredAs(ORC);
        // Hive 1.2.1000 (used in singlecluster) requires a table to be bucketed to be considered ACID compliant.
        hiveOrcPartitionedTable.setClusteredBy(new String[]{"num1"});
        hiveOrcPartitionedTable.setClusterBucketCount(1);
        hiveOrcPartitionedTable.setTableProperties(Arrays.asList(transactionalTrue, defaultTransactionalProperties));

        setAcidHiveSessionProperties();

        hive.createTableAndVerify(hiveOrcPartitionedTable);
    }

    /**
     * Query for small data hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void sanity() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE + "_orc" + ACID_POSTPEND,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcSmallDataTable);

        runTincTest("pxf.features.hive.small_data_orc_acid.runTest");
    }


    /**
     * PXF on Hive table partitioned to one field
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void hivePartitionedTable() throws Exception {

        preparePartitionedData();
        // Create PXF Table using HiveOrc profile
        createExternalTable(PXF_HIVE_PARTITIONED_TABLE + ACID_POSTPEND,
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveOrcPartitionedTable);

        runTincTest("pxf.features.hive.hive_partitioned_table_orc_acid.runTest");
    }

}
