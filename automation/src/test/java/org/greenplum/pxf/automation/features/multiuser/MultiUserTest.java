package org.greenplum.pxf.automation.features.multiuser;

import org.greenplum.pxf.automation.components.gpdb.Gpdb;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.io.File;

public class MultiUserTest extends BaseFeature {

    private static final String GPDB_PXF_AUTOMATION_DB_JDBC = "jdbc:postgresql://";
    private static final String[] TYPES_TABLE_FIELDS = new String[]{
            "t1    text",
            "t2    text",
            "num1  int",
            "dub1  double precision",
            "dec1  numeric",
            "tm timestamp",
            "r real",
            "bg bigint",
            "b boolean",
            "tn smallint",
            "sml smallint",
            "dt date",
            "vc1 varchar(5)",
            "c1 char(3)",
            "bin bytea"
    };
    private static final String gpdbTypesDataFileName = "gpdb_types.txt";
    private static final String gpdbTypesExDataFileName = "gpdb_types_ex.txt";
    private Table gpdbNativeTable;
    private ExternalTable pxfJdbcReadable, pxfJdbcReadableOverrideDDL;

    @Override
    protected void beforeClass() throws Exception {
        prepareData();
    }

    protected void prepareData() throws Exception {
        //CustomAutomationLogger.revertStdoutStream();
        prepareNative();
        prepareReadable();
    }

    private void prepareNative() throws Exception {
        gpdbNativeTable = new Table("gpdb_table", TYPES_TABLE_FIELDS);
        gpdbNativeTable.setDistributionFields(new String[]{"t1"});

        // create a native table (used by default user)
        gpdb.createTableAndVerify(gpdbNativeTable);
        gpdb.copyFromFile(gpdbNativeTable, new File(localDataResourcesFolder
                + "/gpdb/" + gpdbTypesDataFileName), "E'\\t'", "E'\\\\N'", true);

        // create a native table in another database (will be used by testuser)
        Gpdb gpdbEx = new Gpdb();
        gpdbEx.setDb("template1");
        gpdbEx.setHost(gpdb.getHost());
        gpdbEx.setMasterHost(gpdb.getMasterHost());
        gpdbEx.init();
        gpdbEx.createTableAndVerify(gpdbNativeTable);
        gpdbEx.copyFromFile(gpdbNativeTable, new File(localDataResourcesFolder
                + "/gpdb/" + gpdbTypesExDataFileName), "E'\\t'", "E'\\\\N'", true);
        gpdbEx.close();
    }

    private void prepareReadable() throws Exception {
        pxfJdbcReadable = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_readable",
                TYPES_TABLE_FIELDS,
                gpdbNativeTable.getName(),
                "database");
        pxfJdbcReadable.setHost(pxfHost);
        pxfJdbcReadable.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcReadable);

        pxfJdbcReadableOverrideDDL = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_readable_overrideddl",
                TYPES_TABLE_FIELDS,
                gpdbNativeTable.getName(),
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                "database");
        pxfJdbcReadableOverrideDDL.setHost(pxfHost);
        pxfJdbcReadableOverrideDDL.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcReadableOverrideDDL);
    }

    @Test(groups = {"features", "gpdb", "security"})
    public void testUsers() throws Exception {
        runTincTest("pxf.features.multi_user.runTest");
    }
}
