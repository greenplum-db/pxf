package org.greenplum.pxf.automation.features.general;

import annotations.SkipForFDW;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.postgresql.util.PSQLException;
import org.testng.annotations.Test;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;

import java.io.File;

@SkipForFDW
public class PluginTest extends BaseFeature {

    private String resourcePath;

    private final String SUFFIX_CLASS = ".class";

    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String testPackage = "org.greenplum.pxf.automation.testplugin.";

    String[] testPluginFileNames = { "DummyFragmenter", "DummyAccessor", "DummyResolver",
            "FaultyGUCFragmenter", "FaultyGUCAccessor" };

    @Override
    public void beforeClass() throws Exception {
        // location of test plugin files
        resourcePath = "target/classes" + testPackageLocation;

        String newPath = "/tmp/publicstage/pxf";
        // copy test plugin files to cluster nodes
        for(String testPluginFileName: testPluginFileNames) {
            cluster.copyFileToNodes(new File(resourcePath + testPluginFileName + SUFFIX_CLASS).getAbsolutePath(),
                    newPath + testPackageLocation, true, false);
        }

        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    /**
     * Create PXF external readable table using external plugins for Fragmenter,
     * Accessor, and Resolver.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void readableTableExternalPlugins() throws Exception {

        ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
                "num1 integer",
                "t1 text",
                "num2 integer" }, "regression_location", "CUSTOM");

        exTable.setFragmenter(testPackage + "DummyFragmenter");
        exTable.setAccessor(testPackage + "DummyAccessor");
        exTable.setResolver(testPackage + "DummyResolver");
        exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
        exTable.setFormatter("pxfwritable_import");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        gpdb.createTableAndVerify(exTable);
        gpdb.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");

        Table dataCompareTable = new Table("dataCompareTable", null);

        dataCompareTable.addRow(new String[] { "0", "fragment1" });
        dataCompareTable.addRow(new String[] { "0", "fragment2" });
        dataCompareTable.addRow(new String[] { "0", "fragment3" });
        dataCompareTable.addRow(new String[] { "1", "fragment1" });
        dataCompareTable.addRow(new String[] { "1", "fragment2" });
        dataCompareTable.addRow(new String[] { "1", "fragment3" });

        ComparisonUtils.compareTables(exTable, dataCompareTable, null);

        gpdb.analyze(exTable);
    }

    /**
     * Test options are case insensitive
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void optionsCaseInsensitive() throws Exception {
        ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
                "num1 integer",
                "t1 text",
                "num2 integer" }, "regression_location", "CUSTOM");

        exTable.setUserParameters(new String[] {
                "fragmenter=org.greenplum.pxf.automation.testplugin.DummyFragmenter",
                "Accessor=org.greenplum.pxf.automation.testplugin.DummyAccessor",
                "ReSoLvEr=org.greenplum.pxf.automation.testplugin.DummyResolver" });
        exTable.setFormatter("pxfwritable_import");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        gpdb.createTableAndVerify(exTable);
        gpdb.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");

        Table dataCompareTable = new Table("dataCompareTable", null);

        dataCompareTable.addRow(new String[] { "0", "fragment1" });
        dataCompareTable.addRow(new String[] { "0", "fragment2" });
        dataCompareTable.addRow(new String[] { "0", "fragment3" });
        dataCompareTable.addRow(new String[] { "1", "fragment1" });
        dataCompareTable.addRow(new String[] { "1", "fragment2" });
        dataCompareTable.addRow(new String[] { "1", "fragment3" });

        ComparisonUtils.compareTables(exTable, dataCompareTable, null);
    }

    /**
     * Create PXF external writable table using external plugins
     * Accessor and Resolver.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void writableTableExternalPlugins() throws Exception {

        WritableExternalTable exTable = new WritableExternalTable("extens_write", new String[] {
                "t1 text",
                "t2 text" }, "regression_location", "CUSTOM");

        exTable.setAccessor(testPackage +"DummyAccessor");
        exTable.setResolver(testPackage +"DummyResolver");
        exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
        exTable.setFormatter("pxfwritable_export");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        gpdb.createTableAndVerify(exTable);

        Table dataTable = new Table("dataTable", null);

        dataTable.addRow(new String[] { "something", "big" });
        dataTable.addRow(new String[] { "is", "going" });
        dataTable.addRow(new String[] { "to", "happen" });

        gpdb.insertData(dataTable, exTable);
    }

    @Test(groups = "features")
    public void credentialsGUCsTransferredToFragmenter() throws Exception {

        ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
                "num1 integer",
                "t1 text",
                "num2 integer" }, "regression_location", "CUSTOM");

        exTable.setFragmenter(testPackage + "FaultyGUCFragmenter");
        exTable.setAccessor(testPackage + "DummyAccessor");
        exTable.setResolver(testPackage + "DummyResolver");
        exTable.setFormatter("pxfwritable_import");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        gpdb.runQuery("SET pxf_remote_service_login = 'mommy'");
        gpdb.runQuery("SET pxf_remote_service_secret = 'daddy'");
        gpdb.createTableAndVerify(exTable);
        try {
            gpdb.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");
        } catch (Exception e) {
            ExceptionUtils.validate(null, e, new PSQLException("FaultyGUCFragmenter: login mommy secret daddy", null), true);
        } finally {
            gpdb.runQuery("SET pxf_remote_service_login = ''");
            gpdb.runQuery("SET pxf_remote_service_secret = ''");
        }
    }

    @Test(groups = "features")
    public void credentialsGUCsTransferredToAccessor() throws Exception {

        ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
                "num1 integer",
                "t1 text",
                "num2 integer" }, "regression_location", "CUSTOM");

        exTable.setFragmenter(testPackage + "DummyFragmenter");
        exTable.setAccessor(testPackage + "FaultyGUCAccessor");
        exTable.setResolver(testPackage + "DummyResolver");
        exTable.setFormatter("pxfwritable_import");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        gpdb.runQuery("SET pxf_remote_service_login = 'mommy'");
        gpdb.runQuery("SET pxf_remote_service_secret = 'daddy'");
        gpdb.createTableAndVerify(exTable);
        try {
            gpdb.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");
        } catch (Exception e) {
            ExceptionUtils.validate(null, e, new PSQLException("FaultyGUCAccessor: login mommy secret daddy", null), true);
        } finally {
            gpdb.runQuery("SET pxf_remote_service_login = ''");
            gpdb.runQuery("SET pxf_remote_service_secret = ''");
        }
    }

    @Test(groups = "features")
    public void emptyCredentialsGUCsTransferredAsNull() throws Exception {

        ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
                "num1 integer",
                "t1 text",
                "num2 integer" }, "regression_location", "CUSTOM");

        exTable.setFragmenter(testPackage + "DummyFragmenter");
        exTable.setAccessor(testPackage + "FaultyGUCAccessor");
        exTable.setResolver(testPackage + "DummyResolver");
        exTable.setFormatter("pxfwritable_import");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        gpdb.runQuery("SET pxf_remote_service_login = ''");
        gpdb.runQuery("SET pxf_remote_service_secret = ''");
        gpdb.createTableAndVerify(exTable);
        try {
            gpdb.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");
        } catch (Exception e) {
            ExceptionUtils.validate(null, e, new PSQLException("FaultyGUCAccessor: login null secret null", null), true);
        }
    }

    @Test(groups = "features")
    public void defaultCredentialsGUCsTransferredAsNull() throws Exception {

        ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
                "num1 integer",
                "t1 text",
                "num2 integer" }, "regression_location", "CUSTOM");

        exTable.setFragmenter(testPackage + "DummyFragmenter");
        exTable.setAccessor(testPackage + "FaultyGUCAccessor");
        exTable.setResolver(testPackage + "DummyResolver");
        exTable.setFormatter("pxfwritable_import");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        gpdb.createTableAndVerify(exTable);
        try {
            gpdb.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");
        } catch (Exception e) {
            ExceptionUtils.validate(null, e, new PSQLException("FaultyGUCAccessor: login null secret null", null), true);
        }
    }
}
