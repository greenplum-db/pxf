package org.greenplum.pxf.automation.features.hcfs;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Functional File as Row Test
 */
public class FileAsRowTest extends BaseFeature {

    private String hdfsBasePath;
    private static final String emptyTextFile = "empty";
    private static final String twoLineTextFile = "twoline";
    private static final String singleLineTextFile = "singleline";
    private static final String multiLineTextFile = "multiline";
    private static final String multiLineJsonFile = "tweets-pp.json";

    private static final String[] PXF_MULTILINE_COLS = {"text_blob text"};

    @BeforeClass
    protected void prepareData() throws Exception {
        hdfsBasePath = hdfs.getWorkingDirectory() + "/file_as_row/";

        // copy files to hdfs
        hdfs.copyFromLocal(localDataResourcesFolder + "/json/" + multiLineJsonFile,
                hdfsBasePath + multiLineJsonFile);
        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + emptyTextFile,
                hdfsBasePath + emptyTextFile);
        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + singleLineTextFile,
                hdfsBasePath + singleLineTextFile);
        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + twoLineTextFile,
                hdfsBasePath + twoLineTextFile);
        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + multiLineTextFile,
                hdfsBasePath + multiLineTextFile);

        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + multiLineTextFile,
                hdfsBasePath + "foo/" + multiLineTextFile);
        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + singleLineTextFile,
                hdfsBasePath + "foo/" + singleLineTextFile);
        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + twoLineTextFile,
                hdfsBasePath + "foo/" + twoLineTextFile);
    }

    @AfterClass
    protected void cleanupData() throws Exception {
        hdfs.removeDirectory(hdfsBasePath);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testEmptyFile() throws Exception {
        runTestScenario("empty_text", PXF_MULTILINE_COLS,
                hdfsBasePath + emptyTextFile);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testSingleLineFile() throws Exception {
        runTestScenario("singleline_text", PXF_MULTILINE_COLS,
                hdfsBasePath + singleLineTextFile);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testTwoLineFile() throws Exception {
        runTestScenario("twoline_text", PXF_MULTILINE_COLS,
                hdfsBasePath + twoLineTextFile);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testMultilineFile() throws Exception {
        runTestScenario("text", PXF_MULTILINE_COLS,
                hdfsBasePath + multiLineTextFile);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testMultilineJsonFile() throws Exception {
        runTestScenario("json", new String[]{
                "json_blob json"
        }, hdfsBasePath + multiLineJsonFile);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testMultilineWithDirectory() throws Exception {
        runTestScenario("multi_files", PXF_MULTILINE_COLS,
                hdfsBasePath + "foo/");
    }

    private void runTestScenario(String name, String[] fields,
                                 String hdfsPath) throws Exception {
        String tableName = "file_as_row_" + name;
        exTable = new ReadableExternalTable(tableName, fields, hdfsPath, "CSV");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text:multi");
        exTable.setUserParameters(new String[]{"FILE_AS_ROW=true"});
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hcfs.file_as_row." + name + ".runTest");
    }
}
