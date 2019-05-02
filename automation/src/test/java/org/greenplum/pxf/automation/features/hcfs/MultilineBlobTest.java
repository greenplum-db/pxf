package org.greenplum.pxf.automation.features.hcfs;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

/**
 * Functional Multiline Blob Test
 */
public class MultilineBlobTest extends BaseFeature {

    private String hdfsBasePath;
    private static final String emptyTextFile = "empty";
    private static final String singleLineTextFile = "singleline";
    private static final String multiLineTextFile = "multiline";
    private static final String multiLineJsonFile = "tweets-pp.json";

    private static final String[] PXF_MULTILINE_COLS = {"text_blob text"};

    @Override
    protected void beforeClass() throws Exception {
        super.beforeClass();
        hdfsBasePath = hdfs.getWorkingDirectory() + "/blob/";
        prepareData();
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
        cleanupData();
    }

    protected void prepareData() throws Exception {
        // copy files to hdfs
        hdfs.copyFromLocal(localDataResourcesFolder + "/json/" + multiLineJsonFile,
                hdfsBasePath + multiLineJsonFile);
        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + emptyTextFile,
                hdfsBasePath + emptyTextFile);
        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + singleLineTextFile,
                hdfsBasePath + singleLineTextFile);
        hdfs.copyFromLocal(localDataResourcesFolder + "/text/" + multiLineTextFile,
                hdfsBasePath + multiLineTextFile);
    }

    protected void cleanupData() throws Exception {
        hdfs.removeDirectory(hdfsBasePath);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testEmptyTextBlob() throws Exception {
        runTestScenario("empty_text", PXF_MULTILINE_COLS,
                hdfsBasePath + emptyTextFile);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testSingleLineTextBlob() throws Exception {
        runTestScenario("singleline_text", PXF_MULTILINE_COLS,
                hdfsBasePath + singleLineTextFile);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testMultilineTextBlob() throws Exception {
        runTestScenario("text", PXF_MULTILINE_COLS,
                hdfsBasePath + multiLineTextFile);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testMultilineJsonBlob() throws Exception {
        runTestScenario("json", new String[]{
                "json_blob json"
        }, hdfsBasePath + multiLineJsonFile);
    }

    private void runTestScenario(String name, String[] fields,
                                 String hdfsPath) throws Exception {
        String tableName = "multiline_blob_" + name;
        exTable = new ReadableExternalTable(tableName, fields, hdfsPath, "CSV");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setUserParameters(new String[]{"BLOB=true"});
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hcfs.multiline_blob." + name + ".runTest");
    }
}
