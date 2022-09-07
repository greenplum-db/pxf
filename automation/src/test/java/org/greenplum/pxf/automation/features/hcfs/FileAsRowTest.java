package org.greenplum.pxf.automation.features.hcfs;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

/**
 * Functional File as Row Test
 */
public class FileAsRowTest extends BaseFeature {

    private static final String emptyTextFile = "empty";
    private static final String twoLineTextFile = "twoline";
    private static final String singleLineTextFile = "singleline";
    private static final String multiLineTextFile = "multiline";
    private static final String multiLineJsonFile = "tweets-pp.json";

    private static final String[] PXF_MULTILINE_COLS = {"text_blob text"};

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
        if (hdfs != null) {
            hdfs.removeDirectory(hdfs.getWorkingDirectory() + "/file_as_row/");
        }
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testEmptyFile() throws Exception {
        String hdfsBasePath = hdfs.getWorkingDirectory() + "/file_as_row/";
        String[] srcPaths = {
                localDataResourcesFolder + "/text/" + emptyTextFile};
        runTestScenario("empty_text", PXF_MULTILINE_COLS,
                hdfsBasePath + emptyTextFile, srcPaths);
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testSingleLineFile() throws Exception {
        String hdfsBasePath = hdfs.getWorkingDirectory() + "/file_as_row/";
        String[] srcPaths = {
                localDataResourcesFolder + "/text/" + singleLineTextFile};
        runTestScenario("singleline_text", PXF_MULTILINE_COLS,
                hdfsBasePath + singleLineTextFile, srcPaths);
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testTwoLineFile() throws Exception {
        String hdfsBasePath = hdfs.getWorkingDirectory() + "/file_as_row/";
        String[] srcPaths = {
                localDataResourcesFolder + "/text/" + twoLineTextFile};
        runTestScenario("twoline_text", PXF_MULTILINE_COLS,
                hdfsBasePath + twoLineTextFile, srcPaths);
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMultilineFile() throws Exception {
        String hdfsBasePath = hdfs.getWorkingDirectory() + "/file_as_row/";
        String[] srcPaths = {
                localDataResourcesFolder + "/text/" + multiLineTextFile};
        runTestScenario("text", PXF_MULTILINE_COLS,
                hdfsBasePath + multiLineTextFile, srcPaths);
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMultilineJsonFile() throws Exception {
        String hdfsBasePath = hdfs.getWorkingDirectory() + "/file_as_row/";
        String[] srcPaths = {
                localDataResourcesFolder + "/json/" + multiLineJsonFile};

        runTestScenario("json", new String[]{
                "json_blob json"
        }, hdfsBasePath + multiLineJsonFile, srcPaths);
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMultilineWithDirectory() throws Exception {
        String hdfsBasePath = hdfs.getWorkingDirectory() + "/file_as_row/";
        String[] srcPaths = {
                localDataResourcesFolder + "/text/" + multiLineTextFile,
                localDataResourcesFolder + "/text/" + singleLineTextFile,
                localDataResourcesFolder + "/text/" + twoLineTextFile};

        runTestScenario("multi_files", PXF_MULTILINE_COLS,
                hdfsBasePath + "multi/", srcPaths);
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMultilineWithDirectoryWildcardLocation() throws Exception {
        String hdfsBasePath = hdfs.getWorkingDirectory() + "/file_as_row/";
        String[] srcPaths = {
                localDataResourcesFolder + "/text/" + multiLineTextFile,
                localDataResourcesFolder + "/text/" + singleLineTextFile,
                localDataResourcesFolder + "/text/" + twoLineTextFile};

        runTestScenario("multi_files", PXF_MULTILINE_COLS,
                hdfsBasePath + "multi/", hdfsBasePath + "multi/*line", srcPaths);
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMultilineWithDirectoryWildcardPathLocation() throws Exception {
        String hdfsBasePath = hdfs.getWorkingDirectory() + "/file_as_row/";
        String[] srcPaths = {
                localDataResourcesFolder + "/text/" + multiLineTextFile,
                localDataResourcesFolder + "/text/" + singleLineTextFile,
                localDataResourcesFolder + "/text/" + twoLineTextFile};

        runTestScenario("multi_files", PXF_MULTILINE_COLS,
                hdfsBasePath + "multi/", hdfsBasePath + "m*ti/*line", srcPaths);
    }

    private void runTestScenario(String name, String[] fields, String hdfsPath, String[] srcPaths) throws Exception {
        runTestScenario(name, fields, hdfsPath, hdfsPath, srcPaths);
    }

    private void runTestScenario(String name, String[] fields, String hdfsPath, String locationPath, String[] srcPaths) throws Exception {

        if (srcPaths != null) {
            for (String srcPath : srcPaths) {
                if (hdfsPath.endsWith("/")) {
                    String path = hdfsPath +
                            srcPath.substring(srcPath.lastIndexOf("/"));
                    hdfs.copyFromLocal(srcPath, path);
                } else {
                    hdfs.copyFromLocal(srcPath, hdfsPath);
                }
            }
        }

        String tableName = "file_as_row_" + name;

        ProtocolEnum protocol = ProtocolUtils.getProtocol();
        exTable = new ReadableExternalTable(tableName, fields, protocol.getExternalTablePath(hdfs.getBasePath(), locationPath), "CSV");
        exTable.setProfile(protocol.value() + ":text:multi");
        exTable.setUserParameters(new String[]{"FILE_AS_ROW=true"});
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hcfs.file_as_row." + name + ".runTest");
    }
}
