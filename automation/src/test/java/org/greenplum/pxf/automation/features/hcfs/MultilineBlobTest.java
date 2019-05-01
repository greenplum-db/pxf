package org.greenplum.pxf.automation.features.hcfs;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

/**
 * Functional Multiline Blob Test
 */
public class MultilineBlobTest extends BaseFeature {

    private static final String multiLineJsonFile = "tweets-pp.json";

    @Override
    protected void beforeMethod() throws Exception {
        super.beforeMethod();
        prepareData();
    }

    protected void prepareData() throws Exception {
        // Create local Large file and copy to HDFS
        String textFilePath = hdfs.getWorkingDirectory() + "/" + multiLineJsonFile;
        String localDataFile = localDataResourcesFolder + "/json/" + multiLineJsonFile;

        hdfs.copyFromLocal(localDataFile, textFilePath);
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testMultilineTextBlob() throws Exception {

        String tableName = "multiline_blob_text";
        exTable = new ReadableExternalTable(tableName,
                new String[]{
                        "text_blob text"
                },
                hdfs.getWorkingDirectory() + "/" + multiLineJsonFile,
                "CSV");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setUserParameters(new String[]{"BLOB=true"});
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hcfs.multiline_blob.text.runTest");
    }

    @Test(groups = {"gpdb", "hcfs"})
    public void testMultilineJsonBlob() throws Exception {

        String tableName = "multiline_blob_json";
        exTable = new ReadableExternalTable(tableName,
                new String[]{
                        "json_blob json"
                },
                hdfs.getWorkingDirectory() + "/" + multiLineJsonFile,
                "CSV");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setUserParameters(new String[]{"BLOB=true"});
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hcfs.multiline_blob.json.runTest");
    }
}
