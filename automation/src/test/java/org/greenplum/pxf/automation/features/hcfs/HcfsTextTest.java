package org.greenplum.pxf.automation.features.hcfs;

import annotations.FailsWithFDW;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

/**
 * Functional Text Files Test in HCFS
 */
@FailsWithFDW
public class HcfsTextTest extends BaseFeature {

    private static final String[] PXF_SINGLE_COL = {"text_blob text"};
    private static final String[] PXF_THREE_COLS = {"a text", "b text", "c text"};

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testDelimiterOff() throws Exception {
        String hdfsPath = hdfs.getWorkingDirectory() + "/hcfs-text/no-delimiter";
        String srcPath = localDataResourcesFolder + "/text/no-delimiter";

        runTestScenario("delimiter_off", PXF_SINGLE_COL, srcPath, hdfsPath, "OFF", null);
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testEscapeOff() throws Exception {
        String hdfsPath = hdfs.getWorkingDirectory() + "/hcfs-text/no-escape";
        String srcPath = localDataResourcesFolder + "/text/no-escape";

        runTestScenario("escape_off", PXF_THREE_COLS, srcPath, hdfsPath, null, "OFF");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testDelimiterOffAndEscapeOff() throws Exception {
        String hdfsPath = hdfs.getWorkingDirectory() + "/hcfs-text/no-delimiter-or-escape";
        String srcPath = localDataResourcesFolder + "/text/no-delimiter-or-escape";

        runTestScenario("delimiter_off_escape_off", PXF_SINGLE_COL, srcPath, hdfsPath, "OFF", "OFF");
    }

    private void runTestScenario(String name, String[] fields, String srcPath, String hdfsPath, String delimiter, String escape) throws Exception {
        hdfs.copyFromLocal(srcPath, hdfsPath);

        ProtocolEnum protocol = ProtocolUtils.getProtocol();
        String tableName = "hcfs_text_" + name;
        exTable = TableFactory.getPxfReadableTextTable(tableName, fields, protocol.getExternalTablePath(hdfs.getBasePath(), hdfsPath), delimiter);
        exTable.setProfile(protocol.value() + ":text");

        if (delimiter != null) {
            exTable.setDelimiter(delimiter);
        }

        if (escape != null) {
            exTable.setEscape(escape);
        }

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hcfs.text." + name + ".runTest");
    }
}
