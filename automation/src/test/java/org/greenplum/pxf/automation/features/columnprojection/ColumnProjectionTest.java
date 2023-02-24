package org.greenplum.pxf.automation.features.columnprojection;

import annotations.FailsWithFDW;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.io.File;

/** Functional PXF column projection cases */
@FailsWithFDW
public class ColumnProjectionTest extends BaseFeature {

    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String testPackage = "org.greenplum.pxf.automation.testplugin.";

    @Override
    protected void beforeClass() throws Exception {
        String newPath = "/tmp/publicstage/pxf";
        // copy additional plugins classes to cluster nodes, used for filter pushdown cases
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "ColumnProjectionVerifyFragmentMetadata.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "ColumnProjectionVerifyFragmenter.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "ColumnProjectionVerifyAccessor.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    /**
     * Check PXF receive the expected column projection string from GPDB.
     * Column delimiter is ",".
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void checkColumnProjection() throws Exception {

        // Create PXF external table for column projection testing
        ReadableExternalTable pxfExternalTable = TableFactory.getPxfReadableTestTextTable("test_column_projection", new String[] {
                "t0    text",
                "a1    integer",
                "b2    boolean",
                "colprojValue  text"
        }, "dummy_path",",");

        pxfExternalTable.setFragmenter(testPackage + "ColumnProjectionVerifyFragmenter");
        pxfExternalTable.setAccessor(testPackage + "ColumnProjectionVerifyAccessor");
        pxfExternalTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        gpdb.createTableAndVerify(pxfExternalTable);

        // TODO: revert when 2 queries with GP7 planner start propagating projection info to foreign scans
        // SELECT t0, colprojvalue FROM test_column_projection GROUP BY t0, colprojvalue HAVING AVG(a1) < 5 ORDER BY t0;
        // SELECT b.value, a.colprojvalue FROM test_column_projection a JOIN t0_values b ON a.t0 = b.key;
        if (gpdb.getVersion() >= 7) {
            runTincTest("pxf.features.columnprojection.checkColumnProjection_gp7.runTest");
        } else {
            runTincTest("pxf.features.columnprojection.checkColumnProjection.runTest");
        }
    }
}
