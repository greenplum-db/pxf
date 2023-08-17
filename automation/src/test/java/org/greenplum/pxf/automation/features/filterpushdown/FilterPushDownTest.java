package org.greenplum.pxf.automation.features.filterpushdown;

import annotations.SkipForFDW;
import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Functional PXF filter pushdown cases
 */
@WorksWithFDW
public class FilterPushDownTest extends BaseFeature {

    private static final String COMMA = ",";

    private static final String[] FIELDS = new String[]{
            "t0    text",
            "a1    integer",
            "b2    boolean",
            "c3    numeric",
            "d4    char(3)",
            "e5    varchar(2)",
            "filterValue  text"
    };

    /**
     * Check that PXF receives the expected filter string, using a table with a comma delimiter
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void checkFilterPushDown() throws Exception {
        preparePxfTable(COMMA);
        runTincTest("pxf.features.filterpushdown.checkFilterPushDown.runTest");
    }

    /**
     * Check that PXF receives no filter string, using a table with a comma delimiter
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    @SkipForFDW // the guc used in the test is not applicable to FDW and has no effect
    public void checkFilterPushDownDisabled() throws Exception {
        preparePxfTable(COMMA);
        runTincTest("pxf.features.filterpushdown.checkFilterPushDownDisabled.runTest");
    }

    /**
     * Check that PXF receives the expected filter string, using a table with a hexademical delimiter
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void checkFilterStringHexDelimiter() throws Exception {
        preparePxfTable("E'\\x01'");
        runTincTest("pxf.features.filterpushdown.checkFilterPushDownHexDelimiter.runTest");
    }

    /**
     * Prepares a PXF external text table with a given delimiter.
     * @param delimiter delimiter
     * @throws Exception
     */
    private void preparePxfTable(String delimiter) throws Exception {
        // Create PXF external table for filter testing
        exTable = TableFactory.getPxfReadableTestTextTable("test_filter", FIELDS, "dummy_path", delimiter);
        exTable.setProfile("system:filter"); // use system:filter profile shipped with PXF server
        gpdb.createTableAndVerify(exTable);
    }
}
