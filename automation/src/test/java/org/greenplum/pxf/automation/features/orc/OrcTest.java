package org.greenplum.pxf.automation.features.orc;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

public class OrcTest extends BaseFeature {

    private static final String ORC_PRIMITIVE_TYPES = "orc_types.orc";
    private static final String PXF_ORC_TABLE = "pxf_orc_primitive_types";
    private static final String ORC_PRIMITIVE_TYPES_UNORDERED_SUBSET = "orc_types_unordered_subset.orc";

    private static final String[] ORC_TABLE_COLUMNS = {
            "id      integer",
            "name    text",
            "cdate   date",
            "amt     double precision",
            "grade   text",
            "b       boolean",
            "tm      timestamp without time zone",
            "bg      bigint",
            "bin     bytea",
            "sml     smallint",
            "r       real",
            "vc1     character varying(5)",
            "c1      character(3)",
            "dec1    numeric",
            "dec2    numeric(5,2)",
            "dec3    numeric(13,5)",
            "num1    integer"
    };

    private static final String[] ORC_TABLE_COLUMNS_SUBSET = new String[]{
            "name    TEXT",
            "num1    INTEGER",
            "amt     DOUBLE PRECISION",
            "r       REAL",
            "b       BOOLEAN",
            "vc1     VARCHAR(5)",
            "bin     BYTEA"
    };

    private String hdfsPath;
    private ProtocolEnum protocol;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/orc/";
        protocol = ProtocolUtils.getProtocol();

        String resourcePath = localDataResourcesFolder + "/orc/";
        hdfs.copyFromLocal(resourcePath + ORC_PRIMITIVE_TYPES, hdfsPath + ORC_PRIMITIVE_TYPES);
        hdfs.copyFromLocal(resourcePath + ORC_PRIMITIVE_TYPES_UNORDERED_SUBSET, hdfsPath + ORC_PRIMITIVE_TYPES_UNORDERED_SUBSET);

        prepareReadableExternalTable(PXF_ORC_TABLE, ORC_TABLE_COLUMNS, hdfsPath + ORC_PRIMITIVE_TYPES);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadPrimitives() throws Exception {
        runTincTest("pxf.features.orc.primitive_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadPrimitivesMapByPosition() throws Exception {
        prepareReadableExternalTable(PXF_ORC_TABLE, ORC_TABLE_COLUMNS,
                hdfsPath + ORC_PRIMITIVE_TYPES, true);
        runTincTest("pxf.features.orc.primitive_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadPrimitivesWithUnorderedSubsetFile() throws Exception {
        prepareReadableExternalTable("pxf_orc_primitive_types_with_subset",
                ORC_TABLE_COLUMNS, hdfsPath + "orc_types*.orc");
        runTincTest("pxf.features.orc.primitive_types_with_subset.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadSubset() throws Exception {
        prepareReadableExternalTable("pxf_orc_primitive_types_subset",
                ORC_TABLE_COLUMNS_SUBSET, hdfsPath + ORC_PRIMITIVE_TYPES);
        runTincTest("pxf.features.orc.read_subset.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcPredicatePushDown() throws Exception {
        runTincTest("pxf.features.orc.pushdown.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcPredicatePushDownMapByPosition() throws Exception {
        prepareReadableExternalTable(PXF_ORC_TABLE, ORC_TABLE_COLUMNS, hdfsPath + ORC_PRIMITIVE_TYPES, true);
        runTincTest("pxf.features.orc.pushdown.runTest");
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path) throws Exception {
        prepareReadableExternalTable(name, fields, path, false);
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path, boolean mapByPosition) throws Exception {
        exTable = new ReadableExternalTable(name, fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(protocol.value() + ":orc");

        if (mapByPosition) {
            exTable.setUserParameters(new String[]{"MAP_BY_POSITION=true"});
        }

        createTable(exTable);
    }
}
