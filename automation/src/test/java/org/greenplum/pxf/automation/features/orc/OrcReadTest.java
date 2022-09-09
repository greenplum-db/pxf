package org.greenplum.pxf.automation.features.orc;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

public class OrcReadTest extends BaseFeature {

    private static final String ORC_PRIMITIVE_TYPES = "orc_types.orc";
    private static final String PXF_ORC_TABLE = "pxf_orc_primitive_types";
    private static final String ORC_PRIMITIVE_TYPES_UNORDERED_SUBSET = "orc_types_unordered_subset.orc";
    private static final String ORC_LIST_TYPES = "orc_list_types.orc";
    private static final String ORC_MULTIDIM_LIST_TYPES = "orc_multidim_list_types.orc";

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

    private static final String[] ORC_LIST_TYPES_TABLE_COLUMNS = new String[]{
            "id           integer",
            "bool_arr     boolean[]",
            "int2_arr     smallint[]",
            "int_arr      int[]",
            "int8_arr     bigint[]",
            "float_arr    real[]",
            "float8_arr   float[]",
            "text_arr     text[]",
            "bytea_arr    bytea[]",
            "char_arr     bpchar(15)[]",
            "varchar_arr  varchar(15)[]"
    };

    // char arrays and varchar arrays should also be allowed as text arrays
    private static final String[] ORC_LIST_TYPES_TABLE_COLUMNS_TEXT = new String[]{
            "id           integer",
            "bool_arr     boolean[]",
            "int2_arr     smallint[]",
            "int_arr      int[]",
            "int8_arr     bigint[]",
            "float_arr    real[]",
            "float8_arr   float[]",
            "text_arr     text[]",
            "bytea_arr    bytea[]",
            "char_arr     text[]",
            "varchar_arr  text[]"
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
        hdfs.copyFromLocal(resourcePath + ORC_LIST_TYPES, hdfsPath + ORC_LIST_TYPES);
        hdfs.copyFromLocal(resourcePath + ORC_MULTIDIM_LIST_TYPES, hdfsPath + ORC_MULTIDIM_LIST_TYPES);

        prepareReadableExternalTable(PXF_ORC_TABLE, ORC_TABLE_COLUMNS, hdfsPath + ORC_PRIMITIVE_TYPES);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadPrimitives() throws Exception {
        runTincTest("pxf.features.orc.read.primitive_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadPrimitivesMapByPosition() throws Exception {
        prepareReadableExternalTable(PXF_ORC_TABLE, ORC_TABLE_COLUMNS,
                hdfsPath + ORC_PRIMITIVE_TYPES, true);
        runTincTest("pxf.features.orc.read.primitive_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadPrimitivesWithUnorderedSubsetFile() throws Exception {
        prepareReadableExternalTable("pxf_orc_primitive_types_with_subset",
                ORC_TABLE_COLUMNS, hdfsPath + "orc_types*.orc");
        runTincTest("pxf.features.orc.read.primitive_types_with_subset.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadSubset() throws Exception {
        prepareReadableExternalTable("pxf_orc_primitive_types_subset",
                ORC_TABLE_COLUMNS_SUBSET, hdfsPath + ORC_PRIMITIVE_TYPES);
        runTincTest("pxf.features.orc.read.read_subset.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcPredicatePushDown() throws Exception {
        runTincTest("pxf.features.orc.read.pushdown.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcPredicatePushDownMapByPosition() throws Exception {
        prepareReadableExternalTable(PXF_ORC_TABLE, ORC_TABLE_COLUMNS, hdfsPath + ORC_PRIMITIVE_TYPES, true);
        runTincTest("pxf.features.orc.read.pushdown.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadLists() throws Exception {
        prepareReadableExternalTable("pxf_orc_list_types", ORC_LIST_TYPES_TABLE_COLUMNS, hdfsPath + ORC_LIST_TYPES);
        runTincTest("pxf.features.orc.read.list_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadBpCharAndVarCharListsAsTextArr() throws Exception {
        prepareReadableExternalTable("pxf_orc_bpchar_varchar_list_types_as_textarr", ORC_LIST_TYPES_TABLE_COLUMNS_TEXT, hdfsPath + ORC_LIST_TYPES);
        runTincTest("pxf.features.orc.read.bpchar_varchar_list_types_as_textarr.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadMultiDimensionalLists() throws Exception {
        prepareReadableExternalTable("pxf_orc_multidim_list_types", ORC_LIST_TYPES_TABLE_COLUMNS, hdfsPath + ORC_MULTIDIM_LIST_TYPES);
        runTincTest("pxf.features.orc.read.multidim_list_types.runTest");
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path) throws Exception {
        prepareReadableExternalTable(name, fields, path, false);
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path, boolean mapByPosition) throws Exception {
        exTable = TableFactory.getPxfOrcReadableTable(name, fields, path, hdfs.getBasePath());
        if (mapByPosition) {
            exTable.setUserParameters(new String[]{"MAP_BY_POSITION=true"});
        }
        createTable(exTable);
    }
}
