package org.greenplum.pxf.automation.features.orc;

import annotations.FailsWithFDW;
import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

@WorksWithFDW
public class OrcReadTest extends BaseFeature {

    private static final String ORC_PRIMITIVE_TYPES = "orc_types.orc";
    private static final String PXF_ORC_TABLE = "pxf_orc_primitive_types";
    private static final String ORC_PRIMITIVE_TYPES_UNORDERED_SUBSET = "orc_types_unordered_subset.orc";
    private static final String ORC_LIST_TYPES = "orc_list_types.orc";
    private static final String ORC_MULTIDIM_LIST_TYPES = "orc_multidim_list_types.orc";
    private static final String ORC_NULL_IN_STRING = "orc_null_in_string.orc";

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

    private static final String[] ORC_NULL_IN_STRING_COLUMNS = new String[]{
            "id      int",
            "context text",
            "value   text"
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
        hdfs.copyFromLocal(resourcePath + ORC_NULL_IN_STRING, hdfsPath + ORC_NULL_IN_STRING);

        prepareReadableExternalTable(PXF_ORC_TABLE, ORC_TABLE_COLUMNS, hdfsPath + ORC_PRIMITIVE_TYPES);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadPrimitives() throws Exception {
        runSqlTest("features/orc/read/primitive_types");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadPrimitivesMapByPosition() throws Exception {
        prepareReadableExternalTable(PXF_ORC_TABLE, ORC_TABLE_COLUMNS,
                hdfsPath + ORC_PRIMITIVE_TYPES, true);
        runSqlTest("features/orc/read/primitive_types");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadPrimitivesWithUnorderedSubsetFile() throws Exception {
        prepareReadableExternalTable("pxf_orc_primitive_types_with_subset",
                ORC_TABLE_COLUMNS, hdfsPath + "orc_types*.orc");
        runSqlTest("features/orc/read/primitive_types_with_subset");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadSubset() throws Exception {
        prepareReadableExternalTable("pxf_orc_primitive_types_subset",
                ORC_TABLE_COLUMNS_SUBSET, hdfsPath + ORC_PRIMITIVE_TYPES);
        runSqlTest("features/orc/read/read_subset");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcPredicatePushDown() throws Exception {
        runSqlTest("features/orc/read/pushdown");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcPredicatePushDownMapByPosition() throws Exception {
        prepareReadableExternalTable(PXF_ORC_TABLE, ORC_TABLE_COLUMNS, hdfsPath + ORC_PRIMITIVE_TYPES, true);
        runSqlTest("features/orc/read/pushdown");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadLists() throws Exception {
        prepareReadableExternalTable("pxf_orc_list_types", ORC_LIST_TYPES_TABLE_COLUMNS, hdfsPath + ORC_LIST_TYPES);
        runSqlTest("features/orc/read/list_types");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadBpCharAndVarCharListsAsTextArr() throws Exception {
        prepareReadableExternalTable("pxf_orc_bpchar_varchar_list_types_as_textarr", ORC_LIST_TYPES_TABLE_COLUMNS_TEXT, hdfsPath + ORC_LIST_TYPES);
        runSqlTest("features/orc/read/bpchar_varchar_list_types_as_textarr");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadMultiDimensionalLists() throws Exception {
        prepareReadableExternalTable("pxf_orc_multidim_list_types", ORC_LIST_TYPES_TABLE_COLUMNS, hdfsPath + ORC_MULTIDIM_LIST_TYPES);
        runSqlTest("features/orc/read/multidim_list_types");
    }

    /*
     * FDW fails for the data that contain a NUL-byte (i.e. '\/u000'"). This behaviour is different from external-table but same as GPDB Heap
     * FDW Failure: invalid byte sequence for encoding "UTF8": 0x00
     *
     * GPDB also throws the same error when copying the data containing a NUL-byte
     *
     * postgres=# copy test from '/Users/pandeyhi/Documents/bad_data.txt' ;
     * ERROR:  invalid byte sequence for encoding "UTF8": 0x00
     * TODO Do we need to do some changes to make sure the external-table behaves the same way as GPDB/FDW?
     *
     */
    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcReadStringsContainingNullByte() throws Exception {
        prepareReadableExternalTable("pxf_orc_null_in_string", ORC_NULL_IN_STRING_COLUMNS, hdfsPath + ORC_NULL_IN_STRING);
        runSqlTest("features/orc/read/null_in_string");
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path) throws Exception {
        prepareReadableExternalTable(name, fields, path, false);
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path, boolean mapByPosition) throws Exception {
        exTable = TableFactory.getPxfHcfsReadableTable(name, fields, path, hdfs.getBasePath(), "orc");
        if (mapByPosition) {
            exTable.setUserParameters(new String[]{"MAP_BY_POSITION=true"});
        }
        createTable(exTable);
    }
}
