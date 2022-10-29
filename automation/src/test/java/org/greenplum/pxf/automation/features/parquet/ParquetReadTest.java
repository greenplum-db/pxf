package org.greenplum.pxf.automation.features.parquet;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.io.File;

public class ParquetReadTest extends BaseFeature {

    private String hdfsPath;

    private static final String NUMERIC_TABLE = "numeric_precision";
    private static final String NUMERIC_UNDEFINED_PRECISION_TABLE = "numeric_undefined_precision";
    private static final String PXF_PARQUET_TABLE = "pxf_parquet_primitive_types";
    private static final String PARQUET_PRIMITIVE_TYPES = "parquet_primitive_types";
    private static final String PARQUET_TYPES = "parquet_types.parquet";
    private static final String PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE = "undefined_precision_numeric.parquet";
    private static final String PARQUET_NUMERIC_FILE = "numeric.parquet";
    private static final String UNDEFINED_PRECISION_NUMERIC_FILENAME = "undefined_precision_numeric.csv";
    private static final String NUMERIC_FILENAME = "numeric_with_precision.csv";
    private static final String PARQUET_LIST_FILE = "parquet_list_types.parquet";
    private static final String PARQUET_TIMESTAMP_LIST_TYPE_FILE="parquet_timestamp_list_type.parquet";

    private static final String[] PARQUET_TABLE_COLUMNS = new String[]{
            "s1    TEXT",
            "s2    TEXT",
            "n1    INTEGER",
            "d1    DOUBLE PRECISION",
            "dc1   NUMERIC",
            "tm    TIMESTAMP",
            "f     REAL",
            "bg    BIGINT",
            "b     BOOLEAN",
            "tn    SMALLINT",
            "vc1   VARCHAR(5)",
            "sml   SMALLINT",
            "c1    CHAR(3)",
            "bin   BYTEA"
    };

    private static final String[] PARQUET_TYPES_COLUMNS = new String[]{
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

    private static final String[] PARQUET_TABLE_DECIMAL_COLUMNS = new String[]{
            "description   TEXT",
            "a             DECIMAL(5,  2)",
            "b             DECIMAL(12, 2)",
            "c             DECIMAL(18, 18)",
            "d             DECIMAL(24, 16)",
            "e             DECIMAL(30, 5)",
            "f             DECIMAL(34, 30)",
            "g             DECIMAL(38, 10)",
            "h             DECIMAL(38, 38)"
    };

    private static final String[] UNDEFINED_PRECISION_NUMERIC = new String[]{
            "description   text",
            "value         numeric"};

    private static final String[] PARQUET_TABLE_COLUMNS_SUBSET = new String[]{
            "s1    TEXT",
            "n1    INTEGER",
            "d1    DOUBLE PRECISION",
            "f     REAL",
            "b     BOOLEAN",
            "vc1   VARCHAR(5)",
            "bin   BYTEA"
    };

    // parquet LIST data are generated using HIVE, but HIVE doesn't support TIMESTAMP for parquet LIST
    private static final String[] PARQUET_LIST_TABLE_COLUMNS = {
            "id                   INTEGER"      ,
            "bool_arr             BOOLEAN[]"    , // DataType.BOOLARRAY
            "smallint_arr         SMALLINT[]"   , // DataType.INT2ARRAY
            "int_arr              INTEGER[]"    , // DataType.INT4ARRAY
            "bigint_arr           BIGINT[]"     , // DataType.INT8ARRAY
            "real_arr             REAL[]"       , // DataType.FLOAT4ARRAY
            "double_arr           FLOAT[]"      , // DataType.FLOAT8ARRAY
            "text_arr             TEXT[]"       , // DataType.TEXTARRAY
            "bytea_arr            BYTEA[]"      , // DataType.BYTEAARRAY
            "char_arr             CHAR(15)[]"    , // DataType.BPCHARARRAY
            "varchar_arr          VARCHAR(15)[]" , // DataType.VARCHARARRAY
            "date_arr             DATE[]"       , // DataType.DATEARRAY
            "numeric_arr          NUMERIC[]"    , // DataType.NUMERICARRAY
//            "varchar_arr_nolimit  VARCHAR[]"    , // DataType.VARCHARARRAY with no length limit
    };

    // parquet TIMESTAMP LIST data generated using Spark
    private static final String[] PARQUET_TIMESTAMP_LIST_TABLE_COLUMNS = {
            "id            INTEGER"      ,
            "tm_arr        TIMESTAMP[]"  , // DataType.TIMESTAMPARRAY
    };

    private ProtocolEnum protocol;


    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/parquet/";
        protocol = ProtocolUtils.getProtocol();

        String resourcePath = localDataResourcesFolder + "/parquet/";
        hdfs.copyFromLocal(resourcePath + PARQUET_PRIMITIVE_TYPES, hdfsPath + PARQUET_PRIMITIVE_TYPES);
        hdfs.copyFromLocal(resourcePath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE, hdfsPath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE);
        hdfs.copyFromLocal(resourcePath + PARQUET_NUMERIC_FILE, hdfsPath + PARQUET_NUMERIC_FILE);
        hdfs.copyFromLocal(resourcePath + PARQUET_TYPES, hdfsPath + PARQUET_TYPES);
        hdfs.copyFromLocal(resourcePath + PARQUET_LIST_FILE, hdfsPath+PARQUET_LIST_FILE);
        hdfs.copyFromLocal(resourcePath+PARQUET_TIMESTAMP_LIST_TYPE_FILE, hdfsPath+PARQUET_TIMESTAMP_LIST_TYPE_FILE);

        Table gpdbUndefinedPrecisionNumericTable = new Table(NUMERIC_UNDEFINED_PRECISION_TABLE, UNDEFINED_PRECISION_NUMERIC);
        gpdbUndefinedPrecisionNumericTable.setDistributionFields(new String[]{"description"});
        gpdb.createTableAndVerify(gpdbUndefinedPrecisionNumericTable);
        gpdb.copyFromFile(gpdbUndefinedPrecisionNumericTable, new File(localDataResourcesFolder
                + "/numeric/" + UNDEFINED_PRECISION_NUMERIC_FILENAME), "E','", true);

        Table gpdbNumericWithPrecisionScaleTable = new Table(NUMERIC_TABLE, PARQUET_TABLE_DECIMAL_COLUMNS);
        gpdbNumericWithPrecisionScaleTable.setDistributionFields(new String[]{"description"});
        gpdb.createTableAndVerify(gpdbNumericWithPrecisionScaleTable);
        gpdb.copyFromFile(gpdbNumericWithPrecisionScaleTable, new File(localDataResourcesFolder
                + "/numeric/" + NUMERIC_FILENAME), "E','", true);

        prepareReadableExternalTable(PXF_PARQUET_TABLE, PARQUET_TABLE_COLUMNS, hdfsPath + PARQUET_PRIMITIVE_TYPES);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadPrimitives() throws Exception {
        gpdb.runQuery("CREATE OR REPLACE VIEW parquet_view AS SELECT s1, s2, n1, d1, dc1, " +
                "CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm, " +
                "f, bg, b, tn, sml, vc1, c1, bin FROM " + PXF_PARQUET_TABLE);

        runTincTest("pxf.features.parquet.primitive_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadSubset() throws Exception {
        prepareReadableExternalTable("pxf_parquet_subset",
                PARQUET_TABLE_COLUMNS_SUBSET, hdfsPath + PARQUET_PRIMITIVE_TYPES);
        runTincTest("pxf.features.parquet.read_subset.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadUndefinedPrecisionNumericFromAParquetFileGeneratedByHive() throws Exception {
        // Make sure that data generated by Hive is the same as data
        // written by PXF to Parquet for the same dataset
        prepareReadableExternalTable("pxf_parquet_read_undefined_precision_numeric",
                UNDEFINED_PRECISION_NUMERIC, hdfsPath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE);
        runTincTest("pxf.features.parquet.decimal.numeric_undefined_precision.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadNumericWithPrecisionAndScaleFromAParquetFileGeneratedByHive() throws Exception {
        prepareReadableExternalTable("pxf_parquet_read_numeric",
                PARQUET_TABLE_DECIMAL_COLUMNS, hdfsPath + PARQUET_NUMERIC_FILE);
        runTincTest("pxf.features.parquet.decimal.numeric.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetPredicatePushDown() throws Exception {
        prepareReadableExternalTable("parquet_types_hcfs_r", PARQUET_TYPES_COLUMNS, hdfsPath + PARQUET_TYPES);
        runTincTest("pxf.features.parquet.pushdown.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadListGeneratedByHive() throws Exception {
        prepareReadableExternalTable("pxf_parquet_list_types", PARQUET_LIST_TABLE_COLUMNS, hdfsPath + PARQUET_LIST_FILE);
        runTincTest("pxf.features.parquet.list.runTest");
    }
    private void prepareReadableExternalTable(String name, String[] fields, String path) throws Exception {
        exTable = TableFactory.getPxfHcfsReadableTable(name, fields, path, hdfs.getBasePath(), "parquet");
        createTable(exTable);
    }

}
