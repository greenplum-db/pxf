package org.greenplum.pxf.automation.features.parquet;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

public class ParquetTest extends BaseFeature {

    private String hdfsPath;
    private String resourcePath;

    private final String pxfParquetTable = "pxf_parquet_primitive_types";
    private final String parquetWritePrimitives = "parquet_write_primitives";
    private final String parquetPrimitiveTypes = "parquet_primitive_types";
    private final String[] parquet_table_columns = new String[] {
            "t1    TEXT",
            "t2    TEXT",
            "num1  INTEGER",
            "dub1  DOUBLE PRECISION",
            "dec1  NUMERIC",
            "tm    TIMESTAMP",
            "r     REAL",
            "bg    BIGINT",
            "b     BOOLEAN",
            "tn    SMALLINT",
            "sml   SMALLINT",
            "vc1   VARCHAR(5)",
            "c1    CHAR(3)",
            "bin   BYTEA"
    };

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/parquet/";

        resourcePath = localDataResourcesFolder + "/parquet/";
        hdfs.copyFromLocal(resourcePath + parquetPrimitiveTypes, hdfsPath + parquetPrimitiveTypes);
    }


    @Test(groups = {"features", "gpdb", "hcfs"})
    public void parquetReadPrimitives() throws Exception {

        exTable = new ReadableExternalTable(pxfParquetTable,
                parquet_table_columns, hdfsPath + parquetPrimitiveTypes, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);
        gpdb.runQuery("CREATE OR REPLACE VIEW parquet_view AS SELECT t1, t2, num1, dub1, dec1, " +
                "CAST (((CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT') AT TIME ZONE " +
                "current_setting('TIMEZONE')) AS TIMESTAMP WITHOUT TIME ZONE) as tm, " +
                "r, bg, b, tn, sml, vc1, c1, bin FROM " + pxfParquetTable);

        runTincTest("pxf.features.parquet.primitive_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs"})
    public void parquetWritePrimitives() throws Exception {

        exTable = new WritableExternalTable("pxf_parquet_write_primitives",
                parquet_table_columns, hdfsPath + parquetWritePrimitives, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT t1, t2, num1, dub1, dec1, " +
                "tm, r, bg, b, tn, sml, vc1, c1, bin FROM " + pxfParquetTable);

        exTable = new ReadableExternalTable("pxf_parquet_read_primitives",
                parquet_table_columns, hdfsPath + parquetWritePrimitives, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");
        gpdb.createTableAndVerify(exTable);
        gpdb.runQuery("CREATE OR REPLACE VIEW parquet_view AS SELECT t1, t2, num1, dub1, dec1, " +
                "CAST (((CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT') AT TIME ZONE " +
                "current_setting('TIMEZONE')) AS TIMESTAMP WITHOUT TIME ZONE) as tm, " +
                "r, bg, b, tn, sml, vc1, c1, bin FROM pxf_parquet_read_primitives");

        runTincTest("pxf.features.parquet.primitive_types.runTest");
    }
}
