package org.greenplum.pxf.automation.features.cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.UUID;

/**
 * Functional S3 Select Test
 */
public class S3SelectTest extends BaseFeature {

    private static final String PROTOCOL_S3 = "s3a://";

    private static final String[] PXF_S3_SELECT_COLS = {
            "l_orderkey       BIGINT",
            "l_partkey        BIGINT",
            "l_suppkey        BIGINT",
            "l_linenumber     BIGINT",
            "l_quantity       DECIMAL(15,2)",
            "l_extendedprice  DECIMAL(15,2)",
            "l_discount       DECIMAL(15,2)",
            "l_tax            DECIMAL(15,2)",
            "l_returnflag     CHAR(1)",
            "l_linestatus     CHAR(1)",
            "l_shipdate       DATE",
            "l_commitdate     DATE",
            "l_receiptdate    DATE",
            "l_shipinstruct   CHAR(25)",
            "l_shipmode       CHAR(10)",
            "l_comment        VARCHAR(44)"
    };

    private Hdfs s3Server;
    private String s3Path;

    private static final String sampleCsvFile = "sample.csv";
    private static final String sampleGzippedCsvFile = "sample.csv.gz";
    private static final String sampleBzip2CsvFile = "sample.csv.bz2";
    private static final String sampleCsvNoHeaderFile = "sample-no-header.csv";
    private static final String sampleParquetFile = "sample.parquet";
    private static final String sampleParquetSnappyFile = "sample.snappy.parquet";
    private static final String sampleParquetGzipFile = "sample.gz.parquet";

    /**
     * Prepare all server configurations and components
     */
    @Override
    public void beforeClass() throws Exception {
        // Initialize server objects
        s3Path = String.format("gpdb-ud-scratch/tmp/pxf_automation_data/%s/s3select/", UUID.randomUUID().toString());
        Configuration s3Configuration = new Configuration();
        s3Configuration.set("fs.s3a.access.key", ProtocolUtils.getAccess());
        s3Configuration.set("fs.s3a.secret.key", ProtocolUtils.getSecret());

        FileSystem fs2 = FileSystem.get(URI.create(PROTOCOL_S3 + s3Path + fileName), s3Configuration);
        s3Server = new Hdfs(fs2, s3Configuration, true);
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
        s3Server.removeDirectory(PROTOCOL_S3 + s3Path);
    }

    @Test(groups = {"gpdb", "s3"})
    public void testPlainCsvWithHeaders() throws Exception {
        String[] userParameters = {"FILE_HEADER=IGNORE", "S3-SELECT=ON"};
        runTestScenario("csv", "s3", "csv", s3Path,
                localDataResourcesFolder + "/s3select/", sampleCsvFile,
                "|", userParameters);
    }

    @Test(groups = {"gpdb", "s3"})
    public void testPlainCsvWithHeadersUsingHeaderInfo() throws Exception {
        String[] userParameters = {"FILE_HEADER=USE", "S3-SELECT=ON"};
        runTestScenario("csv_use_headers", "s3", "csv", s3Path,
                localDataResourcesFolder + "/s3select/", sampleCsvFile,
                "|", userParameters);
    }

    @Test(groups = {"gpdb", "s3"})
    public void testPlainCsvWithNoHeaders() throws Exception {
        String[] userParameters = {"FILE_HEADER=NONE", "S3-SELECT=ON"};
        runTestScenario("csv_noheaders", "s3", "csv", s3Path,
                localDataResourcesFolder + "/s3select/", sampleCsvNoHeaderFile,
                "|", userParameters);
    }

    @Test(groups = {"gpdb", "s3"})
    public void testGzipCsvWithHeadersUsingHeaderInfo() throws Exception {
        String[] userParameters = {"FILE_HEADER=USE", "S3-SELECT=ON", "COMPRESSION_CODEC=gzip"};
        runTestScenario("gzip_csv_use_headers", "s3", "csv", s3Path,
                localDataResourcesFolder + "/s3select/", sampleGzippedCsvFile,
                "|", userParameters);
    }

    @Test(groups = {"gpdb", "s3"})
    public void testBzip2CsvWithHeadersUsingHeaderInfo() throws Exception {
        String[] userParameters = {"FILE_HEADER=USE", "S3-SELECT=ON", "COMPRESSION_CODEC=bzip2"};
        runTestScenario("bzip2_csv_use_headers", "s3", "csv", s3Path,
                localDataResourcesFolder + "/s3select/", sampleBzip2CsvFile,
                "|", userParameters);
    }

    @Test(groups = {"gpdb", "s3"})
    public void testParquet() throws Exception {
        String[] userParameters = {"S3-SELECT=ON"};
        runTestScenario("parquet", "s3", "parquet", s3Path,
                localDataResourcesFolder + "/s3select/", sampleParquetFile,
                null, userParameters);
    }

    @Test(groups = {"gpdb", "s3"})
    public void testSnappyParquet() throws Exception {
        String[] userParameters = {"S3-SELECT=ON"};
        runTestScenario("parquet_snappy", "s3", "parquet", s3Path,
                localDataResourcesFolder + "/s3select/", sampleParquetSnappyFile,
                null, userParameters);
    }

    @Test(groups = {"gpdb", "s3"})
    public void testGzipParquet() throws Exception {
        String[] userParameters = {"S3-SELECT=ON"};
        runTestScenario("parquet_gzip", "s3", "parquet", s3Path,
                localDataResourcesFolder + "/s3select/", sampleParquetGzipFile,
                null, userParameters);
    }

    private void runTestScenario(
            String name,
            String server,
            String format,
            String s3Path,
            String srcPath,
            String filename,
            String delimiter,
            String[] userParameters)
            throws Exception {
        String tableName = "s3select_" + name;
        String serverParam = (server == null) ? null : "server=" + server;

        s3Server.copyFromLocal(srcPath + filename, PROTOCOL_S3 + s3Path + filename);

        exTable = new ReadableExternalTable(tableName, PXF_S3_SELECT_COLS,
                "/" + s3Path + filename, "CSV");
        exTable.setProfile("s3:" + format);
        exTable.setServer(serverParam);

        if (delimiter != null)
            exTable.setDelimiter("|");
        if (userParameters != null)
            exTable.setUserParameters(userParameters);

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.s3_select." + name + ".runTest");
    }
}
