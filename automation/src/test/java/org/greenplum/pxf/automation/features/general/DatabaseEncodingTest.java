package org.greenplum.pxf.automation.features.general;

import org.greenplum.pxf.automation.components.gpdb.Gpdb;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.util.List;

import static java.lang.Thread.sleep;

/**
 * Tests non-UTF8 database encodings
 **/
public class DatabaseEncodingTest extends BaseFeature {

    private static final String[] PARQUET_TYPES_COLUMNS = new String[]{
            "id      integer",
            "name    text",
            "cdate   date",
            "amt     double precision",
            "grade   text",
            "b       boolean",
            "tm      timestamp without time zone",
            "bg      bigint",
            "sml     smallint",
            "r       real",
            "vc1     character varying(5)",
            "c1      character(3)",
            "dec1    numeric",
            "dec2    numeric(5,2)",
            "dec3    numeric(13,5)",
            "num1    integer"
    };

    private static final String FIRST_ROW = "1, 'однако', '2019-12-01', 1200, 'хорошо', false, '2013-07-13 21:00:00', 2147483647, -32768, 7.7, 's_6', 'руб', 1.234560000000000000, 0.00, 0.12345, 1";
    private static final String SECOND_ROW = "2, 'теперь', '2019-12-02', 1300, 'отлично', true, '2013-07-13 21:00:00', 2147483648, -31500, 8.7, 's_7', 'руб', 1.234560000000000000, 123.45, -0.12345, 1";

    private String hdfsPath;
    private ProtocolEnum protocol;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/database-encoding/";
        protocol = ProtocolUtils.getProtocol();

        String extensionName = FDWUtils.useFDW ? "pxf_fdw" : "pxf";
        nonUtf8Gpdb.runQuery("CREATE EXTENSION IF NOT EXISTS " + extensionName, true, false);
    }

    /**
     * Write and read a parquet table from a UTF-8 encoded database.
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void writeReadUTF8() throws Exception {

        String filename = "write_read_utf8";
        prepareWritableExternalTable(gpdb, "db_encoding_write_utf8", hdfsPath + filename);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " VALUES (" + FIRST_ROW + "), (" + SECOND_ROW + ")");
        waitForFiles(filename);
        prepareReadableExternalTable(gpdb, "db_encoding_read_utf8", hdfsPath + filename);

        runSqlTest("features/general/databaseEncoding/readUTF8");
    }

    /**
     * Write and read a parquet table from a database with non-UTF8 encoding
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void writeReadOtherEncoding() throws Exception {

        String filename = "write_read_other_encoding";
        prepareWritableExternalTable(nonUtf8Gpdb, "db_encoding_write_other", hdfsPath + filename);
        nonUtf8Gpdb.runQuery("INSERT INTO " + exTable.getName() + " VALUES (" + FIRST_ROW + "), (" + SECOND_ROW + ")");
        waitForFiles(filename);
        prepareReadableExternalTable(nonUtf8Gpdb, "db_encoding_read_other", hdfsPath + filename);

        runSqlTest("features/general/databaseEncoding/readOtherEncoding");
    }

    /**
     * Write a parquet table from a UTF-8 encoded database, and read it
     * from a database with different encoding.
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void writeUTF8ReadOtherEncoding() throws Exception {

        String filename = "write_utf8_read_other_encoding";
        prepareWritableExternalTable(gpdb, "db_encoding_write_utf8", hdfsPath + filename);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " VALUES (" + FIRST_ROW + "), (" + SECOND_ROW + ")");
        waitForFiles(filename);
        prepareReadableExternalTable(nonUtf8Gpdb, "db_encoding_read_other", hdfsPath + filename);

        runSqlTest("features/general/databaseEncoding/readOtherEncoding");
    }

    @Test(groups = {"features", "gpdb", "security"})
    public void writeOtherEncodingReadUTF8() throws Exception {

        String filename = "write_other_encoding_read_utf8";
        prepareWritableExternalTable(nonUtf8Gpdb, "db_encoding_write_other", hdfsPath + filename);
        nonUtf8Gpdb.runQuery("INSERT INTO " + exTable.getName() + " VALUES (" + FIRST_ROW + "), (" + SECOND_ROW + ")");
        waitForFiles(filename);
        prepareReadableExternalTable(gpdb, "db_encoding_read_utf8", hdfsPath + filename);

        runSqlTest("features/general/databaseEncoding/readUTF8");
    }

    private void prepareReadableExternalTable(Gpdb database, String name, String path) throws Exception {
        exTable = new ReadableExternalTable(name, PARQUET_TYPES_COLUMNS,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(protocol.value() + ":parquet");
        exTable.setEncoding("UTF8");
        createTable(database, exTable);
    }

    private void prepareWritableExternalTable(Gpdb database, String name, String path) throws Exception {
        exTable = new WritableExternalTable(name, PARQUET_TYPES_COLUMNS,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(protocol.value() + ":parquet");
        exTable.setEncoding("UTF8");
        createTable(database, exTable);
    }

    private void createTable(Gpdb database, ReadableExternalTable gpdbExternalTable) throws Exception {
        gpdbExternalTable.setHost(pxfHost);
        gpdbExternalTable.setPort(pxfPort);
        database.createTableAndVerify(gpdbExternalTable);
    }

    private void waitForFiles(String filename) throws Exception {
        if (protocol != ProtocolEnum.HDFS && protocol != ProtocolEnum.FILE) {
            // for HCFS on Cloud, wait a bit for async write in previous steps to finish
            sleep(10000);
            List<String> files = hdfs.list(hdfsPath + filename);
            for (String file : files) {
                // make sure the file is available, saw flakes on Cloud that listed files were not available
                int attempts = 0;
                while (!hdfs.doesFileExist(file) && attempts++ < 20) {
                    sleep(1000);
                }
            }
        }
    }
}
