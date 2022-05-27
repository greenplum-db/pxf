package org.greenplum.pxf.automation.features.orc;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;

import static java.lang.Thread.sleep;

public class OrcWriteTest extends BaseFeature {

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

    private ArrayList<File> filesToDelete;
    private String publicStage;
    private String gpdbTable;
    private String hdfsPath;
    private String resourcePath;
    private String fullTestPath;
    private ProtocolEnum protocol;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/writableOrc/";
        protocol = ProtocolUtils.getProtocol();

        resourcePath = localDataResourcesFolder + "/orc/";
    }

    @Override
    public void beforeMethod() throws Exception {
        filesToDelete = new ArrayList<>();
        publicStage = "/tmp/publicstage/pxf/";
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitives() throws Exception {
        gpdbTable = "orc_primitive_types";
        fullTestPath = hdfsPath + "orc_primitive_types";
        prepareWritableExternalTable(gpdbTable, ORC_TABLE_COLUMNS, fullTestPath);

        prepareReadableExternalTable(gpdbTable, ORC_TABLE_COLUMNS  , fullTestPath, false /*mayByPosition*/);

        insertPrimitives(gpdbTable);

        // pull down and read created orc files using orc-tools (org.apache.orc.tools)
        for (String srcPath : hdfs.list(fullTestPath)) {
            final String fileName = srcPath.replaceAll("(.*)" + fullTestPath + "/", "");
            final String filePath = publicStage + fileName;
            filesToDelete.add(new File(filePath));
            filesToDelete.add(new File(publicStage + "." + fileName + ".crc"));

            int attempts = 0;
            while (!hdfs.doesFileExist(srcPath) && attempts++ < 20) {
                sleep(1000);
            }
            hdfs.copyToLocal(srcPath, filePath);
            sleep(250);
        }

        runTincTest("pxf.features.orc.write.primitive_types.runTest");
    }


    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitivesWithNulls() throws Exception {
        gpdbTable = "orc_primitive_types_nulls";
        fullTestPath = hdfsPath + "orc_primitive_types_nulls";
        prepareWritableExternalTable(gpdbTable, ORC_TABLE_COLUMNS, fullTestPath);

        prepareReadableExternalTable(gpdbTable, ORC_TABLE_COLUMNS  , fullTestPath, false /*mayByPosition*/);

        insertDataWithNulls(gpdbTable);

        runTincTest("pxf.features.orc.write.primitive_types_nulls.runTest");
    }

    @Override
    protected void afterMethod() throws Exception {
        super.afterMethod();
        if (ProtocolUtils.getPxfTestDebug().equals("true")) {
            return;
        }
        for (File file : filesToDelete) {
            if (!file.delete()) {
                ReportUtils.startLevel(null, getClass(), String.format("Problem deleting file '%s'", file));
            }
        }
    }

    private void insertPrimitives(String exTable) throws Exception {
        gpdb.runQuery("INSERT INTO " + exTable + "_writable " + "VALUES " +
            "(1,'row1','2019-12-01',1200,'good',FALSE,'2013-07-13 21:00:00',2147483647,'\\x31'::bytea,-32768,7.7,'s_6','USD',1.23456,0,0.12345,1)," +
            "(2,'row2','2019-12-02',1300,'excellent',TRUE,'2013-07-13 21:00:00',2147483648,'\\x32'::bytea,-31500,8.7,'s_7','USD',1.23456,123.45,-0.12345,2)," +
            "(3,'row3','2019-12-03',1400,'good',FALSE,'2013-07-15 21:00:00',2147483649,'\\x33'::bytea,-31000,9.7,'s_8','USD',-1.23456,-1.45,12345678.90123,3)," +
            "(4,'row4','2019-12-04',1500,'excellent',TRUE,'2013-07-16 21:00:00',2147483650,'\\x34'::bytea,-30000,10.7,'s_9','USD',123456789.1,0.25,-12345678.90123,4)," +
            "(5,'row5','2019-12-05',1600,'good',FALSE,'2013-07-17 21:00:00',2147483651,'\\x35'::bytea,-20000,11.7,'s_10','USD',1E-12,-.25,99999999,5)," +
            "(6,'row6','2019-12-06',1700,'bad',TRUE,'2013-07-18 21:00:00',2147483652,'\\x36'::bytea,-10000,12.7,'s_11','USD',1234.889,999.99,-99999999,6)," +
            "(7,'row7','2019-12-07',1800,'good',FALSE,'2013-07-19 21:00:00',2147483653,'\\x37'::bytea,-1000,7.7,'s_12','USD',0.0001,-999.99,-99999999.99999,7)," +
            "(8,'row8','2019-12-08',1900,'bad',TRUE,'2013-07-20 21:00:00',2147483654,'\\x38'::bytea,-550,7.7,'s_13','EUR',45678.00002,1,99999999.99999,8)," +
            "(9,'row9','2019-12-09',2000,'excellent',FALSE,'2013-07-21 21:00:00',2147483655,'\\x39'::bytea,-320,7.7,'s_14','UAH',23457.1,-1,0,9)," +
            "(10,'row10','2019-12-10',2100,'bad',TRUE,'2013-07-22 21:00:00',2147483656,'\\x30'::bytea,-120,7.7,'s_15','USD',45678.00002,789,1,10)," +
            "(11,'row11','2019-12-11',2200,'good',FALSE,'2013-07-23 21:00:00',2147483657,'\\x31'::bytea,-40,7.7,'s_16','UAH',0.123456789,-789,-1,11);");
    }

    private void insertDataWithNulls(String exTable) throws Exception {
        gpdb.runQuery("INSERT INTO " + exTable + "_writable " + "VALUES " +
                "(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)," +
                "(2,'row2_text_null','2019-12-12',2300,NULL,FALSE,'2013-07-23 21:00:00',2147483658,'\\x31'::bytea,-1,7.7,'s_16','EUR',0.123456789,0.99,0.9,11)," +
                "(3,'row3_int_null','2019-12-13',2400,'good',FALSE,'2013-07-23 21:00:00',2147483659,'\\x31'::bytea,0,7.7,'s_16','USD',0.123456789,-0.99,-0.9,NULL)," +
                "(4,'row4_double_null','2019-12-14',NULL,'excellent',FALSE,'2013-07-23 21:00:00',2147483660,'\\x31'::bytea,1,7.7,'s_16','UAH',0.123456789,1.99,45,11)," +
                "(5,'row5_decimal_null','2019-12-15',2500,'good',FALSE,'2013-07-24 21:00:00',2147483661,'\\x31'::bytea,100,7.7,'s_17','USD',NULL,NULL,NULL,12)," +
                "(6,'row6_timestamp_null','2019-12-16',2550,'bad',FALSE,NULL,2147483662,'\\x31'::bytea,1000,7.7,'s_160','USD',0.123456789,-1.99,-45,11)," +
                "(7,'row7_real_null','2019-12-17',2600,'good',FALSE,'2013-07-23 21:00:00',2147483663,'\\x31'::bytea,10000,NULL,'s_161','EUR',0.123456789,15.99,3.14159,11)," +
                "(8,'row8_bigint_null','2019-12-18',2600,'bad',FALSE,'2013-07-23 21:00:00',NULL,'\\x31'::bytea,20000,7.7,'s_162','USD',0.123456789,-15.99,-3.14159,11)," +
                "(9,'row19_bool_null','2019-12-19',2600,'good',NULL,'2013-07-23 21:00:00',-1,'\\x31'::bytea,30000,7.7,'s_163','USD',0.123456789,-299.99,2.71828,11)," +
                "(10,'row10','2019-12-20',2600,'excellent',FALSE,'2013-07-23 21:00:00',-2147483643,'\\x31'::bytea,31000,7.7,'s_164','UAH',0.123456789,299.99,-2.71828,11)," +
                "(11,'row11_smallint_null','2019-12-21',2600,'good',FALSE,'2013-07-23 21:00:00',-2147483644,'\\x31'::bytea,NULL,7.7,'s_165','USD',0.123456789,555.55,45.99999,11)," +
                "(12,'row12_date_null',NULL,2600,'excellent',FALSE,'2013-07-23 21:00:00',-2147483645,'\\x31'::bytea,32100,7.7,'s_166','EUR',0.123456789,0.15,-45.99999,11)," +
                "(13,'row13_varchar_null','2019-12-23',2600,'good',FALSE,'2013-07-23 21:00:00',-2147483646,'\\x31'::bytea,32200,7.7,NULL,'EUR',0.123456789,3.89,450.45001,11)," +
                "(14,'row14_char_null','2019-12-24',2600,'bad',FALSE,'2013-07-23 21:00:00',-2147483647,'\\x31'::bytea,32500,7.7,'s_168',NULL,0.123456789,3.14,0.00001,11)," +
                "(15,'row15_binary_null','2019-12-25',2600,'good',FALSE,'2013-07-23 21:00:00',-2147483648,NULL,32767,7.7,'s_169','USD',0.123456789,8,-0.00001,11);");
   }

    private void prepareWritableExternalTable(String name, String[] fields, String path) throws Exception {
        exTable = new WritableExternalTable(name + "_writable", fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(protocol.value() + ":orc");

        createTable(exTable);
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path, boolean mapByPosition) throws Exception {
        exTable = new ReadableExternalTable(name+ "_readable", fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(protocol.value() + ":orc");

        if (mapByPosition) {
            exTable.setUserParameters(new String[]{"MAP_BY_POSITION=true"});
        }

        createTable(exTable);
    }
}
