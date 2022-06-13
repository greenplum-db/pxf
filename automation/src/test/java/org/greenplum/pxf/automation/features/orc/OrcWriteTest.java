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
import java.util.Arrays;

import static java.lang.Thread.sleep;

public class OrcWriteTest extends BaseFeature {

    private static final String[] ORC_PRIMITIVE_TABLE_COLUMNS = {
            "id     integer"      ,
            "col00  boolean"      , // DataType.BOOLEAN
            "col01  bytea"        , // DataType.BYTEA
            "col02  bigint"       , // DataType.BIGINT
            "col03  smallint"     , // DataType.SMALLINT
            "col04  integer"      , // DataType.INTEGER
            "col05  text"         , // DataType.TEXT
            "col06  real"         , // DataType.REAL
            "col07  float"        , // DataType.FLOAT8
            "col08  char(4)"      , // DataType.BPCHAR
            "col09  varchar(7)"   , // DataType.VARCHAR
            "col10  date"         , // DataType.DATE
            "col11  time"         , // DataType.TIME
            "col12  timestamp"    , // DataType.TIMESTAMP
            "col13  timestamptz"  , // DataType.TIMESTAMP_WITH_TIME_ZONE
            "col14  numeric"      , // DataType.NUMERIC
            "col15  uuid"           // DataType.UUID
    };

    private static final boolean[] NO_NULLS = new boolean[ORC_PRIMITIVE_TABLE_COLUMNS.length - 1]; // 16 main columns
    private static final boolean[] ALL_NULLS = new boolean[ORC_PRIMITIVE_TABLE_COLUMNS.length - 1]; // 16 main columns
    static {
        Arrays.fill(ALL_NULLS, true);
    }

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
        prepareWritableExternalTable(gpdbTable, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);

        prepareReadableExternalTable(gpdbTable, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath, false /*mapByPosition*/);

        insertDataWithoutNulls(gpdbTable, 33); // > 30 to let the DATE field to repeat the value

        // pull down and read created orc files using orc-tools (org.apache.orc.tools)
        /*
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
        */
        runTincTest("pxf.features.orc.write.primitive_types.runTest");
    }


    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitivesWithNulls() throws Exception {
        gpdbTable = "orc_primitive_types_nulls";
        fullTestPath = hdfsPath + "orc_primitive_types_nulls";
        prepareWritableExternalTable(gpdbTable, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);

        prepareReadableExternalTable(gpdbTable, ORC_PRIMITIVE_TABLE_COLUMNS  , fullTestPath, false /*mapByPosition*/);

        insertDataWithNulls(gpdbTable, 33);

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

    private void insertDataWithoutNulls(String exTable, int numRows) throws Exception {
        StringBuilder statementBuilder = new StringBuilder("INSERT INTO " + exTable + "_writable VALUES ");
        for (int i = 0; i < numRows; i++) {
            statementBuilder.append(getRecordCSV(i, NO_NULLS));
            statementBuilder.append((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(statementBuilder.toString());
    }

    private void insertDataWithNulls(String exTable, int numRows) throws Exception {
        StringBuilder statementBuilder = new StringBuilder("INSERT INTO " + exTable + "_writable VALUES ");
        int nullableColumnCount = ORC_PRIMITIVE_TABLE_COLUMNS.length - 1;
        boolean[] isNull = new boolean[nullableColumnCount]; // id column does not count
        for (int i = 0; i < numRows; i++) {
            Arrays.fill(isNull, false); // reset the isNull array
            if (i > 0) {
                int indexOfNullColumn = (i - 1) % nullableColumnCount;
                isNull[indexOfNullColumn] = true;
            }
            statementBuilder.append(getRecordCSV(i, (i == 0) ? ALL_NULLS : isNull)); // zero row is all nulls
            statementBuilder.append((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(statementBuilder.toString());
    }

    private String getRecordCSV(int row, boolean[] isNull) {
        // refer to ORCVectorizedResolverWriteTest unit test where this data is used
        StringBuilder rowBuilder = new StringBuilder("(")
        .append(row).append(",")    // always not-null row index, column index starts with 0 after it
        .append(isNull [0] ? "NULL" : row % 2 != 0).append(",")                                                  // DataType.BOOLEAN
        .append(isNull [1] ? "NULL" : String.format("'\\x%02d%02d'::bytea", row%100, (row+1)%100)).append(",")   // DataType.BYTEA
        .append(isNull [2] ? "NULL" : 123456789000000000L + row).append(",")                                     // DataType.BIGINT
        .append(isNull [3] ? "NULL" : 10L + row % 32000).append(",")                                             // DataType.SMALLINT
        .append(isNull [4] ? "NULL" : 100L + row).append(",")                                                    // DataType.INTEGER
        .append(isNull [5] ? "NULL" : String.format("'row-%02d'", row)).append(",")                              // DataType.TEXT
        .append(isNull [6] ? "NULL" : Float.valueOf(row + 0.00001f * row).doubleValue()).append(",")          // DataType.REAL
        .append(isNull [7] ? "NULL" : row + Math.PI).append(",")                                                 // DataType.FLOAT8
        .append(isNull [8] ? "NULL" : String.format("'%s'", row)).append(",")                                    // DataType.BPCHAR
        .append(isNull [9] ? "NULL" : String.format("'var%02d'", row)).append(",")                               // DataType.VARCHAR
        .append(isNull[10] ? "NULL" : String.format("'2010-01-%02d'", (row%30)+1)).append(",")                   // DataType.DATE
        .append(isNull[11] ? "NULL" : String.format("'10:11:%02d'", row % 60)).append(",")                       // DataType.TIME
        .append(isNull[12] ? "NULL" : String.format("'2013-07-13 21:00:05.%03d456'", row % 1000)).append(",")    // DataType.TIMESTAMP
        .append(isNull[13] ? "NULL" : String.format("'2013-07-13 21:00:05.987%03d-07'", row % 1000)).append(",") // DataType.TIMESTAMP_WITH_TIME_ZONE
        .append(isNull[14] ? "NULL" : String.format("'12345678900000.00000%s'", row)).append(",")                // DataType.NUMERIC
        .append(isNull[15] ? "NULL" : String.format("'476f35e4-da1a-43cf-8f7c-950a%08d'", row % 100000000))      // DataType.UUID
        .append(")");
        return rowBuilder.toString();
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
