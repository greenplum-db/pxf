package org.greenplum.pxf.automation.datapreparer.hive;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * This Class is generating specific data for {@link HiveRcTest} binaryData
 * case. After running main, "hiveBinaryTextFormatData" text file will be
 * generated into src/test/resources/data/hive directory. In order to use it for
 * this case you will need to create an RC file out of it with the following
 * steps:<br>
 * Create Hive table using:<br>
 * CREATE TABLE <tbl_name> (s1 STRING, n1 INT, data1 BINARY, data2 BINARY) ROW
 * FORMAT DELIMITED FIELDS TERMINATED BY '\001';<br>
 * Load it to the created table:<br>
 * LOAD DATA LOCAL INPATH
 * '<fullpath>src/test/resources/data/hive/hiveBinaryTextFormatData' INTO TABLE
 * <tbl_name>;<br>
 * Create Hive RC table:<br>
 * CREATE TABLE <rc_tbl_name> (s1 STRING, n1 INT, data1 BINARY, data2 BINARY)
 * ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
 * STORED AS RCFILE;<br>
 * Insert the date from the Hive text table to the Hive RC table:<br>
 * INSERT INTO TABLE <rc_tbl_name> SELECT * FROM <tbl_name>;<br>
 * Override existing file: src/test/resources/data/hive/hiveBinaryRcFormatData
 * with the created RC file from HDFS path:
 * /hive/warehouse/<rc_tbl_name>/000000_0.
 */
public class HiveRcBinaryDataGen {

    public static byte delimiter = '\001';

    public static void main(String[] args) throws IOException {
        byte[] data = new byte[223];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i + 33);
        }
        // generate local data file from data byte array
        File tempDataFile = new File(
                "src/test/resources/data/hive/hiveBinaryTextFormatData");
        tempDataFile.getParentFile().mkdirs();
        FileOutputStream fos = new FileOutputStream(tempDataFile);

        for (int i = 0; i < 10; i++) {
            fos.write(("index_" + String.valueOf(i + 1)).getBytes());
            fos.write(delimiter);
            fos.write(String.valueOf(i + 1).getBytes());
            fos.write(delimiter);
            fos.write(data, 0, data.length);
            fos.write(delimiter);
            fos.write(data, 0, data.length);
            fos.write(System.lineSeparator().getBytes());
        }
        fos.flush();
        fos.close();
    }
}
