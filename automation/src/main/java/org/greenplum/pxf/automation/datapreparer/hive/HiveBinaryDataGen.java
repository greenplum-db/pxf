package org.greenplum.pxf.automation.datapreparer.hive;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * This Class is generating specific data for {@link HivelongBinaryData} binaryData
 * case. After running main, "hiveBinaryData" text file will be
 * generated into src/test/resources/data/hive directory. In order to use it for
 * this case you will need to create an RC file out of it with the following
 * steps:<br>
 * Create Hive table using:<br>
 * CREATE TABLE <tbl_name> (b1 BINARY) ROW
 * FORMAT DELIMITED FIELDS TERMINATED BY '\001';<br>
 * Load it to the created table:<br>
 * LOAD DATA LOCAL INPATH
 * '<fullpath>src/test/resources/data/hive/hiveBinaryData' INTO TABLE
 * <tbl_name>;<br>
 *
 * Override existing file: src/test/resources/data/hive/hiveBinaryData
 */
public class HiveBinaryDataGen {

    public static void main(String args[]) throws IOException {
        try {
            FileOutputStream outputStream = new FileOutputStream("src/test/resources/data/hive/hiveBinaryData");
            BufferedOutputStream out = new BufferedOutputStream(outputStream);
            try {
                for (int i = 0; i < 256; i++) {
                    out.write(i);
                }
                out.flush();
            } finally {
                out.close();
            }
        } catch (IOException ex) {
            System.out.println (ex.toString());
        }
    }
}
