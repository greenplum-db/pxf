package org.greenplum.pxf.automation.datapreparer.hive;

import java.util.ArrayList;
import java.util.List;

import org.greenplum.pxf.automation.fileformats.IDataPreparer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Currently used for generating data for {@link HiveRcTest} for
 * "binaryData" case.<br>
 * Columns being used:<br>
 * "s1 string", "n1 int", "data1 BINARY", "data2 BINARY".<br>
 *
 * Non printable characters are escaped in octal form (\xxx).<br>
 * Slash ('\') is escaped.<br>
 */
public class HiveRcBinaryPreparer implements IDataPreparer {

    @Override
    public Object[] prepareData(int rows, Table dataTable) throws Exception {
        StringBuilder sb = new StringBuilder();

        // define the binary data for the binary fields. 0-33 are empty values
        // according to the ASCII table
        for (int i = 33; i < 127; i++) {
            char c = (char) i;
            sb.append(c);
            // escape slash
            if (c == '\\') {
                sb.append(c);
            }
        }
        // escape the non-printable characters in the octal form \xxx
        for (int i = 127; i < 256; i++) {
            byte b = (byte) i;
            sb.append(String.format("\\%03o", b & 0xff));
        }
        String binaryData = sb.toString();
        // fill the dataTable with data * rows
        for (int i = 0; i < rows; i++) {
            List<String> row = new ArrayList<String>();
            row.add(("index_" + String.valueOf(i + 1)));
            row.add(String.valueOf(i + 1));
            row.add(binaryData);
            row.add(binaryData);
            dataTable.addRow(row);
        }
        return null;
    }
}
