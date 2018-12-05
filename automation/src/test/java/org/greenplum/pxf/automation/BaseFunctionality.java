package org.greenplum.pxf.automation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.greenplum.pxf.automation.structures.tables.basic.Table;

/** Functionality Tests Base Class */
public abstract class BaseFunctionality extends BaseTestParent {
    // file name for storing data on HDFS
    protected String fileName = "data.txt";

    /**
     * Create Data Table with small data
     * with numRows number of rows
     * following fields: int, String, double, long and boolean
     *
     * @return Table
     * @throws IOException
     */
    protected Table getSmallData(int numRows) throws IOException {
        List<List<String>> data = new ArrayList<List<String>>();

        for (int i = 1; i <= numRows; i++) {
            List<String> row = new ArrayList<String>();
            row.add("row_" + i);
            row.add(String.valueOf(i));
            row.add(String.valueOf(Double.toString(i)));
            row.add(String.valueOf(Long.toString(100000000000L * i)));
            row.add(String.valueOf(i % 2 == 0));
            data.add(row);
        }

        Table dataTable = new Table("dataTable", null);
        dataTable.setData(data);

        return dataTable;
    }

    protected Table getSmallData() throws IOException {
        return getSmallData(100);
    }

    @Override
    protected void runTincTest(String tincTest) throws Exception {
        try {
            if (tincTest.contains("hcatalog")) {
                // These features/test cases are not supported. Do Nothing.
            } else {
                super.runTincTest(tincTest);
            }
        } catch (Exception e) {
            throw new Exception("Tinc Failure (" + e.getMessage() + ")");
        }
    }
}
