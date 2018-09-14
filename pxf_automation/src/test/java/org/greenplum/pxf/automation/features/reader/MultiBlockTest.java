package org.greenplum.pxf.automation.features.reader;

import java.util.List;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.utils.data.DataUtils;
import org.greenplum.pxf.automation.utils.json.JsonUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import org.greenplum.pxf.automation.features.BaseFeature;

/**
 * Test class to test reading of multi block data from HDFS in TEXT format. The
 * class will read a json format list of table and data meta info and then in a
 * sequential order, will generate the external table and their HDFS data, then
 * will run a query and compare to verify the expected result. Data and table
 * will remain after test finish, on next run the corresponding table/data will
 * be dropped/deleted before each new table/data. The json list is read from a
 * text file, the file include - Static tables/data that are generated from a
 * fixed values defined in the file and repeated . - Dynamic tables/data that
 * their values are generated online (the tables column type and list is
 * statically defined)
 */
public class MultiBlockTest extends BaseFeature {
    private List<Table> testTables;

    @Override
    protected void beforeClass() throws Exception {
        // Initialize the collection of tables from JSON format file.
        testTables = JsonUtils.getTablesDataFromFile(System.getProperty("user.dir")
                + "/src/test/resources/multi_block_tables.json");
    }

    /**
     * Main test method. Run on the table collection and in each iteration: -
     * generate the gpdb external table & hdfs data - run the query - verify the
     * result In case test result verify is failed and exception is thrown, the
     * test ends unsuccessfully.
     *
     * @throws Exception
     */
    @Test(groups = "load")
    public void mainTest() throws Exception {
        for (Table table : testTables) {
            System.out.println(table.getDataPattern());
            // Create the data and load to HDFS //
            DataUtils.generateAndLoadData(table, hdfs);
            // Create Gpdb external table
            createExternalTable(table);
            // Run the Gpdb query and verify its result
            runQuery(table);
            // Clean last passed file from HDFS
            String textFilePath = hdfs.getWorkingDirectory() + "/"
                    + table.getName();
            hdfs.removeDirectory(textFilePath);
        }
    }

    /**
     * Create external table on GPDB using table meta info
     *
     * @param table
     * @throws Exception
     */
    private void createExternalTable(final Table table) throws Exception {
        exTable = TableFactory.getPxfReadableTextTable(table.getName(),
                table.getFields(),
                hdfs.getWorkingDirectory() + "/" + table.getName(),
                table.getDataPattern().getColumnDelimiter());
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
    }

    /**
     * Run the GPDB query and verify the expected result
     *
     * @param table
     * @throws Exception
     */
    private void runQuery(final Table table) throws Exception {
        Table analyzeResults = new Table("results", null);
        gpdb.queryResults(analyzeResults,
                "SELECT COUNT(*) FROM " + table.getName());
        Table sudoResults = new Table("sudoResults", null);
        sudoResults.addRow(new String[] { new Long(table.getNumberOfLines()).toString() });
        ComparisonUtils.compareTables(analyzeResults, sudoResults, null);
    }
}
