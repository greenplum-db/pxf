package org.greenplum.pxf.automation.features.hbase;

import org.greenplum.pxf.automation.components.hbase.HBase;
import org.greenplum.pxf.automation.structures.tables.hbase.HBaseTable;
import org.greenplum.pxf.automation.structures.tables.hbase.LookupTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.datapreparer.hbase.HBaseDataPreparer;
import org.greenplum.pxf.automation.datapreparer.hbase.HBaseLongQualifierDataPreparer;
import org.greenplum.pxf.automation.features.BaseFeature;
import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.postgresql.util.PSQLException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Functional cases for PXF HBase connector
 */
public class HBaseTest extends BaseFeature {

    // Components that are used in different cases
    private HBase hbase;
    // tables objects that are used in different cases
    private HBaseTable hbaseTable;
    private HBaseTable hbaseTableWithNulls;
    private LookupTable lookupTable;
    private ReadableExternalTable exTableNullHBase;
    // used to verify no filter string returned from GPDB side
    private final String NO_FILTER = "No filter";
    // Data preparer to create data matching to the HBase tables
    private HBaseDataPreparer dataPreparer = new HBaseDataPreparer();
    // qualifiers for HBase tables
    private String[] hbaseTableQualifiers =
            new String[] { "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11", "q12" };
    // External table columns matching to HBase qualifiers
    private String[] exTableFields = new String[] {
            "recordkey TEXT",
            "\"cf1:q1\" VARCHAR",
            "\"cf1:q2\" TEXT",
            "\"cf1:q3\" INT",
            "\"cf1:q4\" BYTEA",
            "\"cf1:q5\" REAL",
            "\"cf1:q6\" FLOAT",
            "\"cf1:q7\" BYTEA",
            "\"cf1:q8\" SMALLINT",
            "\"cf1:q9\" BIGINT",
            "\"cf1:q10\" BOOLEAN",
            "\"cf1:q11\" NUMERIC",
            "\"q12\" TIMESTAMP"
    };

    // External table columns matching to HBase qualifiers with q2 and q3 appearing as mapped in the
    // lookup table
    private String[] exTableDifferentFieldsNames = new String[] {
            "recordkey TEXT",
            "\"cf1:q1\" VARCHAR",
            "\"q2\" TEXT",
            "\"q3\" INT",
            "\"cf1:q4\" BYTEA",
            "\"cf1:q5\" REAL",
            "\"cf1:q6\" FLOAT",
            "\"cf1:q7\" BYTEA",
            "\"cf1:q8\" SMALLINT",
            "\"cf1:q9\" BIGINT",
            "\"cf1:q10\" BOOLEAN",
            "\"cf1:q11\" NUMERIC",
            "\"cf1:q12\" TIMESTAMP"
    };

    private String testPackage = "org.greenplum.pxf.automation.testplugin.";

    /**
     * Prepare all components and all data flow (HBase to GPDB)
     */
    @Override
    public void beforeClass() throws Exception {
        // Initialize HBase component
        hbase = (HBase) SystemManagerImpl.getInstance().getSystemObject("hbase");

        // if hbase authorization is not enabled then grant will not be performed
        hbase.grantGlobalForUser("pxf");

        String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
        String newPath = "/tmp/publicstage/pxf";

        // copy additional plugins classes to cluster nodes, used for filter pushdown cases
        cluster.copyFileToNodes(new File("target/classes" + testPackageLocation +
                "FilterPrinterAccessor$FilterPrinterException.class").getAbsolutePath(),
                newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes" + testPackageLocation +
                "FilterPrinterAccessor.class").getAbsolutePath(),
                newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes" + testPackageLocation +
                "HBaseAccessorWithFilter$SplitBoundary.class").getAbsolutePath(),
                newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes" + testPackageLocation +
                "HBaseAccessorWithFilter.class").getAbsolutePath(),
                newPath + testPackageLocation, true, false);
        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        // Initialize HBaseTable component
        hbaseTable = new HBaseTable("hbase_table", new String[] { "cf1" });
        // get external table pointing to HBase table loaded with data
        exTable = prepareDataChain(hbaseTable, dataPreparer, 100);
        // Initialize HBaseTable component for null values table
        hbaseTableWithNulls = new HBaseTable("hbase_null_table", new String[] { "cf1" });
        dataPreparer.setUseNull(true);
        // get external table pointing to HBase table loaded with data contains 'null' values
        exTableNullHBase = prepareDataChain(hbaseTableWithNulls, dataPreparer, 100);
        // create external table with hbase full names (no mapping)
        String[] fields = exTableFields.clone();
        fields[12] = "\"cf1:q12\" TIMESTAMP";
        ReadableExternalTable exTableFullColumnNames = TableFactory.getPxfHBaseReadableTable(
                "pxf_hbase_full_names", fields, hbaseTable);
        exTableFullColumnNames.setHost(pxfHost);
        exTableFullColumnNames.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableFullColumnNames);
        // prepare lookup table with suitable mapping for 2 hbase tables
        prepareLookupTable();
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();

        // close hbase connection
        if(hbase != null) {
            hbase.close();
        }
    }

    /**
     * Prepares lookup table with suitable mapping for 2 hbase tables.
     *
     * @throws Exception if test fails to run
     */
    private void prepareLookupTable() throws Exception {
        lookupTable = new LookupTable();
        hbase.createTableAndVerify(lookupTable);
        lookupTable.addMapping(hbaseTable.getName(), "q12", "cf1:q12");
        lookupTable.addMapping(hbaseTableWithNulls.getName(), "q12", "cf1:q12");
        hbase.put(lookupTable);
    }

    /**
     * validate all data without filters are queried from HBase table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "sanity", "gpdb" })
    public void sanity() throws Exception {

        verifyFilterResults(hbaseTable, exTable, "", NO_FILTER, "sanity", false);
    }

    /**
     * Test Lower Filter
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void lowerFilter() throws Exception {

        String whereClause = " WHERE \"cf1:q3\" < '00000030'";
        String filterString = "a3c23s2d30o1";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString, "lower");
    }

    /**
     * Filter range of values
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void rangeFilter() throws Exception {

        String whereClause = " WHERE \"cf1:q3\" > '00000090' AND \"cf1:q3\" <= '00000103'";
        String filterString = "a3c23s2d90o2a3c23s3d103o3l0";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString, "range");
    }

    /**
     * filter for specific row
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void specificRowFilter() throws Exception {

        String whereClause = " WHERE \"cf1:q3\" = 4";
        String filterString = "a3c23s1d4o5";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString, "specificRow");
    }

    /**
     * not equals value
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void notEqualsFilter() throws Exception {

        String whereClause = " WHERE \"cf1:q3\" != 30";
        String filterString = "a3c23s2d30o6";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString, "notEquals");
    }

    /**
     * filter for specific row key rows
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void rowkeyEqualsFilter() throws Exception {

        String whereClause = " WHERE recordkey = '00000090'";
        String filterString = "a0c25s8d00000090o5";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString, "rowkeyEquals");
    }

    /**
     * filter for range of row keys
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void rowkeyRangeFilter() throws Exception {

        String whereClause = " WHERE recordkey > '00000090' AND recordkey <= '00000103'";
        String filterString = "a0c25s8d00000090o2a0c25s8d00000103o3l0";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString, "rowkeyRange");
    }

    /**
     * Filter with multiple supported pushdown filters
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void multipleQualifiersPushdownFilter() throws Exception {

        String whereClause = " WHERE recordkey != '00000002' AND \"cf1:q3\" > 6  AND \"cf1:q8\" < 10 AND \"cf1:q9\" > 0";
        String filterString = "a0c25s8d00000002o6a3c23s1d6o2a8c23s2d10o1a9c23s1d0o2l0l0l0";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString,
                "multipleQualifiers");
    }

    /**
     * use supported and unsupported pushdown filters
     * if at least one filter isn't supported, nothing should be pushed down
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void partialFilterPushdown() throws Exception {

        String whereClause = " WHERE \"cf1:q3\" > 6  AND \"cf1:q7\" = '42'";
        String partialfilterString = "No filter";
        verifyFilterResults(hbaseTable, exTable, whereClause, partialfilterString,
                "partialFilterPushdown", false);
    }

    /**
     * Filter for text.<br>
     * Serialized filter is corrupted when arriving to the PXF side, therefore no results.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void textFilter() throws Exception {

        String whereClause = " WHERE \"cf1:q2\" = 'UTF8_計算機用語_00000024'";
        String filterString = "a2c25s29dUTF8_計算機用語_00000024o5";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString, "text");
    }

    /**
     * Filter for double. (supported in PXF as of HAWQ-1100)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void doubleFilter() throws Exception {

        String whereClause = " WHERE \"cf1:q5\" > 91.92 AND \"cf1:q6\" <= 99999999.99";
        String filterString = "a5c701s5d91.92o2a6c701s11d99999999.99o3l0";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString,
                "double", false);
    }

    /**
     * filter using OR (supported in PXF as of HAWQ-964)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void orFilter() throws Exception {

        String whereClause = " WHERE \"cf1:q3\" < 10 OR \"cf1:q5\" > 90";
        String filterString = "a3c23s2d10o1a5c701s2d90o2l1";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString, "or", false);

        whereClause = " WHERE (((recordkey > '00000090') AND (recordkey <= '00000103')) OR (recordkey = '00000005'))";
        filterString = "a0c25s8d00000090o2a0c25s8d00000103o3l0a0c25s8d00000005o5l1";
        verifyFilterResults(hbaseTable, exTable, whereClause, filterString, "andOr", false);
    }

    /**
     * filter with AND and OR should be fully passed to PXF
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void mixedFilterPushdownOrAnd() throws Exception {

        String whereClause = " WHERE (\"cf1:q3\" < 10 OR \"cf1:q5\" > 90) AND (\"cf1:q3\" > 5 AND \"cf1:q8\" < 30)";
        String partialfilterString = "a3c23s2d10o1a5c701s2d90o2l1a3c23s1d5o2a8c23s2d30o1l0l0";
        verifyFilterResults(hbaseTable, exTable, whereClause, partialfilterString,
                "partialFilterPushdown", false);

        whereClause = " WHERE (recordkey > '00000001') AND ((recordkey <= '00000093') " +
                "AND (recordkey >= '00000080') OR recordkey = '0')";
        partialfilterString = "a0c25s8d00000001o2a0c25s8d00000093o3a0c25s8d00000080o4l0a0c25s1d0o5l1l0";
        verifyFilterResults(hbaseTable, exTable, whereClause, partialfilterString,
                "partialFilterPushdownAndOr", false);

    }

    /**
     * filter for "not null" values
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void isNullFilter() throws Exception {

        String whereClause = " WHERE \"cf1:q3\" is null";
        String partialfilterString = "a3o8";
        verifyFilterResults(hbaseTableWithNulls, exTable, whereClause, partialfilterString,
                "isNull", false);
    }

    /**
     * no match between some of external table columns and HBase table qualifier names
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void differentColumnNames() throws Exception {

        exTableNullHBase = TableFactory.getPxfHBaseReadableTable(
                "pxf_hbase_different_columns_names", exTableDifferentFieldsNames, hbaseTable);
        exTableNullHBase.setHost(pxfHost);
        exTableNullHBase.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableNullHBase);
        runSqlTest("features/hbase/errors/differentColumnsNames");
    }

    /**
     * Disable Lookup table and verify that
     * 1) query fails if a field name doesn't
     * match the hbase name and no mapping is available.
     * 2) query succeeds if no mapping is needed.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void disableLookupTable() throws Exception {

        try {
            hbase.disableTable(lookupTable);
            runSqlTest("features/hbase/errors/lookupTable");
        } finally {
            hbase.enableTable(lookupTable);
        }
    }

    /**
     * Drop Lookup table and verify that
     * 1) query fails if a field name doesn't
     * match the hbase name and no mapping is available.
     * 2) query succeeds if no mapping is needed.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void noLookupTable() throws Exception {

        try {
            hbase.dropTable(lookupTable, false);
            runSqlTest("features/hbase/errors/lookupTable");
        } finally {
            prepareLookupTable();
        }
    }

    /**
     * Remove column from Lookup table and verify that
     * 1) query fails if a field name doesn't
     * match the hbase name and no mapping is available.
     * 2) query succeeds if no mapping is needed.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void removeColumnFromLookupTable() throws Exception {

        try {
            // add column before removing "mapping" column,
            // because a table has to have at least one column.
            hbase.addColumn(lookupTable,new String[] { "no_mapping" });
            hbase.removeColumn(lookupTable, new String[] { "mapping" });
            runSqlTest("features/hbase/errors/lookupTable");
        } finally {
            prepareLookupTable();
        }
    }

    /**
     * Recoded key defined as INTEGER in GPDB table, Filter range of record keys as integer
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void recordkeyAsInteger() throws Exception {

        // create external table with record key as INTEGER
        String[] fields = exTableFields.clone();
        fields[0] = "recordkey INTEGER";
        ReadableExternalTable integerRecordKeyExtTable = TableFactory.getPxfHBaseReadableTable(
                "pxf_hbase_integer_key", fields, hbaseTable);
        integerRecordKeyExtTable.setHost(pxfHost);
        integerRecordKeyExtTable.setPort(pxfPort);
        gpdb.createTableAndVerify(integerRecordKeyExtTable);
        String whereClause = " WHERE recordkey > 90 AND recordkey <= 103";
        String filterString = "a0c23s2d90o2a0c23s3d103o3l0";
        verifyFilterResults(hbaseTable, integerRecordKeyExtTable, whereClause,
                filterString, "recordkeyAsInteger");
    }

    /**
     * verify error for not existing HBase table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void notExistingHBaseTable() throws Exception {

        ReadableExternalTable notExistsHBaseTableExtTable = TableFactory.getPxfHBaseReadableTable(
                "pxf_not_existing_hbase_table", exTableFields,
                new HBaseTable("dummy", null));
        notExistsHBaseTableExtTable.setHost(pxfHost);
        notExistsHBaseTableExtTable.setPort(pxfPort);
        gpdb.createTableAndVerify(notExistsHBaseTableExtTable);
        runSqlTest("features/hbase/errors/notExistingHBaseTable");
    }

    /**
     * Create 100 regions and 300 rows of data for each region.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void multiRegionsData() throws Exception {

        HBaseTable multiDataHBaseTable = new HBaseTable(
                "hbase_table_multi_regions", new String[] { "cf1" });
        int numberOfRegions = 100;
        int rowsPerRegion = 300;
        multiDataHBaseTable.setNumberOfSplits(numberOfRegions);
        multiDataHBaseTable.setRowsPerSplit(rowsPerRegion);
        LookupTable addtionalMapping = new LookupTable();
        addtionalMapping.addMapping(multiDataHBaseTable.getName(), "q12", "cf1:q12");
        hbase.put(addtionalMapping);
        dataPreparer.setNumberOfSplits(numberOfRegions);
        // prepare all data flow
        prepareDataChain(multiDataHBaseTable, dataPreparer, rowsPerRegion);
        runSqlTest("features/hbase/multiRegionsData");
    }

    /**
     * check default analyze results for PXF external table is as required (pages=1000
     * tuples=1000000)
     *
     * @throws Exception if test fails to run
     */
    // @Test(groups = { "features" })
    public void defaultAnalyze() throws Exception {

        // Perform Analyze on external table and check suitable Warnings
        gpdb.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(),
                "ANALYZE for HBase plugin is not supported", true);
        runSqlTest("features/hbase/default_analyze");
    }

    /**
     * use long HBase table qualifier (over 64 chars) with no lookup table mapping. Verify the
     * column name in GPDB is being truncated and data for this column is missing
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void longHBaseQualifierNameNoLookupTable() throws Exception {

        HBaseTable longQualifiersNamesHBaseTable = new HBaseTable(
                "long_qualifiers_hbase_table", new String[] { "cf1" });
        String[] qualifiers = new String[] {
                "very_long_qualifier_name_that_gpdb_will_probaly_is_going_to_cut",
                "short_qualifier"
        };
        String[] gpdbFields = new String[] {
                "recordkey TEXT",
                "\"cf1:very_long_qualifier_name_that_gpdb_will_probaly_is_going_to_cut\" TEXT",
                "\"cf1:short_qualifier\" TEXT"
        };
        // create HBase table with data
        longQualifiersNamesHBaseTable.setQualifiers(qualifiers);
        hbase.createTableAndVerify(longQualifiersNamesHBaseTable);
        HBaseLongQualifierDataPreparer dataPreparer = new HBaseLongQualifierDataPreparer();
        dataPreparer.prepareData(10, longQualifiersNamesHBaseTable);
        hbase.put(longQualifiersNamesHBaseTable);
        // create external table pointing the the HBase table
        ReadableExternalTable externalTable = TableFactory.getPxfHBaseReadableTable(
                "long_qualifiers_hbase_table", gpdbFields, longQualifiersNamesHBaseTable);
        externalTable.setHost(pxfHost);
        externalTable.setPort(pxfPort);
        try {
            gpdb.createTableAndVerify(externalTable);
            // if no Exception thrown, fail test
            Assert.fail("Exception should have been thrown");
        } catch (Exception e) {
            // Verify message in caught exception
            ExceptionUtils.validate(null, e,
                    new PSQLException("identifier \"cf1:very_long_qualifier_name_that_gpdb_will_probaly_is_going_to_cut\" "
                            + "will be truncated to \"cf1:very_long_qualifier_name_that_gpdb_will_probaly_is_going_to\"",
                            null), false, true);
        }
        // verify query results
        runSqlTest("features/hbase/longQualifierNoLookup");
    }

    /**
     * use long HBase table qualifier (over 64 chars) using lookup table mapping. Query all data and
     * verify.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void longHBaseQualifierNameUsingLookupTable() throws Exception {

        HBaseTable longQualifiersNamesHBaseTable = new HBaseTable(
                "long_qualifiers_hbase_table", new String[] { "cf1" });
        String[] qualifiers = new String[] {
                "very_long_qualifier_name_that_gpdb_will_probaly_is_going_to_cut",
                "short_qualifier"
        };
        // alias External table column to HBase table qualifiers
        String[] gpdbFields = new String[] {
                "used_to_be_long TEXT",
                "short TEXT"
        };
        // create HBase table with data
        longQualifiersNamesHBaseTable.setQualifiers(qualifiers);
        hbase.createTableAndVerify(longQualifiersNamesHBaseTable);
        HBaseLongQualifierDataPreparer dataPreparer = new HBaseLongQualifierDataPreparer();
        dataPreparer.prepareData(10, longQualifiersNamesHBaseTable);
        hbase.put(longQualifiersNamesHBaseTable);
        // map alias External table column to qualifiers
        LookupTable addtionalMapping = new LookupTable();
        addtionalMapping.addMapping(longQualifiersNamesHBaseTable.getName(),
                "used_to_be_long", "cf1:" + qualifiers[0]);
        addtionalMapping.addMapping(longQualifiersNamesHBaseTable.getName(),
                "short", "cf1:" + qualifiers[1]);
        hbase.put(addtionalMapping);

        ReadableExternalTable externalTable = TableFactory.getPxfHBaseReadableTable(
                "long_qualifiers_hbase_table", gpdbFields, longQualifiersNamesHBaseTable);
        externalTable.setHost(pxfHost);
        externalTable.setPort(pxfPort);
        gpdb.createTableAndVerify(externalTable);
        runSqlTest("features/hbase/longQualifierWithLookup");
    }

    /**
     * Query an HBase table with no data and verify that 0 rows are returned.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hbase", "features", "gpdb" })
    public void emptyHBaseTable() throws Exception {

        HBaseTable emptyTable = new HBaseTable("empty_table", new String[] { "cf1" });
        String[] exTableFields = new String[] {
                "recordkey TEXT",
                "\"cf1:q1\" VARCHAR",
                "\"cf1:q2\" TEXT",
                "\"cf1:q3\" INT"
        };

        hbase.createTableAndVerify(emptyTable);

        ReadableExternalTable exTable = TableFactory.getPxfHBaseReadableTable(
                "empty_hbase_table", exTableFields, emptyTable);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);

        verifyFilterResults(emptyTable, exTable, "", NO_FILTER,
                "empty", false);
    }

    /**
     * See verifyFilterResults documentation
     *
     * @param hbaseTable HBase table
     * @param externalTable External Table pointing to the HBase table
     * @param whereClause SQL filter expression
     * @param filterString serialized filter string match to the relevant case or NO_FILTER
     * @param sqlTestPath path of SQL test case to run
     *
     * @throws Exception if test fails to run
     */
    private void verifyFilterResults(HBaseTable hbaseTable, ReadableExternalTable externalTable,
            String whereClause, String filterString, String sqlTestPath) throws Exception {

        verifyFilterResults(hbaseTable, externalTable, whereClause, filterString,
                sqlTestPath, true);
    }

    /**
     * Verify PXF Filters over HBase table: <br>
     * pushdown off<br>
     * pushdown on,<br>
     * check correct serialized filter string generated,<br>
     * inject serialized filter string and verify results
     *
     * @param hbaseTable HBase table
     * @param externalTable External Table pointing to the HBase table
     * @param whereClause SQL filter expression
     * @param filterString serialized filter string match to the relevant case or NO_FILTER
     * @param sqlTestPath path of SQL test case to run
     * @param verifyFilterString some cases do not required injecting serialized filter string and
     *            verify results - false for not verifying
     *
     * @throws Exception if test fails to run
     */
    private void verifyFilterResults(HBaseTable hbaseTable, ReadableExternalTable externalTable,
            String whereClause, String filterString, String sqlTestPath, boolean verifyFilterString) throws Exception {

        // check using no filter pushdown
        gpdb.runQuery("SET gp_external_enable_filter_pushdown = off");
        // run SQL test case to verify filter is working
        runSqlTest("features/hbase/" + sqlTestPath);
        // check with filter pushdown
        gpdb.runQuery("SET gp_external_enable_filter_pushdown = on");
        // run SQL test case to see filter is working
        runSqlTest("features/hbase/" + sqlTestPath);
        // use FilterPrinterAccessor to get the serialized filter in the pxf side
        createAndQueryPxfGpdbFilterTable(hbaseTable, externalTable.getFields(), whereClause, filterString);
        if (verifyFilterString) {
            // create another GPDB external table that uses HBaseAccessorWithFilter to get the
            // filter as user parameter
            createPxfHBaseFilterTable(filterString, hbaseTable, externalTable.getFields());
            // run SQL test case to get filtered results
            runSqlTest("features/hbase/filter_accessor/" + sqlTestPath);
        }
    }

    /**
     * Create PXF external table using FilterPrinterAccessor and verify in the Exception output the
     * suitable serialized Filter string
     *
     * @param hbaseTable HBase table
     * @param fields suitable external table column for hbase table
     * @param whereClause SQL filter expression
     * @param expectedFilter expected serialized filter string match to the relevant where Clause
     *
     * @throws Exception if test fails to run
     */
    private void createAndQueryPxfGpdbFilterTable(HBaseTable hbaseTable, String[] fields,
            String whereClause, String expectedFilter) throws Exception {

        ReadableExternalTable externalTableFilterPrinter = new ReadableExternalTable(
                "hbase_pxf_print_filter", fields, hbaseTable.getName(), "CUSTOM");
        externalTableFilterPrinter.setFragmenter("org.greenplum.pxf.plugins.hbase.HBaseDataFragmenter");
        externalTableFilterPrinter.setAccessor(testPackage + "FilterPrinterAccessor");
        externalTableFilterPrinter.setResolver("org.greenplum.pxf.plugins.hbase.HBaseResolver");
        externalTableFilterPrinter.setFormatter("pxfwritable_import");
        externalTableFilterPrinter.setHost(pxfHost);
        externalTableFilterPrinter.setPort(pxfPort);
        gpdb.createTableAndVerify(externalTableFilterPrinter);

        try {
            gpdb.queryResults(externalTableFilterPrinter,
                    "SELECT * FROM " + externalTableFilterPrinter.getName() +
                            " " + whereClause + " ORDER BY recordkey ASC");
        } catch (Exception e) {
            // tomcat displays special character in their HTML representation, need to convert our
            // string as well. We can't use StringEscapeUtils.escapeHtml() because
            // it escapes also the special UTF-8 characters, while the exception does not.
            expectedFilter = expectedFilter.replace("\"", "&quot;");
            // verify filter in the caught Exception
            ExceptionUtils.validate(null, e, new Exception("ERROR.*Filter string: '" + expectedFilter + "'.*"),
                    true, true);
        }
    }

    /**
     * Creates PXF HBase table using HBaseAccessorWithFilter.
     *
     * @param filter serialized filter string to be used in table created.
     * @param hbaseTable HBase table to be used in the table's path.
     * @param fields table's fields.
     *
     * @throws Exception if test fails to run
     */
    private void createPxfHBaseFilterTable(String filter, HBaseTable hbaseTable, String[] fields) throws Exception {

        ReadableExternalTable externalTableHBaseWithFilter = new ReadableExternalTable(
                "hbase_pxf_with_filter", fields, hbaseTable.getName(), "CUSTOM");
        externalTableHBaseWithFilter.setFragmenter("org.greenplum.pxf.plugins.hbase.HBaseDataFragmenter");
        externalTableHBaseWithFilter.setAccessor(testPackage + "HBaseAccessorWithFilter");
        externalTableHBaseWithFilter.setResolver("org.greenplum.pxf.plugins.hbase.HBaseResolver");
        externalTableHBaseWithFilter.setFormatter("pxfwritable_import");
        externalTableHBaseWithFilter.setUserParameters(new String[] { "TEST-HBASE-FILTER=" + filter });
        externalTableHBaseWithFilter.setHost(pxfHost);
        externalTableHBaseWithFilter.setPort(pxfPort);
        gpdb.createTableAndVerify(externalTableHBaseWithFilter);
    }

    /**
     * Create data flow:<br>
     * Create HBase table<br>
     * Load data<br>
     * Create External table pointing to the HBase table
     *
     * @param hbaseTable HBase table
     * @param dataPreparer Data preparer
     * @param rows amount of rows to generate
     * @return {@link ReadableExternalTable} PXF External table pointing to the HBase table
     *
     * @throws Exception if test fails to run
     */
    private ReadableExternalTable prepareDataChain(HBaseTable hbaseTable,
            HBaseDataPreparer dataPreparer, int rows) throws Exception {

        hbaseTable.setRowsPerSplit(rows);
        hbaseTable.setRowKeyPrefix("row");
        hbaseTable.setQualifiers(hbaseTableQualifiers);
        // create HBase table
        hbase.createTableAndVerify(hbaseTable);
        // create and load data to HBase table
        dataPreparer.setColumnFamilyName(hbaseTable.getFields()[0]);
        dataPreparer.prepareData(hbaseTable.getRowsPerSplit(), hbaseTable);
        hbase.put(hbaseTable);
        // Create external tables pointing to the HBase table
        ReadableExternalTable exTable = TableFactory.getPxfHBaseReadableTable(
                "pxf_" + hbaseTable.getName(), exTableFields, hbaseTable);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
        return exTable;
    }
}
