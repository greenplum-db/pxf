package org.greenplum.pxf.automation.features.hdfs;

import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.datapreparer.CustomSequencePreparer;
import org.greenplum.pxf.automation.datapreparer.CustomTextPreparer;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * PXF on HDFS Analyze tests:
 *
 * <ol>
 * <li>analyze of ok table</li>
 * <li>analyze of bad table (no estimate)</li>
 * <li>analyze of table without analyzer (no estimate)</li>
 * <li>analyze of table with bad resolver (estimate ok, resolver fails)</li>
 * <li>analyze of table with bad resolver in the middle of stream (estimate ok,
 * resolver fails after 10000)</li>
 * <li>analyze of table failing on reject limit</li>
 * <li>pass pxf_max_fragments as a table parameter and check that the expected
 * number of fragments returned (several files, each with 1 record)</li>
 * <li>pass pxf_sample_ratio=x as a table parameter and check that out of 100
 * (and more) records in a file, only x% are returned.</li>
 * <li>analyze on tables with ~10M records.</li>
 * </ol>
 *
 * Recommended table sizes:
 * <ul>
 * <li>TPCH 1GB ~ 6M records.</li>
 * <li>large scale 10^10 records.</li>
 * <li>performance 10TB.</li>
 * </ul>
 */
@WorksWithFDW
public class HdfsAnalyzeTest extends BaseFeature {

    private String resourcePath;

    private final String SUFFIX_CLASS = ".class";

    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String testPackage = "org.greenplum.pxf.automation.testplugin.";

    String throwOn10000Resolver = "ThrowOn10000Resolver";

    String schemaPackageLocation = "/org/greenplum/pxf/automation/dataschema/";
    String schemaPackage = "org.greenplum.pxf.automation.dataschema.";

    String customSchemaFileName = "CustomWritable";

    final int DEFAULT_RELPAGES = 1000;
    final int DEFAULT_RELTUPLES = 1000000;

    // holds data for file generation
    Table dataTable = null;
    // path for storing data on HDFS
    String hdfsFilePath = "";

    String[] textFields = new String[] {
            "s1 text",
            "s2 text",
            "s3 text",
            "d1 timestamp",
            "n1 int",
            "n2 int",
            "n3 int",
            "n4 int",
            "n5 int",
            "n6 int",
            "n7 int",
            "s11 text",
            "s12 text",
            "s13 text",
            "d11 timestamp",
            "n11 int",
            "n12 int",
            "n13 int",
            "n14 int",
            "n15 int",
            "n16 int",
            "n17 int" };

    String[] customWritableFields = {
            "tmp1  timestamp",
            "num1  integer",
            "num2  integer",
            "num3  integer",
            "num4  integer",
            "t1    text",
            "t2    text",
            "t3    text",
            "t4    text",
            "t5    text",
            "t6    text",
            "dub1  double precision",
            "dub2  double precision",
            "dub3  double precision",
            "ft1   real",
            "ft2   real",
            "ft3   real",
            "ln1   bigint",
            "ln2   bigint",
            "ln3   bigint",
            "bool1 boolean",
            "bool2 boolean",
            "bool3 boolean",
            "short1 smallint",
            "short2 smallint",
            "short3 smallint",
            "short4 smallint",
            "short5 smallint",
            "bt    bytea" };

    /**
     * Prepares all components and all data flow (Hdfs to GPDB)
     */
    @Override
    public void beforeClass() throws Exception {
        // location of test plugin files
        resourcePath = "target/classes" + testPackageLocation;

        String newPath = "/tmp/publicstage/pxf";
        // copy test plugin files to cluster nodes
        cluster.copyFileToNodes(new File(resourcePath + throwOn10000Resolver
                + SUFFIX_CLASS).getAbsolutePath(), newPath
                + testPackageLocation, true, false);

        // location of schema file
        resourcePath = "target/classes" + schemaPackageLocation;

        // copy schema file to cluster nodes
        cluster.copyFileToNodes(new File(resourcePath + customSchemaFileName
                + SUFFIX_CLASS).getAbsolutePath(), newPath
                + schemaPackageLocation, true, false);

        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    /**
     * Before every method determine default hdfs data Path, default data, and
     * default external table structure. Each case change it according to it
     * needs.
     *
     * @throws Exception
     */
    @Override
    protected void beforeMethod() throws Exception {
        super.beforeMethod();

        // prepare data in table
        dataTable = new Table("dataTable", null);

        // default definition of external table, matching
        // data produced by CustomTextPreparer.
        exTable = TableFactory.getPxfReadableTextTable("analyze_text",
                textFields, hdfsFilePath, ",");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
    }

    /**
     * Run Analyze on HDFS text file and check results in pg_class and pg_stats
     * tables.
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features" })
    public void analyzeOnText() throws Exception {

        // path for storing data on HDFS
        String csvPath = hdfs.getWorkingDirectory() + "/analyze_text_data.csv";

        FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

        hdfs.writeTableToFile(csvPath, dataTable, ",");

        exTable.setPath(csvPath);
        exTable.setName("analyze_ok");
        gpdb.createTableAndVerify(exTable);

        gpdb.analyze(exTable);

        verifyPgClassValues(-1, 1000, 10);
        verifyPgStatsEntries(exTable.getFields().length);

        ReportUtils.report(null, getClass(), "Sanity: table is queryable");
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                + " ORDER BY n1");
        ComparisonUtils.compareTables(exTable, dataTable, null);
    }

    /**
     * Run Analyze on HDFS sequence file and check results in pg_class and
     * pg_stats tables.
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features" })
    public void analyzeOnSequenceSmall() throws Exception {

        String path = hdfs.getWorkingDirectory()
                + "/analyze_sequence_small.tbl";

        Table smallDataTable = new Table("smallDataTable", null);

        Object[] data = FileFormatsUtils.prepareData(
                new CustomSequencePreparer(), 1000, smallDataTable);

        hdfs.writeSequenceFile(data, path);

        exTable.setName("analyze_sequence_small");
        exTable.setFields(customWritableFields);
        exTable.setPath(path);
        exTable.setDelimiter(null);
        exTable.setFormat("CUSTOM");
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(null);
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.WritableResolver");
        exTable.setDataSchema(schemaPackage + customSchemaFileName);

        gpdb.createTableAndVerify(exTable);

        gpdb.analyze(exTable);

        verifyPgClassValues(-1, 999, 10);
        verifyPgStatsEntries(exTable.getFields().length);

        ReportUtils.report(null, getClass(), "Sanity: table is queryable");
        gpdb.queryResults(exTable, "SELECT COUNT(*) FROM " + exTable.getName());

        Table countTable = new Table("count", null);
        countTable.addRow(new String[] { "999" });

        ComparisonUtils.compareTables(exTable, countTable, null);
    }

    /**
     * Negative test, ANALYZE should fail when bad fragmenter is defined.
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features" })
    public void negativeAnalyzeFailOnFragmenter() throws Exception {

        String csvPath = hdfs.getWorkingDirectory() + "/analyze_nofragmenter.csv";

        Table dataTable = new Table("dataTable", null);

        FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

        hdfs.writeTableToFile(csvPath, dataTable, ",");

        exTable.setName("analyze_nofragmenter");
        exTable.setPath(csvPath);
        exTable.setProfile(null);

        exTable.setFragmenter("IDoNotThink");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");

        gpdb.createTableAndVerify(exTable);

        try {
            gpdb.analyze(exTable);
            Assert.fail("analyze should fail without existing fragmenter defined");
        } catch (SQLWarning e) {
            ExceptionUtils.validate(
                    null,
                    e,
                    new Exception(
                            "skipping \"analyze_nofragmenter\" --- error returned:.*java.lang.ClassNotFoundException: IDoNotThink"),
                    true, true);
            e = e.getNextWarning();
            ExceptionUtils.validate(null, e,
                    new Exception("ANALYZE completed. Success: 0, Failure: 1 ("
                            + exTable.getName() + ")"), false, true);
            e = e.getNextWarning();
            Assert.assertNull(e);
        }

        ReportUtils.report(null, getClass(),
                "Expect default pg_class entries because ANALYZE estimate failed");
        verifyPgClassValues(DEFAULT_RELPAGES, DEFAULT_RELTUPLES, 0);
        ReportUtils.report(null, getClass(),
                "Expect no pg_stats entries because ANALYZE failed");
        verifyPgStatsEntries(0);
    }

    /**
     * Negative test, ANALYZE should fail when there is no file to read.
     * This test is currently disabled since it expects ip address instead of name service
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features" }, enabled = false)

    public void negativeAnalyzeFailOnEstimate() throws Exception {

        String csvPath = hdfs.getWorkingDirectory() + "/no_such_file.csv";

        exTable.setName("analyze_nopath");
        exTable.setPath(csvPath);

        gpdb.createTableAndVerify(exTable);
        try {
            gpdb.analyze(exTable);
            Assert.fail("analyze should issue a warning");
        } catch (SQLWarning e) {
            // TODO: check error message with HA environment
            ExceptionUtils.validate(
                    null,
                    e,
                    new Exception(
                            "skipping \""
                                    + exTable.getName()
                                    + "\" --- error returned: "
                                    + "remote component error \\(500\\) from '.*':  "
                                    + "type  Exception report   "
                                    + "message   org.apache.hadoop.mapred.InvalidInputException: "
                                    + "Input path does not exist: hdfs://0.0.0.0:8020/"
                                    + csvPath + ".*"), true, true);

            e = e.getNextWarning();
            ExceptionUtils.validate(null, e,
                    new Exception("ANALYZE completed. Success: 0, Failure: 1 ("
                            + exTable.getName() + ")"), false, true);
            e = e.getNextWarning();
            Assert.assertNull(e);
        }

        ReportUtils.report(null, getClass(),
                "Expect default pg_class entries because ANALYZE estimate failed");
        verifyPgClassValues(DEFAULT_RELPAGES, DEFAULT_RELTUPLES, 0);
        ReportUtils.report(null, getClass(),
                "Expect no pg_stats entries because ANALYZE failed");
        verifyPgStatsEntries(0);
    }

    /**
     * Negative test, ANALYZE should fail when no resolver is defined. The first
     * stage (tuple estimate) should pass successfully, and so the pg_class
     * values should be correct. The failure is supposed to happen in the second
     * stage (count of first fragment).
     *
     * @throws Exception test failed to run
     */
    @Test(groups = { "features" })
    public void negativeAnalyzeFailOnResolver() throws Exception {

        String csvPath = hdfs.getWorkingDirectory() + "/analyze_noresolver.csv";

        FileFormatsUtils.prepareData(new CustomTextPreparer(), 500, dataTable);

        hdfs.writeTableToFile(csvPath, dataTable, ",");

        exTable.setName("analyze_badresolver");
        exTable.setPath(csvPath);
        exTable.setProfile(null);
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("NoSuchResolver");

        gpdb.createTableAndVerify(exTable);

        try {
            gpdb.analyze(exTable);
            Assert.fail("analyze should fail on resolver");
        } catch (SQLWarning e) {

            ExceptionUtils.validate(
                    null,
                    e,
                    new Exception(
                            "skipping \"analyze_badresolver\" --- error returned: "
                                    + "remote component error \\(500\\) from '.*':  "
                                    + "type  Exception report   "
                                    + "message   java.lang.ClassNotFoundException: NoSuchResolver.*"),
                    true, true);
            e = e.getNextWarning();
            ExceptionUtils.validate(null, e,
                    new Exception("ANALYZE completed. Success: 0, Failure: 1 ("
                            + exTable.getName() + ")"), false, true);
            e = e.getNextWarning();
            Assert.assertNull(e);
        }

        ReportUtils.report(null, getClass(),
                "Expect default pg_class entries because ANALYZE estimate failed");
        verifyPgClassValues(DEFAULT_RELPAGES, DEFAULT_RELTUPLES, 0);

        ReportUtils.report(null, getClass(),
                "Expect no pg_stats entries because ANALYZE failed");
        verifyPgStatsEntries(0);

        ReportUtils.startLevel(null, getClass(),
                "Verify query also fails on bad resolver");
        try {
            gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                    + " ORDER BY n1");
            Assert.fail("query should fail on resolver");
        } catch (SQLException e) {
            ExceptionUtils.validate(
                    null,
                    e,
                    new Exception(
                            "remote component error \\(500\\) from '.*':  "
                                    + "type  Exception report   "
                                    + "message   java.lang.ClassNotFoundException: NoSuchResolver.*"),
                    true, true);
        }
        ReportUtils.stopLevel(null);
    }

    /**
     * Negative test, ANALYZE should fail when the special resolver
     * ThrowOn10000Resolver fails after processing 10,000 rows. The first stage
     * (tuple estimate) should pass successfully, and so the pg_class values
     * should be correct. The failure is supposed to happen in the second stage
     * (count of first fragment).
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features" })
    public void negativeAnalyzeFailAfter10000Rows() throws Exception {

        String dataPath = hdfs.getWorkingDirectory()
                + "/analyze_fail_after_10000.txt";

        for (int i = 0; i < 10001; i++) {
            dataTable.addRow(new String[] { "" + i, "row" + i });
        }

        hdfs.writeTableToFile(dataPath, dataTable, ",");

        exTable.setName("analyze_fail_after_10000");
        exTable.setFields(new String[] { "n1 int", "s1 text" });
        exTable.setPath(dataPath);
        exTable.setProfile(null);
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver(testPackage + throwOn10000Resolver);

        gpdb.createTableAndVerify(exTable);

        try {
            gpdb.analyze(exTable);
            Assert.fail("analyze should fail on resolver");
        } catch (SQLWarning e) {

            ExceptionUtils.validate(
                    null,
                    e,
                    new Exception(
                            "skipping \"analyze_fail_after_10000\" --- error returned: "
                                    + "transfer error \\(18\\): Transferred a partial file from '.*'.*"),
                    true, true);
            e = e.getNextWarning();
            ExceptionUtils.validate(null, e,
                    new Exception("ANALYZE completed. Success: 0, Failure: 1 ("
                            + exTable.getName() + ")"), false, true);
            e = e.getNextWarning();
            Assert.assertNull(e);
        }

        ReportUtils.report(null, getClass(),
                "Expect default pg_class entries because ANALYZE estimate failed");
        verifyPgClassValues(DEFAULT_RELPAGES, DEFAULT_RELTUPLES, 0);

        ReportUtils.report(null, getClass(),
                "Expect no pg_stats entries because ANALYZE failed");
        verifyPgStatsEntries(0);

        ReportUtils.startLevel(null, getClass(),
                "Verify query also fails on bad resolver");
        try {
            gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                    + " ORDER BY n1");
            Assert.fail("query should fail on resolver");
        } catch (SQLException e) {
            ExceptionUtils.validate(
                    null,
                    e,
                    new Exception(
                            "ERROR: transfer error \\(18\\): Transferred a partial file from '.*'"),
                    true, true);
        }
        ReportUtils.stopLevel(null);
    }

    /**
     * Negative test, ANALYZE should fail when the reject limit is breached.
     *
     * The first stage (tuple estimate) should pass successfully, and so the
     * pg_class values should be correct. The failure is supposed to happen in
     * the second stage (count of first fragment), when the new pxf table should
     * have a segment reject limit of 25 percent.
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features" })
    public void negativeAnalyzeFailOnRejectLimit() throws Exception {

        String dataPath = hdfs.getWorkingDirectory()
                + "/analyze_fail_reject_limit.txt";
        String[] fields = new String[] { "n1 int", "s1 text" };

        for (int i = 0; i < 10000; i++) {
            // inject errors
            String n1 = ((i % 3 == 0) ? "oops" : "") + i;
            dataTable.addRow(new String[] { n1, n1 });
        }

        hdfs.writeTableToFile(dataPath, dataTable, ",");

        exTable.setName("analyze_fail_reject_limit");
        exTable.setFields(fields);
        exTable.setPath(dataPath);

        exTable.setSegmentRejectLimit(5000);

        gpdb.createTableAndVerify(exTable);

        try {
            gpdb.analyze(exTable);
            Assert.fail("analyze should fail on segment reject limit 25 percent");
        } catch (SQLWarning e) {

            ExceptionUtils.validate(
                    null,
                    e,
                    new Exception(
                            "skipping \"analyze_fail_reject_limit\" --- error returned: "
                                    + "Segment reject limit reached. Aborting operation. Last error was: invalid input syntax for integer: \"oops300\", column n1"),
                    true, true);
            e = e.getNextWarning();
            ExceptionUtils.validate(null, e,
                    new Exception("ANALYZE completed. Success: 0, Failure: 1 ("
                            + exTable.getName() + ")"), false, true);
            e = e.getNextWarning();
            Assert.assertNull(e);
        }

        ReportUtils.report(null, getClass(),
                "Expect default pg_class entries because ANALYZE estimate failed");
        verifyPgClassValues(DEFAULT_RELPAGES, DEFAULT_RELTUPLES, 0);

        ReportUtils.report(null, getClass(),
                "Expect no pg_stats entries because ANALYZE failed");
        verifyPgStatsEntries(0);

        ReportUtils.startLevel(null, getClass(),
                "Verify query doesn't fail because reject limit on original table is ok");
        gpdb.queryResults(exTable, "SELECT COUNT(*) FROM " + exTable.getName());

        Table countTable = new Table("count", null);
        countTable.addRow(new String[] { "6666" });
        ComparisonUtils.compareTables(exTable, countTable, null);
        ReportUtils.stopLevel(null);
    }

    /**
     * Negative test, ANALYZE should fail when sampling the second fragment.
     *
     * The estimate stages (tuple estimate + count of first fragment) should
     * pass successfully, and so the pg_class values should be correct. The
     * failure is supposed to happen in the last stage (sampling), when
     * accessing the second fragment and reading bad data.
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features" })
    public void negativeAnalyzeFailOnSampling() throws Exception {

        String dataPath = hdfs.getWorkingDirectory()
                + "/analyze_fail_second_fragment/";
        String[] fields = new String[] { "n1 int", "s1 text" };

        for (int i = 0; i < 10000; i++) {
            dataTable.addRow(new String[] { "" + i, "" + i });
        }
        hdfs.writeTableToFile(dataPath + "/file1.txt", dataTable, ",");

        dataTable.addRow(new String[] { "oh", "no" });

        hdfs.writeTableToFile(dataPath + "/file2.txt", dataTable, ",");

        exTable.setName("analyze_fail_second_fragment");
        exTable.setFields(fields);
        exTable.setPath(dataPath);

        gpdb.createTableAndVerify(exTable);

        try {
            gpdb.analyze(exTable);
            Assert.fail("analyze should fail on data error");
        } catch (SQLWarning e) {

            ExceptionUtils.validate(null, e, new Exception(
                    "skipping \"analyze_fail_second_fragment\" --- error returned: "
                            + "invalid input syntax for integer: \"oh\""),
                    true, true);
            e = e.getNextWarning();
            ExceptionUtils.validate(null, e,
                    new Exception("ANALYZE completed. Success: 0, Failure: 1 ("
                            + exTable.getName() + ")"), false, true);
            e = e.getNextWarning();
            Assert.assertNull(e);
        }

        ReportUtils.report(null, getClass(),
                "Even though ANALYZE failed, we still have the estimate");
        ReportUtils.report(null, getClass(),
                "Estimate is based on accessor only, so no tuples are rejected!");
        verifyPgClassValues(-1, 20001, 100);

        ReportUtils.report(null, getClass(),
                "Expect no pg_stats entries because ANALYZE failed");
        verifyPgStatsEntries(0);

        ReportUtils.startLevel(null, getClass(),
                "Verify query also fails on bad resolver");
        try {
            gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                    + " ORDER BY n1");
            Assert.fail("query should fail on data error");
        } catch (SQLException e) {
            ExceptionUtils.validate(null, e, new Exception(
                    "invalid input syntax for integer: \"oh\""), true, true);
        }
        ReportUtils.stopLevel(null);
    }

    /**
     * Create a table with STATS_MAX_FRAGMENTS parameter, and check that the
     * number of returned fragments does not exceed this number when querying
     * the table.
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features", "gpdb", "hcfs", "security" })
    public void statsMaxFragmentsParameter() throws Exception {

        ProtocolEnum protocol = ProtocolUtils.getProtocol();

        String csvPath1 = hdfs.getWorkingDirectory()
                + "/analyze_check_max_fragments1.csv";
        String csvPath2 = hdfs.getWorkingDirectory()
                + "/analyze_check_max_fragments2.csv";
        String csvPath3 = hdfs.getWorkingDirectory()
                + "/analyze_check_max_fragments3.csv";
        String csvPathAll = hdfs.getWorkingDirectory()
                + "/analyze_check_max_fragments*.csv";

        Table dataTable = new Table("dataTable", null);

        FileFormatsUtils.prepareData(new CustomTextPreparer(), 10, dataTable);

        hdfs.writeTableToFile(csvPath1, dataTable, ",");
        hdfs.writeTableToFile(csvPath2, dataTable, ",");
        hdfs.writeTableToFile(csvPath3, dataTable, ",");

        exTable.setName("analyze_max_fragments");
        exTable.setPath(protocol.getExternalTablePath(hdfs.getBasePath(), csvPathAll));

        exTable.setUserParameters(new String[] {
                "STATS_MAX_FRAGMENTS=2",
                "STATS_SAMPLE_RATIO=1.00" });

        gpdb.createTableAndVerify(exTable);

        ReportUtils.report(
                null,
                getClass(),
                "Query table to check STATS_MAX_FRAGMENTS parameter - only 2 fragments should be returned.");
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                + " ORDER BY n1");

        dataTable.pumpUpTableData(2);

        ComparisonUtils.compareTables(exTable, dataTable, null);
    }

    /**
     * Create a table with STATS_SAMPLE_RATIO parameter, and check that the
     * number of returned records matches the ratio.
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features", "gpdb", "hcfs", "security" })
    public void statsSampleRatioParameter() throws Exception {

        ProtocolEnum protocol = ProtocolUtils.getProtocol();

        String csvPath = hdfs.getWorkingDirectory()
                + "/analyze_check_sample_ratio.csv";

        Table dataTable = new Table("dataTable", null);

        FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

        hdfs.writeTableToFile(csvPath, dataTable, ",");

        exTable.setName("analyze_sample_ratio");
        exTable.setPath(protocol.getExternalTablePath(hdfs.getBasePath(), csvPath));

        exTable.setUserParameters(new String[] {
                "STATS_MAX_FRAGMENTS=100",
                "STATS_SAMPLE_RATIO=0.0010" });

        gpdb.createTableAndVerify(exTable);

        ReportUtils.startLevel(
                null,
                getClass(),
                "Query table to check STATS_SAMPLE_RATIO parameter - only 1 record should be returned.");
        gpdb.queryResults(exTable, "SELECT COUNT(*) FROM " + exTable.getName());

        int countResult = Integer.parseInt(exTable.getData().get(0).get(0));
        Assert.assertTrue(almostEquals(1, countResult, 10));
        ReportUtils.stopLevel(null);

        exTable.setUserParameters(new String[] {
                "STATS_MAX_FRAGMENTS=100",
                "STATS_SAMPLE_RATIO=0.05" });

        gpdb.createTableAndVerify(exTable);

        ReportUtils.startLevel(
                null,
                getClass(),
                "Query table to check STATS_SAMPLE_RATIO parameter - only 50 records should be returned.");
        gpdb.queryResults(exTable, "SELECT COUNT(*) FROM " + exTable.getName());

        countResult = Integer.parseInt(exTable.getData().get(0).get(0));
        Assert.assertTrue(almostEquals(50, countResult, 10));
        ReportUtils.stopLevel(null);

        exTable.setUserParameters(new String[] {
                "STATS_MAX_FRAGMENTS=100",
                "STATS_SAMPLE_RATIO=0.935" });

        gpdb.createTableAndVerify(exTable);

        ReportUtils.startLevel(
                null,
                getClass(),
                "Query table to check STATS_SAMPLE_RATIO parameter - only 935 record should be returned.");
        gpdb.queryResults(exTable, "SELECT COUNT(*) FROM " + exTable.getName());

        countResult = Integer.parseInt(exTable.getData().get(0).get(0));
        Assert.assertTrue(almostEquals(935, countResult, 10));
        ReportUtils.stopLevel(null);
    }

    /**
     * Run Analyze on 1,000,000 records text file. Verifies correct behaviour
     * with ChunkReader -
     *
     * The problem is that for text format, several records are processed as one
     * chunk by {@code accessor -> resolver -> output builder}, which distorted
     * stats when treated like regular records, returned wrong sampling rate and
     * resulted in a very long process time (~8 minutes)
     *
     * The fix is to parse the chunky record in the output builder, when it is
     * gathering stats and so get correct record count.
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "features" })
    public void analyzeOnTextBig() throws Exception {

        String csvPath = hdfs.getWorkingDirectory() + "/analyze_text_big.csv";

        Table dataTable = new Table("dataTable", null);

        FileFormatsUtils.prepareData(new CustomTextPreparer(), 100000,
                dataTable);
        hdfs.writeTableToFile(csvPath, dataTable, ",");

        for (int i = 0; i < 9; i++)
            hdfs.appendTableToFile(csvPath, dataTable, ",");

        exTable.setName("analyze_text_big");
        exTable.setPath(csvPath);

        gpdb.createTableAndVerify(exTable);

        gpdb.analyze(exTable);

        verifyPgClassValues(-1, 1000000, 300000);
        verifyPgStatsEntries(exTable.getFields().length);

        ReportUtils.report(null, getClass(), "Sanity: table is queryable");
        gpdb.queryResults(exTable, "SELECT COUNT(*) FROM " + exTable.getName());

        Table countTable = new Table("count", null);
        countTable.addRow(new String[] { "1000000" });
        ComparisonUtils.compareTables(exTable, countTable, null);
    }

    /**
     * Run Analyze on 10,000,000 records sequence file.
     *
     * Note: running time ~13 minutes.
     *
     * @throws Exception if test failed to run
     */
    @Test(groups = { "load" })
    public void analyzeOnSequenceBig() throws Exception {

        String path = hdfs.getWorkingDirectory() + "/analyze_sequence_big.tbl";

        Table smallDataTable = new Table("smallDataTable", null);

        ReportUtils.startLevel(null, getClass(),
                "Preparing data of 10M records");

        Object[] smallData = FileFormatsUtils.prepareData(
                new CustomSequencePreparer(), 1000, smallDataTable);

        ReportUtils.report(null, getClass(), "Done preparing small data");
        Object[] data = new Object[10000000];
        for (int i = 0; i < 10000; i++) {
            System.arraycopy(smallData, 0, data, i * 1000, smallData.length);
        }
        ReportUtils.report(null, getClass(), "Done duplicating data");

        hdfs.writeSequenceFile(data, path);

        ReportUtils.stopLevel(null);

        exTable.setName("analyze_sequence_big");
        exTable.setFields(customWritableFields);
        exTable.setDelimiter(null);
        exTable.setFormat("CUSTOM");
        exTable.setPath(path);
        exTable.setProfile("SequenceWritable");
        exTable.setDataSchema(schemaPackage + "CustomWritable");
        exTable.setFormatter("pxfwritable_import");

        gpdb.createTableAndVerify(exTable);

        gpdb.analyze(exTable);

        verifyPgClassValues(-1, 9999999, 300000);
        verifyPgStatsEntries(exTable.getFields().length);

        ReportUtils.report(null, getClass(), "Sanity: table is queryable");
        gpdb.queryResults(exTable, "SELECT COUNT(*) FROM " + exTable.getName());

        Table countTable = new Table("count", null);
        countTable.addRow(new String[] { "9999999" });

        ComparisonUtils.compareTables(exTable, countTable, null);
    }

    /**
     * Runs query on pg_class table to extract exTable's statistics.
     *
     * @param expectedRelpages expected relpages. if -1, expect any result
     *            DIFFERENT than default (1000)
     * @param expectedReltuples expected reltuples. if -1, expect any result
     *            DIFFERENT than default (1000000)
     * @param epsilon inaccuracy factor. Checks if the expected values are close
     *            enough to the results: ||expected-result|| < epsilon
     * @throws Exception
     */
    private void verifyPgClassValues(int expectedRelpages,
                                     int expectedReltuples, int epsilon)
            throws Exception {

        ReportUtils.startLevel(null, getClass(),
                "Verify ANALYZE estimate in pg_class");

        Table analyzeResults = new Table("results", null);

        gpdb.queryResults(analyzeResults,
                "SELECT relpages::int, reltuples::int FROM pg_class WHERE relname = '"
                        + exTable.getName() + "'");

        Assert.assertTrue("pg_class should have one entry per table",
                analyzeResults.getData().size() == 1);

        String relpagesStr = analyzeResults.getData().get(0).get(0);
        int relpages = Integer.parseInt(relpagesStr);
        String reltuplesStr = analyzeResults.getData().get(0).get(1);
        int reltuples = Integer.parseInt(reltuplesStr);

        ReportUtils.report(null, getClass(), "Expected results: "
                + "relpages: "
                + ((expectedRelpages == -1) ? "<> default " + DEFAULT_RELPAGES
                        : expectedRelpages)
                + ", reltuples: "
                + ((expectedReltuples == -1) ? " <> default "
                        + DEFAULT_RELTUPLES : expectedReltuples));
        if (expectedRelpages == -1) {
            Assert.assertTrue("relpages is different than default 1000",
                    relpages != DEFAULT_RELPAGES);
        } else {
            Assert.assertTrue("abs( " + expectedRelpages + " - " + relpages
                    + ") <= " + epsilon,
                    almostEquals(expectedRelpages, relpages, epsilon));
        }
        if (expectedReltuples == -1) {
            Assert.assertTrue("reltuples is different than default 1,000,000",
                    reltuples != DEFAULT_RELTUPLES);
        } else {
            Assert.assertTrue("abs( " + expectedReltuples + " - " + reltuples
                    + ") <= " + epsilon,
                    almostEquals(expectedReltuples, reltuples, epsilon));
        }

        ReportUtils.stopLevel(null);
    }

    /**
     * Runs query on pg_stats table to extract exTable's statistics. We only
     * check that the number of records matches the number of fields in the
     * table.
     *
     * @param fieldsNum number of fields in the table
     * @throws Exception
     */
    private void verifyPgStatsEntries(int fieldsNum) throws Exception {

        ReportUtils.startLevel(null, getClass(),
                "Verify ANALYZE entries in pg_stats");

        String schema = exTable.getSchema();
        if (schema == null) {
            schema = "public";
        }

        Table analyzeResults = new Table("results", null);

        gpdb.queryResults(analyzeResults,
                "SELECT COUNT(*) FROM pg_stats WHERE schemaname = '" + schema
                        + "' AND tablename = '" + exTable.getName() + "'");

        ReportUtils.report(null, getClass(),
                "pg_stats should have " + fieldsNum + " entries for table "
                        + schema + "." + exTable.getName());

        Table countTable = new Table("count", null);
        countTable.addRow(new String[] { "" + fieldsNum });
        ComparisonUtils.compareTables(analyzeResults, countTable, null);

        ReportUtils.stopLevel(null);
    }

    private boolean almostEquals(int arg1, int arg2, int epsilon) {
        return (Math.abs(arg1 - arg2) <= epsilon);
    }
}
