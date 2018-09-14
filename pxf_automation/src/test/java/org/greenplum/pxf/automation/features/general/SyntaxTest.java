package org.greenplum.pxf.automation.features.general;

import org.greenplum.pxf.automation.features.BaseFeature;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.postgresql.util.PSQLException;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

import org.testng.Assert;

import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;

import java.sql.Types;

/**
 * Test correct syntax when creating and querying PXF tables
 */
public class SyntaxTest extends BaseFeature {

    ReadableExternalTable exTable;
    WritableExternalTable weTable;

    String hdfsWorkingFolder = "dummyLocation";
    String[] syntaxFields = new String[] { "a int", "b text", "c bytea" };

    /**
     * General Table creation Validations with Fragmenter, Accessor and Resolver
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void syntaxValidationsGood() throws Exception {

        ReportUtils.reportBold(null, getClass(),
                "Successfully create external table with required PXF parameters");

        exTable = new ReadableExternalTable("pxf_extable_validations",
                syntaxFields, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

        exTable.setFragmenter("xfrag");
        exTable.setAccessor("xacc");
        exTable.setResolver("xres");
        exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
        exTable.setFormatter("pxfwritable_import");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        gpdb.createTableAndVerify(exTable);

        exTable.setName("pxf_extable_validations1");
        exTable.setPath(hdfsWorkingFolder);
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor");
        exTable.setResolver("org.greenplum.pxf.plugin.hdfs.AvroResolver");
        exTable.setDataSchema("MySchema");
        exTable.setUserParameters(null);

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        gpdb.createTableAndVerify(exTable);
    }

    /**
     * Check Syntax validation, try to create Readable Table without PXF
     * options, expect failure and Error message.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeNoPxfParameters() throws Exception {

        ReportUtils.reportBold(
                null,
                getClass(),
                "Fail to create external table with missing or no PXF parameters: Formatter, Fragmenter, Accessor, Resolver");

        exTable = new ReadableExternalTable("pxf_extable_validations",
                syntaxFields, hdfsWorkingFolder, "CUSTOM");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        try {
            gpdb.createTable(exTable);
            Assert.fail("Table creation should fail with invalid URL error");
        } catch (Exception e) {
            String urlPort = exTable.getPort() == null ? "" : ":"
                    + exTable.getPort();
            String pxfUrl = exTable.getHost() + urlPort + "/"
                    + exTable.getPath();
            ExceptionUtils.validate(null, e, new PSQLException(
                    "ERROR: Invalid URI pxf://" + pxfUrl
                            + "?: invalid option after '?'", null), false);
        }
    }

    /**
     * Create Table with no Fragmenter, Accessor and Resolver. Should fail and
     * throw the right message.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeNoFragmenterNoAccessorNoResolver() throws Exception {

        ReportUtils.reportBold(
                null,
                getClass(),
                "Fail to create external table with no PXF parameters: Fragmenter, Accessor, Resolver");

        exTable = new ReadableExternalTable("pxf_extable_validations",
                syntaxFields, (hdfsWorkingFolder + "/*"), "CUSTOM");

        exTable.setFormatter("pxfwritable_import");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Table creation should fail with invalid URI error");
        } catch (PSQLException e) {
            String urlPort = exTable.getPort() == null ? "" : ":"
                    + exTable.getPort();
            String pxfUrl = exTable.getHost() + urlPort + "/"
                    + exTable.getPath();
            ExceptionUtils.validate(null, e, new PSQLException(
                    "ERROR: Invalid URI pxf://" + pxfUrl
                            + "?: invalid option after '?'", null), false);
        }
    }

    /**
     * Create Table with no Fragmenter. Should fail and throw the right message.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeMissingFragmenter() throws Exception {

        ReportUtils.reportBold(null, getClass(),
                "Fail to create external table with no PXF parameters: Fragmenter");

        exTable = new ReadableExternalTable("pxf_extable_validations",
                syntaxFields, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

        exTable.setAccessor("xacc");
        exTable.setResolver("xres");
        exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
        exTable.setFormatter("pxfwritable_import");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Table creation should fail with invalid URI error");
        } catch (PSQLException e) {
            String urlPort = exTable.getPort() == null ? "" : ":"
                    + exTable.getPort();
            String pxfUrl = exTable.getHost() + urlPort + "/"
                    + exTable.getPath();
            ExceptionUtils.validate(
                    null,
                    e,
                    new PSQLException(
                            "ERROR: Invalid URI pxf://"
                                    + pxfUrl
                                    + "?ACCESSOR=xacc&RESOLVER=xres&someuseropt=someuserval: PROFILE or FRAGMENTER option(s) missing",
                            null), false);
        }
    }

    /**
     * Create Table with no Accessor. Should fail and throw the right message.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeMissingAccessor() throws Exception {

        exTable = new ReadableExternalTable("pxf_extable_validations",
                syntaxFields, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

        exTable.setFragmenter("xfrag");
        exTable.setResolver("xres");
        exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
        exTable.setFormatter("pxfwritable_import");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Table creation should fail with invalid URI error");
        } catch (PSQLException e) {
            String urlPort = exTable.getPort() == null ? "" : ":"
                    + exTable.getPort();
            String pxfUrl = exTable.getHost() + urlPort + "/"
                    + exTable.getPath();
            ExceptionUtils.validate(
                    null,
                    e,
                    new PSQLException(
                            "ERROR: Invalid URI pxf://"
                                    + pxfUrl
                                    + "?FRAGMENTER=xfrag&RESOLVER=xres&someuseropt=someuserval: PROFILE or ACCESSOR option(s) missing",
                            null), false);
        }
    }

    /**
     * Create Table with no Resolver. Should fail and throw the right message.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeMissingResolver() throws Exception {

        exTable = new ReadableExternalTable("pxf_extable_validations",
                syntaxFields, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

        exTable.setFragmenter("xfrag");
        exTable.setAccessor("xacc");
        exTable.setFormatter("pxfwritable_import");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Table creation should fail with invalid URI error");
        } catch (PSQLException e) {
            String urlPort = exTable.getPort() == null ? "" : ":"
                    + exTable.getPort();
            String pxfUrl = exTable.getHost() + urlPort + "/"
                    + exTable.getPath();
            ExceptionUtils.validate(
                    null,
                    e,
                    new PSQLException(
                            "ERROR: Invalid URI pxf://"
                                    + pxfUrl
                                    + "?FRAGMENTER=xfrag&ACCESSOR=xacc: PROFILE or RESOLVER option(s) missing",
                            null), false);
        }
    }

    /**
     * Namenode High-availability test - creating table with non-existent
     * nameservice
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeHaNameserviceNotExist() throws Exception {
        String unknownNameservicePath = "text_data.csv";

        exTable = TableFactory.getPxfReadableTextTable("hatable", new String[] {
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
                "n17 int" }, (unknownNameservicePath), ",");

        exTable.setHost("unrealcluster");
        exTable.setPort(null);

        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Table creation should fail with bad nameservice error");
        } catch (Exception e) {
            ExceptionUtils.validate(
                    null,
                    e,
                    new PSQLException(
                            "ERROR: nameservice unrealcluster not found in client configuration. No HA namenodes provided",
                            null), false);
        }
    }

    /**
     * Create writable external table with accessor and resolver
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void syntaxValidationWritable() throws Exception {

        // String name, String[] fields, String path, String format
        weTable = new WritableExternalTable("pxf_writable", syntaxFields,
                hdfsWorkingFolder + "/writable", "CUSTOM");

        weTable.setAccessor("org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor");
        weTable.setResolver("org.greenplum.pxf.plugins.hdfs.AvroResolver");
        weTable.setDataSchema("MySchema");
        weTable.setFormatter("pxfwritable_export");

        weTable.setHost(pxfHost);
        weTable.setPort(pxfPort);

        gpdb.createTableAndVerify(weTable);
    }

    /**
     * Create writable table with no accessor and resolver. Should fail and
     * throw the right message.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeMissingParameterWritable() throws Exception {

        weTable = new WritableExternalTable("pxf_writable", syntaxFields,
                hdfsWorkingFolder + "/writable", "CUSTOM");
        weTable.setFormatter("pxfwritable_export");
        weTable.setUserParameters(new String[] { "someuseropt=someuserval" });

        weTable.setHost(pxfHost);
        weTable.setPort(pxfPort);

        try {
            gpdb.createTableAndVerify(weTable);
            Assert.fail("Table creation should fail with invalid URI error");
        } catch (Exception e) {
            String urlPort = weTable.getPort() == null ? "" : ":"
                    + weTable.getPort();
            String pxfUrl = weTable.getHost() + urlPort + "/"
                    + weTable.getPath();
            ExceptionUtils.validate(
                    null,
                    e,
                    new PSQLException(
                            "ERROR: Invalid URI pxf://"
                                    + pxfUrl
                                    + "?someuseropt=someuserval: PROFILE or ACCESSOR and RESOLVER option(s) missing",
                            null), false);
        }
    }

    /**
     * Create writable table with no parameters. Should fail and throw the right
     * message.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeNoParametersWritable() throws Exception {

        weTable = new WritableExternalTable("pxf_writable", syntaxFields,
                hdfsWorkingFolder + "/writable/*", "CUSTOM");
        weTable.setFormatter("pxfwritable_export");

        weTable.setHost(pxfHost);
        weTable.setPort(pxfPort);

        String createQuery = weTable.constructCreateStmt();
        createQuery = createQuery.replace("?", "");

        try {
            gpdb.runQuery(createQuery);
            Assert.fail("Table creation should fail with invalid URI error");
        } catch (Exception e) {
            String urlPort = weTable.getPort() == null ? "" : ":"
                    + weTable.getPort();
            String pxfUrl = weTable.getHost() + urlPort + "/"
                    + weTable.getPath();
            ExceptionUtils.validate(null, e, new PSQLException(
                    "ERROR: Invalid URI pxf://" + pxfUrl
                            + ": missing options section", null), false);
        }
    }

    /**
     *
     * set bad host name
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeErrorInHostName() throws Exception {

        exTable = new ReadableExternalTable("host_err", syntaxFields,
                ("somepath/" + hdfsWorkingFolder), "CUSTOM");

        exTable.setFragmenter("xfrag");
        exTable.setAccessor("xacc");
        exTable.setResolver("xres");
        exTable.setFormatter("pxfwritable_import");

        exTable.setHost("badhostname");
        exTable.setPort("5888");

        gpdb.createTableAndVerify(exTable);

        String expectedWarningNormal = "Couldn't resolve host '"
                + exTable.getHost() + "'";
        String expectedWarningSecure = "Failed to acquire a delegation token for uri hdfs://"
                + exTable.getHost();
        String expectedWarning = "(" + expectedWarningNormal + "|"
                + expectedWarningSecure + ")";

        try {
            gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName());
            Assert.fail("Query should fail with bad host name error");
        } catch (PSQLException e) {
            ExceptionUtils.validate(null, e, new PSQLException(expectedWarning,
                    null), true);
        }

        runNegativeAnalyzeTest(expectedWarning);
    }

    /**
     * Analyze should issue a warning when fragmenter class definition is wrong.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeAnalyzeHdfsFileBadClass() throws Exception {
        exTable = new ReadableExternalTable("analyze_bad_class", syntaxFields,
                ("somepath/" + hdfsWorkingFolder), "CUSTOM");

        // define and create external table
        exTable.setFragmenter("NoSuchThing");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        exTable.setFormatter("pxfwritable_import");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        String expectedWarning = "java.lang.ClassNotFoundException: NoSuchThing";

        runNegativeAnalyzeTest(expectedWarning);
    }

    private void runNegativeAnalyzeTest(String expectedWarning)
            throws Exception {
        gpdb.createTableAndVerify(exTable);

        // set pxf_enable_stat_collection=true
        gpdb.runQuery("SET pxf_enable_stat_collection = true");
        // analyze table with expected warning
        gpdb.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(),
                expectedWarning, true);

        // query results from pg_class table
        Table analyzeResults = new Table("analyzeResults", null);
        gpdb.queryResults(
                analyzeResults,
                "SELECT reltuples FROM pg_class WHERE relname='"
                        + exTable.getName() + "'");
        // prepare expected default results and verify
        Table expectedAnalyzeResults = new Table("expectedAnalyzeResults", null);
        expectedAnalyzeResults.addRow(new String[] { "1000000" });
        ComparisonUtils.compareTables(analyzeResults, expectedAnalyzeResults,
                null);

        /*
         * GPSQL-3038 - error stack was not cleaned, causing
         * "ERRORDATA_STACK_SIZE exceeded" crash
         */
        ReportUtils.startLevel(null, getClass(),
                "Repeat analyze with failure 20 times to verify correct error cleanup");

        for (int i = 0; i < 20; i++) {
            ReportUtils.report(null, getClass(), "running analyze for the "
                    + (i + 1) + "/20 time");
            gpdb.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(),
                    expectedWarning, true);
        }
        gpdb.queryResults(
                analyzeResults,
                "SELECT reltuples FROM pg_class WHERE relname='"
                        + exTable.getName() + "'");
        ComparisonUtils.compareTables(analyzeResults, expectedAnalyzeResults,
                null);

        ReportUtils.stopLevel(null);
    }

    /**
     * insert into table with bad host name
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeBadHostWritable() throws Exception {
        weTable = new WritableExternalTable("wr_host_err", new String[] {
                "t1 text",
                "a1 integer" }, hdfsWorkingFolder + "/writable/err", "TEXT");
        weTable.setDelimiter(",");
        weTable.setAccessor("TextFileWAccessor");
        weTable.setResolver("TextWResolver");

        weTable.setHost("badhostname");
        exTable.setPort("5888");

        Table dataTable = new Table("data", null);
        dataTable.addRow(new String[] { "first", "1" });
        dataTable.addRow(new String[] { "second", "2" });
        dataTable.addRow(new String[] { "third", "3" });

        gpdb.createTableAndVerify(weTable);

        try {
            gpdb.insertData(dataTable, weTable);
            Assert.fail("Insert data should fail because of wrong host name");
        } catch (PSQLException e) {
            String expectedWarningNormal = "remote component error \\(0\\): "
                    + "Couldn't resolve host '" + weTable.getHost() + "'";
            String expectedWarningSecure = "fail to get filesystem credential for uri hdfs://badhostname";
            String expectedWarning = "(" + expectedWarningNormal + "|"
                    + expectedWarningSecure + ")";
            ExceptionUtils.validate(null, e, new PSQLException(expectedWarning,
                    null), true);
        }
    }

    /**
     * Netagive test to verify that table with wrong nameservice is not created.
     * The nameservice is defined in GPDB's hdfs-client.xml file.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeHaNameserviceReadable() throws Exception {
        String unknownNameservicePath = "text_data.csv";

        exTable = TableFactory.getPxfReadableTextTable("hatable", new String[] {
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
                "n17 int" }, (unknownNameservicePath), ",");

        exTable.setHost("unrealcluster");
        exTable.setPort(null);

        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Table creation should fail with bad nameservice error");
        } catch (Exception e) {
            Assert.assertEquals(
                    "ERROR: nameservice unrealcluster not found in client configuration. No HA namenodes provided",
                    e.getMessage());
        }
    }

    /**
     * Netagive test to verify that table with wrong nameservice is not created.
     * The nameservice is defined in GPDB's hdfs-client.xml file.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeHaNameserviceWritable() throws Exception {
        String unknownNameservicePath = "text_data.csv";

        exTable = TableFactory.getPxfWritableTextTable("hatable", new String[] {
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
                "n17 int" }, (unknownNameservicePath), ",");

        exTable.setHost("unrealcluster");
        exTable.setPort(null);

        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Table creation should fail with bad nameservice error");
        } catch (Exception e) {
            Assert.assertEquals(
                    "ERROR: nameservice unrealcluster not found in client configuration. No HA namenodes provided",
                    e.getMessage());
        }
    }

    /**
     * Verify pg_remote_credentials exists and created with the expected
     * structure
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void remoteCredentialsCatalogTable() throws Exception {

        Table results = new Table("results", null);
        gpdb.queryResults(results, "SELECT * FROM pg_remote_credentials");

        Table expected = new Table("expected", null);
        expected.addColumn("rcowner", Types.BIGINT);
        expected.addColumn("rcservice", Types.VARCHAR);
        expected.addColumn("rcremoteuser", Types.VARCHAR);
        expected.addColumn("rcremotepassword", Types.VARCHAR);

        ComparisonUtils.compareTablesMetadata(expected, results);
        ComparisonUtils.compareTables(results, expected, null);
    }

    /**
     * Verify pg_remote_logins exists, created with the expected structure and
     * does not print any passwords
     *
     * pg_remote_logins is a view on top pg_remote_credentials.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void remoteLoginsView() throws Exception {
        try {
            // SETUP
            gpdb.runQuery("SET allow_system_table_mods = 'DML';");
            gpdb.runQuery("INSERT INTO pg_remote_credentials VALUES (10, 'a', 'b', 'c');");

            // TEST
            Table results = new Table("results", null);
            gpdb.queryResults(results, "SELECT * FROM pg_remote_logins");

            // COMPARISON
            String aclUser = gpdb.getUserName() == null ? System.getProperty("user.name")
                    : gpdb.getUserName();
            Table expected = new Table("expected", null);
            expected.addColumn("rolname", Types.VARCHAR);
            expected.addColumn("rcservice", Types.VARCHAR);
            expected.addColumn("rcremoteuser", Types.VARCHAR);
            expected.addColumn("rcremotepassword", Types.VARCHAR);
            expected.addRow(new String[] { aclUser, "a", "b", "********" });

            ComparisonUtils.compareTablesMetadata(expected, results);
            ComparisonUtils.compareTables(results, expected, null);
        } finally {
            // CLEANUP
            gpdb.runQuery("DELETE FROM pg_remote_credentials WHERE rcowner = 10;");
            gpdb.runQuery("SET allow_system_table_mods = 'NONE';");
        }
    }

    /**
     * Verify pg_remote_credentials has the correct ACLs
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void remoteCredentialsACL() throws Exception {

        // TEST
        Table results = new Table("results", null);
        gpdb.queryResults(results,
                "SELECT relacl FROM pg_class WHERE relname = 'pg_remote_credentials'");

        // COMPARISON
        String aclUser = gpdb.getUserName() == null ? System.getProperty("user.name")
                : gpdb.getUserName();
        Table expected = new Table("expected", null);
        expected.addColumnHeader("relacl");
        expected.addColDataType(Types.ARRAY);
        expected.addRow(new String[] { "{" + aclUser + ":arwdxt/" + aclUser
                + "}" });

        ComparisonUtils.compareTablesMetadata(expected, results);
        ComparisonUtils.compareTables(results, expected, null);
    }

    /**
     * Verify table creation fails when using the HEADER option
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeHeaderOption() throws Exception {
        ReportUtils.reportBold(null, getClass(),
                "Fail to create external table with HEADER option");

        exTable = new ReadableExternalTable("pxf_extable_header", syntaxFields,
                ("somepath/" + hdfsWorkingFolder), "TEXT");

        exTable.setFragmenter("xfrag");
        exTable.setAccessor("xacc");
        exTable.setResolver("xres");
        exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        String sqlCmd = exTable.constructCreateStmt();
        sqlCmd += " (HEADER)"; // adding the HEADER option

        try {
            gpdb.runQuery(sqlCmd);
            Assert.fail("Table creation should fail with invalid option error");
        } catch (PSQLException e) {
            ExceptionUtils.validate(
                    null,
                    e,
                    new PSQLException(
                            "ERROR: HEADER option is not allowed in a PXF external table",
                            null), false);
        }
    }

    /**
     * Test querying tables with plugins specifying the old package name
     * "com.pivotal.pxf" results in an error message recommending using the new
     * plugin package name "org.gpdb.pxf"
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeOldPackageNameReadable() throws Exception {

        exTable = new ReadableExternalTable("pxf_extable", syntaxFields,
                ("somepath/" + hdfsWorkingFolder), "CUSTOM");

        exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor");
        exTable.setResolver("org.greenplum.pxf.plugin.hdfs.AvroResolver");
        exTable.setFormatter("pxfwritable_import");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        negativeOldPackageCheck(
                false,
                "java.lang.Exception: Class com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter "
                        + "does not appear in classpath. Plugins provided by PXF must "
                        + "start with &quot;org.greenplum.pxf&quot;",
                "Query should fail because the fragmenter is wrong");
    }

    /**
     * Test inserting data into tables with plugins specifying the old package
     * name "com.pivotal.pxf" results in an error message recommending using the
     * new plugin package name "org.gpdb.pxf"
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void negativeOldPackageNameWritable() throws Exception {

        weTable = new WritableExternalTable("pxf_writable", syntaxFields,
                hdfsWorkingFolder + "/writable", "CUSTOM");

        weTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
        weTable.setResolver("org.greenplum.pxf.plugins.hdfs.AvroResolver");
        weTable.setDataSchema("MySchema");
        weTable.setFormatter("pxfwritable_export");

        weTable.setHost(pxfHost);
        weTable.setPort(pxfPort);

        negativeOldPackageCheck(
                true,
                "java.lang.Exception: Class com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor "
                        + "does not appear in classpath. Plugins provided by PXF must "
                        + "start with &quot;org.greenplum.pxf&quot;",
                "Insert should fail because the accessor is wrong");

        weTable.setAccessor("org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor");
        weTable.setResolver("com.pivotal.pxf.plugins.hdfs.AvroResolver");

        negativeOldPackageCheck(
                true,
                "java.lang.Exception: Class com.pivotal.pxf.plugins.hdfs.AvroResolver "
                        + "does not appear in classpath. Plugins provided by PXF must "
                        + "start with &quot;org.greenplum.pxf&quot;",
                "Insert should fail because the resolver is wrong");

    }

    private void negativeOldPackageCheck(boolean isWritable,
                                         String expectedError, String reason)
            throws Exception {
        Table dataTable = new Table("data", syntaxFields);
        dataTable.addRow(new String[] { "1", "2", "3" });

        gpdb.createTableAndVerify(isWritable ? weTable : exTable);
        try {
            if (isWritable) {
                gpdb.insertData(dataTable, weTable);
            } else {
                gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName());
            }
            Assert.fail(reason);
        } catch (Exception e) {
            ExceptionUtils.validate(null, e,
                    new Exception(expectedError, null), true, true);
        }
    }
}
