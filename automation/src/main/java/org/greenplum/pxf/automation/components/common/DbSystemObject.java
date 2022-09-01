package org.greenplum.pxf.automation.components.common;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import jsystem.framework.report.Reporter;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.postgresql.util.PSQLException;

import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * System object for interacting with a DB via JDBC. Every System Object that represents JDBC DB
 * should extend from it.
 */
public abstract class DbSystemObject extends BaseSystemObject implements IDbFunctionality {
	protected String db;
	protected String host;
	protected String masterHost;
	protected String port;
	protected String userName;
    protected String kerberosPrincipal;
	protected String driver;
	protected String address;
	protected String encoding;
	protected String localeCollate;
	protected String localeCollateType;
	protected Connection dbConnection;
	protected Statement stmt;
	// retries for create JDBC connection
	private int connectionRetries = 10;
	private static final long RETRY_INTERVAL = 1000 * 10;

	public DbSystemObject() {
	}

	public DbSystemObject(boolean silentReport) {
		super(silentReport);
	}

	/**
	 * Create JDBC connection to Data Base
	 *
	 * @throws Exception
	 */
	public void connect() throws Exception {
		ReportUtils.startLevel(report, getClass(), "Connect");
		if (stmt == null) {
			int retries = 1;
			boolean connectionEstablished = false;
			while ((retries <= connectionRetries) && (!connectionEstablished)) {
				ReportUtils.report(report, getClass(), "Attempting to connect to: " + address + " (attempt " + retries + ")");
				// load drive to classpath
				Class.forName(driver);
				Properties props = new Properties();
				// set userName if exists
				if (userName != null) {
					props.setProperty("user", userName);
				}
				ReportUtils.report(report, getClass(), "user:" + userName);

				// if connection establishment fails, increment retries and proceed to next try
				try {
					dbConnection = DriverManager.getConnection(address, props);
					stmt = dbConnection.createStatement();
				} catch (Exception e) {
					retries++;
					ReportUtils.report(report, getClass(), e.getMessage());
					Thread.sleep(RETRY_INTERVAL);
					continue;
				}

				connectionEstablished = true;

				ReportUtils.report(report, getClass(), "Connection established to: " + address);
			}

			// if after all attempts no connection established, throw exception
			if (!connectionEstablished) {
				throw new Exception("Error establishing JDBC connection to " + address + " after " + connectionRetries + " attempts");
			}
		}

		ReportUtils.stopLevel(report);
	}

	@Override
	public void createTable(Table table) throws Exception {

		runQuery(table.constructCreateStmt());

	}

	/**
	 * Run analytic query and compare the result with expectedResult, fail if not match.
	 *
	 * @param query analytic query
	 * @param expectedResult to match the query result
	 * @throws Exception
	 */
	public void runAnalyticQuery(String query, String expectedResult) throws Exception {
		Table analyticResult = new Table("analyticResult", null);
		queryResults(analyticResult, query);

		String result = analyticResult.getData().get(0).get(0);
		System.out.println(result);

		String reportMessage = "Analytic Query returned: " + result + " expected result: " + expectedResult;
		int status = Reporter.PASS;

		if (!result.equals(expectedResult)) {
			status = Reporter.FAIL;
		}

		ReportUtils.report(report, this.getClass(), reportMessage, status);
	}

	@Override
	public void dropTable(Table table, boolean cascade) throws Exception {
		runQuery(table.constructDropStmt(cascade), true, false);
	}

	@Override
	public void dropDataBase(String schemaName, boolean cascade, boolean ignoreFail) throws Exception {
		runQuery("DROP SCHEMA " + schemaName + ((cascade) ? " CASCADE" : ""), ignoreFail, false);
	}

	@Override
	public void insertData(Table source, Table target) throws Exception {
		StringBuilder dataStringBuilder = new StringBuilder();
		List<List<String>> data = source.getData();
		int dataSize = data.size();

		for (int i = 0; i < dataSize; i++) {
			List<String> row = data.get(i);
			dataStringBuilder.append("(");

			int rowSize = row.size();
			for (int j = 0; j < rowSize; j++) {
				dataStringBuilder.append("E'").append(row.get(j)).append("'");
				if (j != rowSize - 1) {
					dataStringBuilder.append(",");
				}
			}
			dataStringBuilder.append(")");
			if (i != dataSize - 1) {
				dataStringBuilder.append(",");
			}
		}
		runQuery("INSERT INTO " + target.getName() + " VALUES " + dataStringBuilder);
	}

	@Override
	public void createDataBase(String schemaName, boolean ignoreFail) throws Exception {

		runQuery("CREATE SCHEMA " + schemaName, ignoreFail, false);

	}

	/**
	 * Run execute query (Create, Drop, Update)
	 *
	 * @param query
	 * @throws Exception
	 */
	public void runQuery(String query) throws Exception {
		runQuery(query, false, false);
	}

	/**
	 * Run query and measure elapsed time in ms
	 *
	 * @param query
	 * @return elapsed time in ms
	 * @throws Exception
	 */
	public long runQueryTiming(String query) throws Exception {
	    long startTimeInMillis = System.currentTimeMillis();
	    runQuery(query, false, true);
	    return System.currentTimeMillis() - startTimeInMillis;
	}

	/**
	 * Run query which expected to get warning in execution and match it to expected one.
	 *
	 * @param query to run
	 * @param expectedWarning to match to returned warning
	 * @param isRegex if true refer to expectedWarning as regular expression
	 * @param ignoreNoWarning if no warning returned at all
	 * @throws Exception
	 */
	public void runQueryWithExpectedWarning(String query, String expectedWarning, boolean isRegex, boolean ignoreNoWarning) throws Exception {

		runQuery(query, true, false);

		verifyWarning(expectedWarning, isRegex, ignoreNoWarning);
	}

	/**
	 * Run query which expected to get warning in execution and match it to expected one.
	 *
	 * @param query to run
	 * @param expectedWarning to match to returned warning
	 * @param isRegex if true refer to expectedWarning as regular expression
	 * @throws Exception
	 */
	public void runQueryWithExpectedWarning(String query, String expectedWarning, boolean isRegex) throws Exception {

		runQueryWithExpectedWarning(query, expectedWarning, isRegex, false);
	}

	/**
	 * Run single execute query.
	 *
	 * @param query string query
	 * @param ignoreFail if true will ignore query execution failure.
	 * @param fetchResultSet fetch the whole result set, iterate over all records.
	 * @throws Exception
	 */
	public void runQuery(String query, boolean ignoreFail, boolean fetchResultSet) throws Exception {

		ReportUtils.startLevel(report, getClass(), query);

		try {
			long startTimeInMillis = System.currentTimeMillis();
            if (fetchResultSet) {
                ResultSet rs = stmt.executeQuery(query);
                while (rs.next()) {
                    // Empty loop to scan entire resultset
                }
            } else {
                stmt.execute(query);
            }
			ReportUtils.report(report, getClass(), "Took " + (System.currentTimeMillis() - startTimeInMillis) + " milliseconds");

			if (stmt.getWarnings() != null) {
				throw stmt.getWarnings();
			}
		} catch (PSQLException e) {
			throw e;
		} catch (SQLException e) {
			if (!ignoreFail) {
				throw e;
			}
		} catch (Exception e) {
		    throw e;
		} finally {
			ReportUtils.stopLevel(report);
		}
	}

	/**
	 * Execute give query, If table is not null, the returned data will be stored in the table data.
	 *
	 * @param table to sore results
	 * @param query to run
	 * @throws Exception
	 */
	@Override
	public void queryResults(Table table, String query) throws Exception {
		ReportUtils.startLevel(report, getClass(), "Query results: " + query);
		try {
			long startTimeInMillis = System.currentTimeMillis();
			ResultSet res = stmt.executeQuery(query);
			ReportUtils.report(report, getClass(), "Took " + (System.currentTimeMillis() - startTimeInMillis) + " milliseconds");
			// if table exists store the data and meta data in it
			if (table != null) {
				table.initDataStructures();
				loadMetadata(table, res);
				loadData(table, res);
			}
			ReportUtils.reportHtml(report, getClass(), table.getDataHtml());
		} catch (Exception e) {
			throw e;
		} finally {
			ReportUtils.stopLevel(report);
		}
	}

	@Override
	public boolean checkTableExists(Table table) throws Exception {
		ResultSet res = getTablesForSchema(table);
		if (res.next()) {
			return true;
		}
		return false;
	}

	@Override
	public ArrayList<String> getTableList(String schema) throws Exception {
		ReportUtils.startLevel(report, getClass(), "Get All Tables for schema: " + schema);
		ArrayList<String> list = new ArrayList<String>();
		ResultSet res = getTablesForSchema(null);
		while (res.next()) {
			list.add(res.getString(3));
		}
		ReportUtils.stopLevel(report);
		return list;
	}

	/**
	 * Get Tables {@link ResultSet} for connection's DataBase (Schema)
	 *
	 * @param table if null return all tables in DataBase, if specific table give, return only this
	 *            table if exists
	 * @return {@link ResultSet}
	 * @throws SQLException
	 */
	protected ResultSet getTablesForSchema(Table table) throws SQLException {
		DatabaseMetaData metaData = dbConnection.getMetaData();
		String tableName = "";
		if (table != null) {
			tableName = table.getName();
		}
		return metaData.getTables(null, null, "%" + tableName, new String[] { "TABLE", "FOREIGN TABLE" });
	}

	@Override
	public void close() {
		if (dbConnection != null) {
			try {
				// close connection
				dbConnection.close();
				// init dbConnection and stmt
				dbConnection = null;
				stmt = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		super.close();
	}

	/**
	 * Get warning from SQL statement and verify against expectedWarningMessage
	 *
	 * @param expectedWarningMessage
	 * @param isRegex if true refer to expectedWarningMessage as regular expression.
	 * @param ignoreNoWarning if true ignore case of no warning returned
	 * @throws SQLException
	 * @throws IOException
	 */
	public void verifyWarning(String expectedWarningMessage, boolean isRegex, boolean ignoreNoWarning) throws SQLException, IOException {
		if (stmt.getWarnings() == null) {
			if (!ignoreNoWarning) {
				ReportUtils.report(report, getClass(), "Expected Warning: " + expectedWarningMessage + ": No Warnings thrown", Reporter.WARNING);
			}
			return;
		}
		ExceptionUtils.validate(report, stmt.getWarnings(), new Exception(expectedWarningMessage), isRegex, true);
	}

	@Override
	public void createTableAndVerify(Table table) throws Exception {
		ReportUtils.startLevel(report, getClass(), "Create and Verify Table: " + table.getFullName());
		try {
			dropTable(table, true);
			createTable(table);
		} catch (Exception e) {
			ReportUtils.stopLevel(report);
			throw e;
		}
		if (!checkTableExists(table)) {
			ReportUtils.stopLevel(report);
			throw new Exception("Table " + table.getName() + " do not exists");
		}
		ReportUtils.stopLevel(report);
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getHost() {
		return host;
	}

	public String getMasterHost() {
		return masterHost;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setMasterHost(String masterHost) {
		this.masterHost = masterHost;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public String getLocaleCollate() {
		return localeCollate;
	}

	public void setLocaleCollate(String localeCollate) {
		this.localeCollate = localeCollate;
	}

	public String getLocaleCollateType() {
		return localeCollateType;
	}

	public void setLocaleCollateType(String localeCollateType) {
		this.localeCollateType = localeCollateType;
	}

	@Override
	public boolean checkDataBaseExists(String dbName) throws Exception {

		return getDataBasesList().contains(dbName);
	}

	@Override
	public ArrayList<String> getDataBasesList() throws Exception {
		ResultSet resultSet = dbConnection.getMetaData().getCatalogs();
		ArrayList<String> dbList = new ArrayList<String>();

		while (resultSet.next()) {

			String databaseName = resultSet.getString(1);

			dbList.add(databaseName);
		}
		return dbList;
	}

    @Override
    public void grantReadOnTable(Table table, String user) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantWriteOnTable(Table table, String user) throws Exception {
        throw new UnsupportedOperationException();
    }

    public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	/**
	 * Store Column Data type and Name in given Table
	 *
	 * @param table to store meta data into.
	 * @param result {@link ResultSet} of query
	 * @throws Exception
	 */
	private void loadMetadata(Table table, ResultSet result) throws Exception {
		for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
			ResultSetMetaData metadata = result.getMetaData();
			table.addColDataType(metadata.getColumnType(i));
			table.addColumnHeader(metadata.getColumnName(i));
		}
	}

	/**
	 * Store Data into given table form {@link ResultSet}
	 *
	 * @param table to store data into.
	 * @param result {@link ResultSet} of query
	 * @throws Exception
	 */
	private void loadData(Table table, ResultSet result) throws Exception {
		while (result.next()) {
			List<String> row = new ArrayList<String>();
			for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
				row.add(result.getString(i));
			}
			table.addRow(row);
		}
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public int getConnectionRetries() {
		return connectionRetries;
	}

	public void setConnectionRetries(int connectionRetries) {
		this.connectionRetries = connectionRetries;
	}

    public String getKerberosPrincipal() {
        return this.kerberosPrincipal;
    }

    public void setKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
    }
}
