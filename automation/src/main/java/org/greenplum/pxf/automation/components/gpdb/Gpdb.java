package org.greenplum.pxf.automation.components.gpdb;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.components.common.DbSystemObject;
import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.springframework.util.Assert;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * GPDB system object, defines functionality for GPDB Data Base.
 */
public class Gpdb extends DbSystemObject {

	private static final String DEFAULT_PORT = "5432";
	private static final String GREENPLUM_DATABASE_PREFIX = "Greenplum Database ";
	private static final String IF_NOT_EXISTS_OPTION = "IF NOT EXISTS";

	private String sshUserName;
	private String sshPassword;
	private int version;

	public Gpdb() {
	}

	public Gpdb(boolean silenceReport) {
		super(silenceReport);
	}

	@Override
	public void init() throws Exception {

		super.init();

		ReportUtils.startLevel(report, getClass(), "Init");

		/**
		 * Determine the port
		 */
		initPort();

		/**
		 * Connect using default "template1" database for creating the required database and
		 * connecting it.
		 */
		driver = "org.postgresql.Driver";
		connectToDataBase("template1");
		version = determineVersion();

		if (!checkDataBaseExists(getDb())) {
			String encoding = getEncoding();
			String localeCollate = getLocaleCollate();
			String localeCollateType = getLocaleCollateType();

			if (StringUtils.isNotBlank(encoding)
					&& StringUtils.isNotBlank(localeCollate)
					&& StringUtils.isNotBlank(localeCollateType)) {
				createDataBase(getDb(), true, encoding, localeCollate, localeCollateType);
			} else {
				createDataBase(getDb(), true);
			}
		}

		connectToDataBase(getDb());

		// Create the extensions if they don't exist
		String extensionName = FDWUtils.useFDW ? "pxf_fdw" : "pxf";
		createExtension(extensionName, true);

		if (FDWUtils.useFDW) {
			createTestFDW(true);
			createForeignServers(true);
		}

		ReportUtils.stopLevel(report);
	}

	public int getVersion() {
		return version;
	}

	/**
	 * if the port is in the sut, use it, else check PGPORT var env, if not put the default one.
	 */
	private void initPort() {

		if (port == null) {

			String envPort = System.getenv("PGPORT");

			if (StringUtils.isNumeric(envPort)) {
				port = envPort;
			} else {
				port = DEFAULT_PORT;
			}
		}
	}

	/**
	 * Copies data from source table into target table
	 * @param source source table
	 * @param target target table
	 * @throws Exception if the operation fails
	 */
	public void copyData(Table source, Table target) throws Exception {

		copyData(source, target, false);
	}

	/**
	 * Copies data from source table into target table
	 * @param sourceName name of the source table
	 * @param target target table
	 * @throws Exception if the operation fails
	 */
	public void copyData(String sourceName, Table target) throws Exception {

		copyData(sourceName, target, false);
	}

	/**
	 * Copies data from source table into target table
	 * @param sourceName name of the source table
	 * @param target target table
	 * @param columns columns to select from the source table, if null then all columns will be selected
	 * @throws Exception if the operation fails
	 */
	public void copyData(String sourceName, Table target, String[] columns) throws Exception {

		copyData(sourceName, target, columns,false, null);
	}

	/**
	 * Copies data from source table into target table
	 * @param sourceName name of the source table
	 * @param target target table
	 * @param columns columns to select from the source table, if null then all columns will be selected
	 * @param context extra context to add before the SQL query
	 * @throws Exception if the operation fails
	 */
	public void copyData(String sourceName, Table target, String[] columns, String context) throws Exception {

		copyData(sourceName, target, columns, false, context);
	}

	/**
	 * Copies data from source table into target table
	 * @param source source table
	 * @param target target table
	 * @param ignoreFail whether to ignore any failures
	 * @throws Exception if the operation fails
	 */
	public void copyData(Table source, Table target, boolean ignoreFail) throws Exception {
		copyData(source.getName(), target, ignoreFail);
	}

	/**
	 * Copies data from source table into target table
	 * @param sourceName name of the source table
	 * @param target target table
	 * @param ignoreFail whether to ignore any failures
	 * @throws Exception if the operation fails
	 */
	public void copyData(String sourceName, Table target, boolean ignoreFail) throws Exception {
		copyData(sourceName, target, null, ignoreFail, null);
	}

	/**
	 * Copies data from the source table into the target table with a possibility of selecting specific columns.
	 * If the columns are specified, the schema of the target table should correspond to the projection achieved
	 * by specifying the columns. If the columns are not provided, all columns are selected from the source table.
	 * @param sourceName name of the source table
	 * @param target target table
	 * @param columns columns to select from the source table, if null then all columns will be selected
	 * @param ignoreFail whether to ignore any failures
	 * @param context extra context to add before the SQL query
	 * @throws Exception if the operation fails
	 */
	public void copyData(String sourceName, Table target, String[] columns, boolean ignoreFail, String context) throws Exception {
		String columnList = (columns == null || columns.length == 0) ? "*" : String.join(",", columns);
		String query = String.format("%sINSERT INTO %s SELECT %s FROM %s",
				StringUtils.isBlank(context) ? "" : context + "; " ,target.getName(), columnList, sourceName);
		if (target instanceof ExternalTable) {
			runQueryInsertIntoExternalTable(query);
		} else {
			runQuery(query, ignoreFail, false);
		}
	}

	public void connectToDataBase(String dbName) throws Exception {
		super.close();
		address = "jdbc:postgresql://" + getHost() + ":" + getPort() + "/" + dbName;
		connect();
	}

	@Override
	public void createDataBase(String schemaName, boolean ignoreFail) throws Exception {

		runQuery("CREATE DATABASE " + schemaName, ignoreFail, false);
		setDatabaseLevelGUCsForTesting(schemaName, ignoreFail);
	}

	@Override
	public void createDataBase(String schemaName, boolean ignoreFail, String encoding, String localeCollate, String localeCollateType) throws Exception {

		String createStatement;
		if (version == 5) {
			// Greenplum 5
			ReportUtils.startLevel(report, getClass(), "Unable to create database with encoding that does not match server's locale in Greenplum 5");
			createStatement = String.format("CREATE DATABASE %s", schemaName);
		} else {
			createStatement = String.format("CREATE DATABASE %s TEMPLATE = template0 ENCODING = '%s' LC_COLLATE = '%s' LC_CTYPE = '%s'",
					schemaName, encoding, localeCollate, localeCollateType);
		}

		runQuery(createStatement, ignoreFail, false);
		setDatabaseLevelGUCsForTesting(schemaName, ignoreFail);
	}

	private void setDatabaseLevelGUCsForTesting(String schemaName, boolean ignoreFail) throws Exception {

		runQuery("ALTER DATABASE " + schemaName + " SET bytea_output TO 'escape'", ignoreFail, false);

		// This GUC has a default value of 1 in PG12 (GPDB7) and thus the columns of type REAL display one digit extra.
		// So to keep the behavior consistent with previous version, we're setting this GUC value to 0.
		if (version >= 7) {
			runQuery("ALTER DATABASE " + schemaName + " SET extra_float_digits=0", ignoreFail, false);
		}
	}

	private void createExtension(String extensionName, boolean ignoreFail) throws Exception {
		runQuery("CREATE EXTENSION IF NOT EXISTS " + extensionName, ignoreFail, false);
	}

	private void createTestFDW(boolean ignoreFail) throws Exception {
		runQuery("DROP FOREIGN DATA WRAPPER IF EXISTS test_pxf_fdw CASCADE", ignoreFail, false);
		runQuery("CREATE FOREIGN DATA WRAPPER test_pxf_fdw HANDLER pxf_fdw_handler " +
				 "VALIDATOR pxf_fdw_validator OPTIONS (protocol 'test', mpp_execute 'all segments')",
				ignoreFail, false);
	}

	private void createForeignServers(boolean ignoreFail) throws Exception {
		List<String> servers = Lists.newArrayList(
		"default_hdfs",
		"default_hive",
		"db-hive_jdbc", // Needed for JdbcHiveTest
		"default_hbase",
		"default_jdbc", // Needed for JdbcHiveTest and other JdbcTest which refers to the default server.
		"database_jdbc",
		"db-session-params_jdbc",
		"default_file",
		"default_s3",
		"default_gs",
		"default_adl",
		"default_wasbs",
		"s3_s3",
		"s3-invalid_s3",
		"s3-non-existent_s3",
		"hdfs-non-secure_hdfs",
		"hdfs-secure_hdfs",
		"hdfs-ipa_hdfs",
		"default_test");

		// version below GP7 do not have IF EXISTS / IF NOT EXISTS command options for foreign SERVER creation
		String option = (version < 7) ? "" : IF_NOT_EXISTS_OPTION;
		for (String server : servers) {
			String foreignServerName = server.replace("-", "_");
			if (version < 7 && serverExists(foreignServerName)) {
				continue;
			}

			String pxfServerName = server.substring(0,server.lastIndexOf("_")); // strip protocol at the end
			String fdwName = server.substring(server.lastIndexOf("_") + 1) + "_pxf_fdw"; // strip protocol at the end
			runQuery(String.format("CREATE SERVER %s %s FOREIGN DATA WRAPPER %s OPTIONS(config '%s')",
					option, foreignServerName, fdwName, pxfServerName), ignoreFail, false);
			runQuery(String.format("CREATE USER MAPPING %s FOR CURRENT_USER SERVER %s", option, foreignServerName),
					ignoreFail, false);
		}
	}

	@Override
	public void dropDataBase(String dbName, boolean cascade, boolean ignoreFail) throws Exception {

		if (checkDataBaseExists(dbName)) {

			runQuery("DROP DATABASE IF EXISTS \"" + dbName + "\"", ignoreFail, false);
		}
	}

	/**
	 * Opens a psql connection to getDb() The connection should be closed using closePsql()
	 *
	 * @return shell connection with open psql session
	 * @throws Exception
	 */
	public ShellSystemObject openPsql() throws Exception {

		ReportUtils.report(report, getClass(), "Opening psql connection");
		ShellSystemObject sso = new ShellSystemObject(report.isSilent());

		sso.setHost(getHost());
		sso.setMasterHost(getMasterHost());
		sso.setUserName(getSshUserName());
		sso.setPassword(getSshPassword());

		sso.init();

		sso.runCommand("source $GPHOME/greenplum_path.sh");
		// psql do not return error code so use EXIT_CODE_NOT_EXISTS
		sso.runCommand("psql " + getDb(), ShellSystemObject.EXIT_CODE_NOT_EXISTS);

		return sso;
	}

	/**
	 * disconnects from the given ShellSystemObject. Should be used after calling openPsql();
	 *
	 * @param sso psql connection object
	 */
	public void closePsql(ShellSystemObject sso) {
		if (sso == null)
			return;
		ReportUtils.report(report, getClass(), "Closing psql connection");
		sso.disconnect();
	}

	/**
	 * Runs sql command on the given psql connection, and returns the command's result.
	 *
	 * @param sso psql connection
	 * @param sql sql command to run
	 * @param checkErrors if true assert that there is no ERROR in result
	 * @return sql command's result
	 * @throws Exception
	 */
	public String runSqlCmd(ShellSystemObject sso, String sql, boolean checkErrors) throws Exception {

		if (sso == null)
			return null;
		ReportUtils.report(report, getClass(), "running sql command " + sql);
		// sql commands do not return error code so using EXIT_CODE_NOT_EXISTS
		sso.runCommand(sql, ShellSystemObject.EXIT_CODE_NOT_EXISTS);

		String lastCmd = sso.getLastCmdResult();
		ReportUtils.report(report, getClass(), "sql command status: " + lastCmd);
		if (checkErrors) {
			Assert.doesNotContain(lastCmd, "ERROR");
		}

		return lastCmd;
	}

	/**
	 * Copy data from "from" table to "to" table using cli STDIN
	 *
	 * @param from copy from required Table data
	 * @param to copy to required table
	 * @param delim delimiter
	 * @param csv is csv format - if it is, delimiter is not used.
	 * @throws Exception
	 */
	public void copyFromStdin(Table from, Table to, String delim, boolean csv) throws Exception {

		String delimeter = delim;
		if (delimeter == null) {
			if (csv) {
				// default delimiter for CSV is ','
				delimeter = ",";
			} else {
				// default delimiter for TEXT is '\t'
				delimeter = "\t";
			}
		}

		StringBuilder dataStringBuilder = new StringBuilder();
		List<List<String>> data = from.getData();
		for (int i = 0; i < data.size(); i++) {
			List<String> row = data.get(i);
			for (int j = 0; j < row.size(); j++) {
				dataStringBuilder.append(row.get(j));
				if (j != row.size() - 1) {
					dataStringBuilder.append(delimeter);
				}
			}
			dataStringBuilder.append("\n");
		}
		dataStringBuilder.append("\\.");

		copyWithOptionalCTAS("STDIN", to, dataStringBuilder.toString(), delim, null, csv);
	}

	/**
	 * Copy data file to "to" table from file "path"
	 *
	 * @param to copy to required table
	 * @param path file to copy
	 * @param delim delimiter
	 * @param csv is csv format - if it is, delimiter is not used.
	 * @throws Exception
	 */
	public void copyFromFile(Table to, File path, String delim, boolean csv) throws Exception {
		String from = "'" + path.getAbsolutePath() + "'";
		copyLocalFileToRemoteGpdb(from);
		copyWithOptionalCTAS(from, to, null, delim, null, csv);
	}

	private void copyWithOptionalCTAS(String from, Table to, String dataToCopy, String delim, String nullChar, boolean csv) throws Exception {
		// COPY TO <foreign table> is not supported in PXF FDW with GP6, so we will have to do a workaround by
		// creating a native table, copying data from the file into it and then performing a CTAS into the foreign table
		if (FDWUtils.useFDW && getVersion() < 7) {
			Table nativeTable = createTableLike(to.getName() + "_native", to);
			// copy data into the native table
			copy(nativeTable.getName(), from, dataToCopy, delim, null, csv);
			// CTAS into the foreign table
			copyData(nativeTable, to, true);
		} else {
			copy(to.getName(), from, dataToCopy, delim, null, csv);
		}
	}

	private void copyLocalFileToRemoteGpdb(String from) throws Exception {
		// copy file to remote host if GPDB is not on localhost
		if (masterHost.equals("localhost")) {
			return;
		}
		ShellSystemObject remoteConn = new ShellSystemObject();
		remoteConn.init();
		String user = sshUserName == null ? System.getProperty("gpdbUser", System.getenv("USER")) : sshUserName;
		// first create the directory then ship the files over
		remoteConn.runRemoteCommand(user, sshPassword, masterHost, "mkdir -p " + from.replaceFirst("(/[^/]*/?)'$", "'"));
		remoteConn.copyToRemoteMachine(user, sshPassword, masterHost, from, from);
	}

	/**
     * Copy data file to "to" table from file "path"
     *
     * @param to copy to required table
     * @param path file to copy
     * @param delim delimiter
     * @param nullChar null symbol character
     * @param csv is csv format - if it is, delimiter is not used.
     * @throws Exception
     */
	public void copyFromFile(Table to, File path, String delim, String nullChar, boolean csv) throws Exception {
		String from = "'" + path.getAbsolutePath() + "'";
		copyLocalFileToRemoteGpdb(from);
		copy(to.getName(), from, null, delim, nullChar, csv);
	}

	private void copy(String to, String from, String dataToCopy, String delim, String nullChar, boolean csv) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Copy from " + from + " to " + to);

		String copyParams = "";
		if (delim != null) {
			if (!delim.startsWith("E")) {
				delim = "'" + delim + "'";
			}

			copyParams += "DELIMITER " + delim + " ";
		}
		if (nullChar != null) {
		    copyParams += "NULL as " + nullChar + " ";
		}
		if (csv) {
			copyParams += "CSV ";
		}
		String copyCmd = "\\COPY " + to + " FROM " + from + " " + copyParams + ";";

		ShellSystemObject sso = openPsql();
		// in case data from file, the COPY command can be long. increasing the timeout time.
		if (dataToCopy == null) {
			sso.setCommandTimeout(ShellSystemObject._10_MINUTES);
		}

		runSqlCmd(sso, copyCmd, true);

		if (dataToCopy != null) {
			runSqlCmd(sso, dataToCopy, true);
		}

		closePsql(sso);

		ReportUtils.stopLevel(report);
	}

	@Override
	public ArrayList<String> getDataBasesList() throws Exception {

		Table dataname = new Table("dataname", null);
		queryResults(dataname, "SELECT datname FROM pg_catalog.pg_database");

		ArrayList<String> dbList = new ArrayList<String>();

		for (List<String> row : dataname.getData()) {

			dbList.add(row.get(0));

		}

		return dbList;
	}

	public void analyze(Table table) throws Exception {
		analyze(table, false);
	}

	/**
	 * Perform analyze over table
	 *
	 * @param table
	 * @param expectTurnedOffWarning if true expect specific Warning: <b>analyze for PXF tables is
	 *            turned off by 'pxf_enable_stat_collection'</b>
	 * @throws Exception
	 */
	public void analyze(Table table, boolean expectTurnedOffWarning) throws Exception {

		String query = "ANALYZE " + table.getName();

		if (expectTurnedOffWarning) {
			runQueryWithExpectedWarning(query, "analyze for PXF tables is turned off by 'pxf_enable_stat_collection'", true);

		} else {
			runQuery(query);
		}
	}

	public String getSshUserName() {
		return sshUserName;
	}

	public void setSshUserName(String sshUserName) {
		this.sshUserName = sshUserName;
	}

	public String getSshPassword() {
		return sshPassword;
	}

	public void setSshPassword(String sshPassword) {
		this.sshPassword = sshPassword;
	}

	private int determineVersion() throws Exception {
		String query = "SELECT version()";
		ReportUtils.report(report, getClass(), "Determining Greenplum version - query: " + query);

		ResultSet res = stmt.executeQuery(query);
		res.next();
		String fullVersion = res.getString(1);
		ReportUtils.report(report, getClass(), "Retrieved from Greenplum: [" + fullVersion + "]");
		int gpIndex = fullVersion.indexOf(GREENPLUM_DATABASE_PREFIX); // where the version prefix starts
		int dotIndex = fullVersion.indexOf(".", gpIndex);             // where the first dot of GP version starts
		String versionStr = fullVersion.substring(gpIndex + GREENPLUM_DATABASE_PREFIX.length(), dotIndex);
		int versionInt = Integer.valueOf(versionStr);
		ReportUtils.report(report, getClass(), "Determined Greenplum version: " + versionInt);
		return versionInt;
	}

	private boolean serverExists(String name) throws SQLException {
		/* If in the future we want to check the existence of both the foreign server and the user mapping
		we can use the following query
		SELECT COUNT(*) FROM pg_catalog.pg_user_mapping um
		LEFT JOIN pg_catalog.pg_foreign_server fs ON um.umserver = fs.oid
		LEFT JOIN pg_catalog.pg_roles r ON um.umuser = r.oid
		WHERE r.rolname = session_user::text AND fs.srvname = '%s';
		 */
		String query = String.format("SELECT COUNT(*) FROM pg_catalog.pg_foreign_server WHERE srvname = '%s'", name);
		ReportUtils.report(report, getClass(), "Determining if foreign server exists - query: " + query);

		ResultSet res = stmt.executeQuery(query);
		res.next();
		int count = res.getInt(1);
		ReportUtils.report(report, getClass(), "Retrieved from Greenplum: [" + count + "] servers");
		return count > 0;
	}

	/**
	 * Create a table like the other table, only schema / distribution is copied, not the data.
	 * @param name name of table to create
	 * @param source source table
	 * @return table that got created
	 * @throws Exception if the operation fails
	 */
	private Table createTableLike(String name, Table source) throws Exception {
		Table table = new Table(name, source.getFields());
		String[] distributionFields = source.getDistributionFields();
		if (distributionFields != null && distributionFields.length > 0) {
			table.setDistributionFields(distributionFields);
		} else {
			// set distribution field as the first one so that PSQL does not issue a warning
			// extract the name of the first table field and type, split off the type that follows the name after whitespace
			table.setDistributionFields(new String[]{table.getFields()[0].split("\\s+")[0]});
		}
		createTableAndVerify(table);
		return table;
	}

}
