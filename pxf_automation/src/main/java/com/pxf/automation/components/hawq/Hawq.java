package com.pxf.automation.components.hawq;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

import com.pxf.automation.components.common.DbSystemObject;
import com.pxf.automation.components.common.ShellSystemObject;
import com.pxf.automation.structures.tables.basic.Table;
import com.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * HAWQ system object, defines functionality for HAWQ Data Base.
 */
public class Hawq extends DbSystemObject {

	private String sshUserName;

	private String sshPassword;

	private static final String DEFAULT_PORT = "5432";

	public Hawq() {

	}

	public Hawq(boolean silenceReport) {
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
		address = "jdbc:postgresql://" + getHost() + ":" + getPort() + "/template1";

		connect();

		if (!checkDataBaseExists(getDb())) {
			createDataBase(getDb(), true);
		}

		super.close();

		address = "jdbc:postgresql://" + getHost() + ":" + getPort() + "/" + getDb();

		connect();

		ReportUtils.stopLevel(report);
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
	 * @param source
	 * @param target
	 * @throws Exception
	 */

    public void copyData(Table source, Table target) throws Exception {


        runQuery("INSERT INTO " + target.getName() + " SELECT * FROM "
                + source.getName());
    }

	@Override
	public void createDataBase(String schemaName, boolean ignoreFail) throws Exception {

		runQuery("CREATE DATABASE " + schemaName, ignoreFail, false);
		runQuery("ALTER DATABASE " + schemaName + " SET bytea_output TO 'escape'", ignoreFail, false);
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

		copy(to.getName(), "STDIN", dataStringBuilder.toString(), delim, null, csv);
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
		copy(to.getName(), from, null, delim, null, csv);
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
}
