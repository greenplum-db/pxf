package org.greenplum.pxf.automation.components.common;

import java.util.ArrayList;

import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Define functionality over Data bases
 */
public interface IDbFunctionality {

	public void createTable(Table table) throws Exception;

	/**
	 * Convenient method for creating table - will drop the old table if exists create it and check
	 * if exists
	 * 
	 * @param table
	 * @throws Exception
	 */
	public void createTableAndVerify(Table table) throws Exception;

	public void dropTable(Table table, boolean cascade) throws Exception;

	public void dropDataBase(String schemaName, boolean cascade, boolean ignoreFail)
			throws Exception;

	/**
	 * Inserts data from source Table data to target Table.
	 * 
	 * @param source table to read the data from
	 * @param target table to get the required table to insert to
	 * @throws Exception
	 */
	public void insertData(Table source, Table target) throws Exception;

	public void createDataBase(String schemaName, boolean ignoreFail) throws Exception;

	/**
	 * Gets tables list for data base
	 * 
	 * @param dataBaseName
	 * @return List of Table names for data base
	 * @throws Exception
	 */
	public ArrayList<String> getTableList(String dataBaseName) throws Exception;

	/**
	 * Queries Data into Table Object.
	 * 
	 * @param table
	 * @param query
	 * @throws Exception
	 */
	public void queryResults(Table table, String query) throws Exception;

	/**
	 * Checks if Table exists in specific Schema.
	 * 
	 * @param table
	 * @return true if exists.
	 * @throws Exception
	 */
	public boolean checkTableExists(Table table) throws Exception;

	/**
	 * Checks if Database exists, if so return true.
	 * 
	 * @param dbName
	 * @return if exists or not
	 * @throws Exception
	 */
	public boolean checkDataBaseExists(String dbName) throws Exception;

	/**
	 * Gets list of all existing Data Bases.
	 * 
	 * @return list of existing DBs
	 * @throws Exception
	 */
	public ArrayList<String> getDataBasesList() throws Exception;

    /**
     * Grant read permissions on a table
     * @param table
     * @param user
     * @throws Exception
     */
    public void grantReadOnTable(Table table, String user) throws Exception;

    /**
     * Grant write permissions on a table
     * @param table
     * @param user
     * @throws Exception
     */
    public void grantWriteOnTable(Table table, String user) throws Exception;
}
