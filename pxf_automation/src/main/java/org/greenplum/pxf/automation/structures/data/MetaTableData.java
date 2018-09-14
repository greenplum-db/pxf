package org.greenplum.pxf.automation.structures.data;

/*
 * A meta data of table, hold properties of the external table to be created and its data to be generated,
 * Used to create multi tables to be tested by automation. 
 */
public class MetaTableData {

	private String tableName;
	private DataPattern dataPattern;
	
	// Getters
	public String getTableName() {
		return tableName;
	}

	public DataPattern getDataPattern() {
		return dataPattern;
	}

	// Setters	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void setDataPattern(DataPattern dataPattern) {
		this.dataPattern = dataPattern;
	}

	// Generate an array of column name & column type
    public String [] generateTableColumnsFields() {
    	int i = 0;
		String[] data = new String[dataPattern.getColumnsTypeList().size()];
		for(String column : dataPattern.getColumnsTypeList()) {
			data[i] = ("col" + ++i + " " + column);
		}	
		return data;
	}

	@Override
	public String toString() {
		return "MetaTableData [tableName=" + tableName + ", dataPattern=" + dataPattern + "]";
	}	
}
