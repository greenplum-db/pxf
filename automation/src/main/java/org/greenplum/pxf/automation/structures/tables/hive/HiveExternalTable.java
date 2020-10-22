package org.greenplum.pxf.automation.structures.tables.hive;

/**
 * represents Hive External Table.
 */
public class HiveExternalTable extends HiveTable {

	private String location;

	public HiveExternalTable(String name, String[] fields) {
		this(name, fields, null);
	}

	public HiveExternalTable(String name, String[] fields, String location) {
		super(name, fields);
		this.location = location;
	}

	@Override
	protected String createHeader() {
		return "CREATE EXTERNAL TABLE " + getFullName();
	}

	@Override
	protected String getLocation() {
		return this.location;
	}

}
