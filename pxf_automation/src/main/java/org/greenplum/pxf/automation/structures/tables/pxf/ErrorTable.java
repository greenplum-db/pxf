package org.greenplum.pxf.automation.structures.tables.pxf;

import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Represent GPDB error table.
 */
public class ErrorTable extends Table {

	private static final String[] ERR_TABLE_FIELDS = new String[] {
			"cmdtime timestamp with time zone",
			"relname text",
			"filename text",
			"linenum integer",
			"bytenum integer",
			"errmsg text",
			"rawdata text",
			"rawbytes bytea" };

	public ErrorTable(String name) {
		super(name, ERR_TABLE_FIELDS);
	}
}
