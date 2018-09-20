package org.greenplum.pxf.automation.datapreparer.writable;

import java.sql.Timestamp;
import java.util.TimeZone;

import org.greenplum.pxf.automation.fileformats.IDataPreparer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;


/**
 * Data Preparer for {@link HdfsWritableTextTest} test cases. Prepare data for the following
 * columns:"t1 TEXT", "bi BIGINT", "b BIT", "bool BOOLEAN", "int INTEGER", "si SMALLINT",
 * "bin BYTEA", "ts TIMESTAMP, circ CIRCLE"
 */
public class WritableDataPreparer implements IDataPreparer {

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {
		int timeZoneOffset = TimeZone.getDefault().getRawOffset();
		for (int i = 0; i < rows; i++) {
			dataTable.addRow(new String[] {
					("aaa_" + (i + 1)),
					String.valueOf(i + 1000),
					(((i % 2) == 0) ? "1" : "0"),
					(((i % 2) == 0) ? "t" : "f"),
					String.valueOf(i + 1000),
					String.valueOf(i + 10),
					("b#!?bbb_" + (i + 1)),
					new Timestamp((System.currentTimeMillis()) - timeZoneOffset).toString(),
					("<(" + (i + 1) + "\\," + (i + 1) + ")\\," + (i + 1)) + ">" });
		}
		return null;
	}
}
