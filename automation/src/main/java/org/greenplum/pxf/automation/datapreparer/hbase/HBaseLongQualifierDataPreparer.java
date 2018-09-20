package org.greenplum.pxf.automation.datapreparer.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.greenplum.pxf.automation.fileformats.IDataPreparer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hbase.HBaseTable;

/**
 * HBaseDataPreparer creates given rows for every numberOfSplits. (rows * numberOfSplits). The data
 * tries to include the various data types: String,UTF8 string, Integer, ASCII, real, float, char,
 * small integer, big integer and Time stamp.
 */
public class HBaseLongQualifierDataPreparer implements IDataPreparer {

	private String columnFamilyName = "cf1";

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		byte[] columnFamily = Bytes.toBytes(columnFamilyName);
		List<Put> generatedRows = new ArrayList<Put>();

		HBaseTable hbaseTable = (HBaseTable) dataTable;
		String[] qualifiers = hbaseTable.getQualifiers();

		for (int i = 0; i < rows; ++i) {

			// Row Key
			String rowKey = String.format("%s%08d", 0, i * rows);
			Put newRow = new Put(Bytes.toBytes(rowKey));

			addValue(newRow, columnFamily, qualifiers[0], ("long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_long_" + i));
			addValue(newRow, columnFamily, qualifiers[1], ("short_" + i));

			generatedRows.add(newRow);
		}

		((HBaseTable) dataTable).setRowsToGenerate(generatedRows);

		return null;
	}

	private void addValue(Put row, byte[] cf, String ql, byte[] value) {
		row.add(cf, ql.getBytes(), value);
	}

	private void addValue(Put row, byte[] cf, String ql, String value) throws java.io.UnsupportedEncodingException {
		addValue(row, cf, ql, value.getBytes("UTF-8"));
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}
}