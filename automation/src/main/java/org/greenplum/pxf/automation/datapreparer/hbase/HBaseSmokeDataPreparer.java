package org.greenplum.pxf.automation.datapreparer.hbase;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.greenplum.pxf.automation.fileformats.IDataPreparer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hbase.HBaseTable;

/**
 * Data Preparer for HBase smoke test. Prepare "rows" amount of data of various data type.
 */
public class HBaseSmokeDataPreparer implements IDataPreparer {

	private String columnFamilyName;
	private String[] qualifiers;

	public Object[] prepareData(int rows, Table table) throws Exception {

		HBaseTable hbaseTable = (HBaseTable) table;
		String[] qualifiers = hbaseTable.getQualifiers();
		ArrayList<Put> generatedRows = new ArrayList<Put>();

		for (int i = 1; i <= rows; i++) {

			// define row key and create a Put object the represents the row.
			String rowKey = hbaseTable.getRowKeyPrefix() + "_" + i;
			Put newRow = new Put(Bytes.toBytes(rowKey));

			// String value
			String value = "row_" + i;
			newRow.add(columnFamilyName.getBytes(), qualifiers[0].getBytes(), value.getBytes());

			// int value
			value = Integer.toString(i);
			newRow.add(columnFamilyName.getBytes(), qualifiers[1].getBytes(), value.getBytes());

			// double value
			value = Double.toString(i);
			newRow.add(columnFamilyName.getBytes(), qualifiers[2].getBytes(), value.getBytes());

			// long value
			value = String.valueOf(Long.toString(100000000000L * (i)));
			newRow.add(columnFamilyName.getBytes(), qualifiers[3].getBytes(), value.getBytes());

			// boolean value
			value = Boolean.toString((i % 2) == 0);
			newRow.add(columnFamilyName.getBytes(), qualifiers[4].getBytes(), value.getBytes());

			generatedRows.add(newRow);
		}

		hbaseTable.setRowsToGenerate(generatedRows);
		return null;
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}

	public String[] getQualifiers() {
		return qualifiers;
	}

	public void setQualifiers(String[] qualifiers) {
		this.qualifiers = qualifiers;
	}
}