package org.greenplum.pxf.automation.datapreparer.hbase;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

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
public class HBaseDataPreparer implements IDataPreparer {

	private int numberOfSplits = 0;
	private String rowKeyPrefix = "";
	private String columnFamilyName = "cf1";
	private boolean useNull = false;
	private final int UNSUPORTTED_CHAR = 92;
	private final int FIRST_PRINTABLE_CHAR = 32;
	private final int LAST_PRINTABLE_CHAR = 126;

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		byte[] columnFamily = Bytes.toBytes(columnFamilyName);
		List<Put> generatedRows = new ArrayList<Put>();

		HBaseTable hbaseTable = (HBaseTable) dataTable;
		String[] qualifiers = hbaseTable.getQualifiers();

		for (int splitIndex = 0; splitIndex < (numberOfSplits + 1); ++splitIndex) {

			int chars = FIRST_PRINTABLE_CHAR;

			for (int i = 0; i < rows; ++i) {

				// Row Key
				String rowKey = String.format("%s%08d", rowKeyPrefix, (i + (splitIndex * rows)));
				Put newRow = new Put(Bytes.toBytes(rowKey));

				// Qualifier 1. regular ascii string
				if ((!useNull) || (i % 2 == 0)) {
					addValue(newRow, columnFamily, qualifiers[0], String.format("ASCII%08d", i));
				}

				// Qualifier 2. multibyte utf8 string.
				addValue(newRow, columnFamily, qualifiers[1], String.format("UTF8_計算機用語_%08d", i).getBytes());

				// Qualifier 3. integer value.
				if ((!useNull) || (i % 3 == 0)) {
					addValue(newRow, columnFamily, qualifiers[2], String.format("%08d", 1 + i + splitIndex * rows));
				}

				// Qualifier 4. regular ascii (for a lookup table redirection)
				addValue(newRow, columnFamily, qualifiers[3], String.format("lookup%08d", i * 2));

				// Qualifier 5. real (float)
				addValue(newRow, columnFamily, qualifiers[4], String.format("%d.%d", i, i));

				// Qualifier 6. float (double)
				addValue(newRow, columnFamily, qualifiers[5], String.format("%d%d%d%d.%d", i, i, i, i, i));

				// Qualifier 7. bpchar (char)
				addValue(newRow, columnFamily, qualifiers[6], String.format("%c", chars));

				// Qualifier 8. smallint (short)
				addValue(newRow, columnFamily, qualifiers[7], String.format("%d", (i % Short.MAX_VALUE)));

				// Qualifier 9. bigint (long)
				Long value9 = ((i * i * i * 10000000000L + i) % Long.MAX_VALUE) * (long) Math.pow(-1, i % 2);
				addValue(newRow, columnFamily, qualifiers[8], value9.toString());

				// Qualifier 10. boolean
				addValue(newRow, columnFamily, qualifiers[9], Boolean.toString((i % 2) == 0));

				// Qualifier 11. numeric (string)
				BigInteger bi = new BigInteger("10");
				addValue(newRow, columnFamily, qualifiers[10], bi.pow(i).toString());

				// Qualifier 12. Timestamp
				// Removing system timezone so tests will pass anywhere in the
				// world :)
				int timeZoneOffset = TimeZone.getDefault().getRawOffset();
				addValue(newRow, columnFamily, qualifiers[11], (new Timestamp((6000 * i) - timeZoneOffset)).toString());

				generatedRows.add(newRow);

				chars = nextChar(chars);
			}
		}

		((HBaseTable) dataTable).setRowsToGenerate(generatedRows);

		return null;
	}

	/**
	 * @return the next printable char
	 */
	private int nextChar(int chars) {

		if (chars == LAST_PRINTABLE_CHAR) {
			return FIRST_PRINTABLE_CHAR;
		}

		chars++;

		if (chars == UNSUPORTTED_CHAR) {
			chars++;
		}

		return chars;
	}

	private void addValue(Put row, byte[] cf, String ql, byte[] value) {
		row.add(cf, ql.getBytes(), value);
	}

	private void addValue(Put row, byte[] cf, String ql, String value) throws java.io.UnsupportedEncodingException {
		addValue(row, cf, ql, value.getBytes("UTF-8"));
	}

	public int getNumberOfSplits() {
		return numberOfSplits;
	}

	public void setNumberOfSplits(int numberOfSplits) {
		this.numberOfSplits = numberOfSplits;
	}

	public String getRowKeyPrefix() {
		return rowKeyPrefix;
	}

	public void setRowKeyPrefix(String rowKeyPrefix) {
		this.rowKeyPrefix = rowKeyPrefix;
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}

	public boolean isUseNull() {
		return useNull;
	}

	public void setUseNull(boolean useNull) {
		this.useNull = useNull;
	}
}