package org.greenplum.pxf.automation.datapreparer;

import java.util.ArrayList;

import org.greenplum.pxf.automation.fileformats.IDataPreparer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Data Preparer for MultiLine Automation tests. Prepare 2 fields data of
 * numeric and string "rows" times;
 * 
 */
public class MultiLineSmokeDataPreparer implements IDataPreparer {

	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		Object[] data = new Object[rows];

		for (int j = 0, num1 = 1; j < rows; j++, num1++) {

			ArrayList<String> row = new ArrayList<String>();

			row.add("t" + num1);
			row.add(String.valueOf(num1));

			dataTable.addRow(row);
			data[j] = row;
		}

		return null;
	}
}
