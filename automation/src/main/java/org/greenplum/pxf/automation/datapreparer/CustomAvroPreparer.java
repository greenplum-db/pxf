package org.greenplum.pxf.automation.datapreparer;

import java.util.ArrayList;

import org.greenplum.pxf.automation.fileformats.IDataPreparer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Data Preparer for Avro using following fields created by CustomAvroRecordPreparer.
 * The data is created by CustomAvroRecordPreparer, and consists of the following fields:
 * int array (size 2), int, int, string array (size 5), string, double array (size 2),
 * double, float array (size 2), float, long array (size 2), long, boolean array (size 2), boolean,
 * byte array.
 */
public class CustomAvroPreparer implements IDataPreparer {

	private String schemaName;

	public CustomAvroPreparer(String schemaName) {
		this.schemaName = schemaName;
	}

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		CustomAvroRecordPreparer[] data = new CustomAvroRecordPreparer[rows];

		for (int i = 0; i < data.length; i++) {

			int num1 = i + 1;

			data[i] = new CustomAvroRecordPreparer(schemaName, num1, 10 * num1, 20 * num1);

			ArrayList<String> row = new ArrayList<String>();

			for (int j = 0; j < data[i].num.length; j++) {
				row.add(String.valueOf(data[i].num[j]));
			}

			row.add(String.valueOf(data[i].int1));
			row.add(String.valueOf(data[i].int2));

			for (int j = 0; j < data[i].strings.length; j++) {
				row.add(data[i].strings[j]);
			}

			row.add(data[i].st1);

			for (int j = 0; j < data[i].dubs.length; j++) {
				row.add(String.valueOf(data[i].dubs[j]));
			}

			row.add(String.valueOf(data[i].db));

			for (int j = 0; j < data[i].fts.length; j++) {
				row.add(String.valueOf(data[i].fts[j]));
			}

			row.add(String.valueOf(data[i].ft));

			for (int j = 0; j < data[i].lngs.length; j++) {
				row.add(String.valueOf(data[i].lngs[j]));
			}

			row.add(String.valueOf(data[i].lng));

			for (int j = 0; j < data[i].bls.length; j++) {
                row.add(String.valueOf(data[i].bls[j]));
            }

			row.add(String.valueOf(data[i].bl));

			row.add(new String(data[i].bts));

			dataTable.addRow(row);
		}

		return data;
	}
}
