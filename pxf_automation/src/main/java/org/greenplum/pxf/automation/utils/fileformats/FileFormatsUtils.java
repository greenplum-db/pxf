package org.greenplum.pxf.automation.utils.fileformats;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

import org.greenplum.pxf.automation.fileformats.IDataPreparer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

public class FileFormatsUtils {

	public static Object[] prepareData(IDataPreparer reader, int rows, Table dataTable) throws Exception {
		return reader.prepareData(rows, dataTable);
	}

	public static void prepareDataFile(Table dataTable, int amount, String pathToFile) throws Exception {
		File file = new File(pathToFile);
		file.delete();

		String listString = "";

		for (List<String> row : dataTable.getData()) {

			for (String item : row) {
				listString += item + ",";
			}
			listString = listString.substring(0, listString.length() - 1);
			listString += System.getProperty("line.separator");
		}
		listString = listString.substring(0, listString.length() - 1);

		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));

		for (int i = 0; i < amount; i++) {
			out.println(listString);
		}
		out.close();
	}
}