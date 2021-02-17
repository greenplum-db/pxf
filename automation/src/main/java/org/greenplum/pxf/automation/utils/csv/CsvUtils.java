package org.greenplum.pxf.automation.utils.csv;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Utilities for working with CSV files
 */
public abstract class CsvUtils {

	/**
	 * Get Table of data from CSV file
	 *
	 * @param pathToCsvFile to read from to Table
	 * @return {@link Table} with data list from CSV file
	 * @throws IOException
	 */
	public static Table getTable(String pathToCsvFile) throws IOException {

		// direct CSVReader to csv file
		CSVReader csvReader = new CSVReader(new FileReader(pathToCsvFile));

		// read csv file to List
		List<String[]> list = csvReader.readAll();

		// create table and load csv as list to it
		Table dataTable = new Table(pathToCsvFile, null);

		try {
			for (Iterator<String[]> iterator = list.iterator(); iterator.hasNext();) {
				dataTable.addRow(iterator.next());
			}
		} finally {
			csvReader.close();
		}

		return dataTable;
	}

	/**
	 * Write {@link Table} to a CSV file.
	 *
	 * @param table {@link Table} contains required data list to write to CSV file
	 * @param targetCsvFile to write the data Table
	 * @throws IOException
	 */
	public static void writeTableToCsvFile(Table table, String targetCsvFile)
			throws IOException {

		// create CsvWriter using FileWriter
		CSVWriter csvWriter = new CSVWriter(new FileWriter(targetCsvFile));

		try {
			// go over list and write each inner list to csv file
			for (List<String> currentList : table.getData()) {

				Object[] objectArray = currentList.toArray();

				csvWriter.writeNext(Arrays.copyOf(currentList.toArray(), objectArray.length, String[].class));
			}
		} finally {

			// flush and close writer
			csvWriter.flush();
			csvWriter.close();
		}
	}
}