package org.greenplum.pxf.automation.utils.csv;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
	 * Update the delimiter in a CSV file
	 *
	 * @param originalDelim Original single char delimiter
	 * @param newDelimiter Desired multi-char delimiter
	 * @throws IOException
	 */
	public static void updateDelim(String targetCsvFile, char originalDelim, String newDelimiter)
			throws IOException {

		Path path = Paths.get(targetCsvFile);

		String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
		content = content.replace(String.valueOf(originalDelim), newDelimiter);
		Files.write(path, content.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Write {@link Table} to a CSV file.
	 *
	 * @param table {@link Table} contains required data list to write to CSV file
	 * @param targetCsvFile to write the data Table
	 * @throws IOException
	 */
	public static void writeTableToCsvFileOptions(Table table, String targetCsvFile, Charset charset,
												  char delimiter, char quotechar,
												  char escapechar, String eol)
			throws IOException {

		// create CsvWriter using FileWriter
		CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream(targetCsvFile), charset), delimiter, quotechar, escapechar, eol);
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

	/**
	 * Write {@link Table} to a CSV file.
	 *
	 * @param table {@link Table} contains required data list to write to CSV file
	 * @param targetCsvFile to write the data Table
	 * @throws IOException
	 */
	public static void writeTableToCsvFile(Table table, String targetCsvFile)
		throws IOException {

		writeTableToCsvFileOptions(
				table,
				targetCsvFile,
				StandardCharsets.UTF_8,
				CSVWriter.DEFAULT_SEPARATOR,
				CSVWriter.DEFAULT_QUOTE_CHARACTER,
				CSVWriter.DEFAULT_ESCAPE_CHARACTER,
				CSVWriter.DEFAULT_LINE_END);
	}
}