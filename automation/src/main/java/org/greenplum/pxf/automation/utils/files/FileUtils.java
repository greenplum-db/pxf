package org.greenplum.pxf.automation.utils.files;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;

import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Utilities for handling files
 */
public class FileUtils {

	/**
	 * Writes {@link Table} data to text file using provided delimiter.
	 * 
	 * @param table {@link Table}
	 * @param pathToFile target text file
	 * @param delimiter to use between columns
	 * @throws IOException
	 */
	public static void writeTableDataToFile(Table table, String pathToFile, String delimiter)
			throws IOException {

		File file = new File(pathToFile);
		File parentDirectory = file.getParentFile();

		// create parent directory if not exists
		if (!parentDirectory.exists()) {
			parentDirectory.mkdirs();
		}

		// use FileWrite to write to file
		FileWriter writer = new FileWriter(file);

		// get data from table
		List<List<String>> data = table.getData();

		// go over data list and write to file
		for (List<String> row : data) {
			for (String str : row) {
				writer.write(str);
				if (row.indexOf(str) != row.size() - 1) {
					writer.write(delimiter);
				}
			}

			// new line
			if (data.indexOf(row) != data.size() - 1) {
				writer.write("\n");
			}
		}

		// close write
		writer.close();
	}

	/**
	 * Changes file permission according to {@link PosixFilePermission}
	 * 
	 * @param file {@link File} object
	 * @param permissions required {@link PosixFilePermission}
	 * @throws IOException
	 */
	public static void setFilePermission(File file, Set<PosixFilePermission> permissions)
			throws IOException {

		// check if file exists
		if (file == null || !file.exists()) {
			throw new FileNotFoundException("Please provide a file");
		}

		// set Set of given permissions from user
		Files.setPosixFilePermissions(Paths.get(file.getAbsolutePath()), permissions);
	}
}