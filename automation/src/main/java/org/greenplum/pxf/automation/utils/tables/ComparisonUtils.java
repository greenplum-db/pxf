package org.greenplum.pxf.automation.utils.tables;

import java.io.File;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import jsystem.framework.report.Reporter;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.junit.Assert;

import org.greenplum.pxf.automation.enums.EnumHiveToGpdbType;
import org.greenplum.pxf.automation.utils.csv.CsvUtils;

/**
 * Utility for Comparing all sorts of types such Tables, CSV file
 */
public class ComparisonUtils {

    // offset for \d command in general
    private static final int PSQL_DESCRIBE_HEADER_OFFSET = 1;
    private static final int PSQL_DESCRIBE_FOOTER_OFFSET = 1;
    // offset for \d command per each table
    private static final int PSQL_DESCRIBE_HEADER_TABLE_OFFSET = 3;

	private static StringBuilder htmlReport;

	public static boolean compareTableData(Table table1, Table table2, int limit, String... ignoreChars) throws Exception {
		return compareTableData(table1, table2, limit, null, ignoreChars);
	}

	public static boolean compareTableData(Table table1, Table table2, int limit, Reporter report, String... ignoreChars) throws Exception {
		htmlReport = new StringBuilder();
		List<List<String>> t1Data = table1.getData();
		List<List<String>> t2Data = table2.getData();
		List<Integer> t1Types = table1.getColsDataType();
		List<Integer> t2Types = table2.getColsDataType();

		if (limit > 0) {
			if (t1Data.size() < limit) {
				throw new Exception("Table " + table1.getFullName() + " size is lower than choosen limit (" + limit + ")");
			}
			if (t2Data.size() < limit) {
				throw new Exception("Table " + table2.getFullName() + " size is lower than choosen limit (" + limit + ")");
			}
		}

		else if (t1Data.size() != t2Data.size()) {
			throw new Exception("Table: " + table1.getFullName() + " size = " + t1Data.size() + " not equal to Table: " + table2.getFullName() + " size = " + t2Data.size());
		}

		boolean result = true;

		StringBuilder htmlFormat1 = new StringBuilder();
		htmlFormat1.append("<table border=\"1\">");
		htmlFormat1.append("<tr>");

		StringBuilder htmlFormat2 = new StringBuilder();
		htmlFormat2.append("<table border=\"1\">");

		if (t1Types != null) {

			htmlFormat2.append("<tr>");

			for (int i = 0; i < t1Types.size(); i++) {
				htmlFormat1.append("<td>" + t1Types.get(i) + "</td>");
			}

			htmlFormat1.append("</tr>");
		}

		if (t2Types != null) {

			htmlFormat2.append("<tr>");

			for (int i = 0; i < t2Types.size(); i++) {
				htmlFormat2.append("<td>" + t2Types.get(i) + "</td>");
			}

			htmlFormat2.append("</tr>");
		}

		int comparasionSize = t1Data.size();

		if (limit > 0) {
			comparasionSize = limit;
		}
		for (int i = 0; i < comparasionSize; i++) {

			result &= compareRowData(t1Types, t1Data.get(i), htmlFormat1, t2Data.get(i), htmlFormat2, report, ignoreChars);

			htmlFormat1.append("</tr>");
			htmlFormat2.append("</tr>");
		}

		htmlFormat1.append("</table>");
		htmlFormat2.append("</table>");

		htmlReport.append(table1.getFullName() + "<br>" + htmlFormat1 + "<br><br>" + table2.getFullName() + "<br>" + htmlFormat2);

		return result;
	}

	/**
	 * Checks specific Column and return if equal
	 * 
	 * @param colType
	 * @param dataColT1
	 * @param dataColT2
	 * @return true if columns are equal
	 * @throws ParseException
	 */
	private static boolean checkColData(int colType, String dataColT1, String dataColT2, Reporter report) throws ParseException {

		try {
			if ((dataColT1.equals("null") && dataColT2.equals("")) || (dataColT2.equals("null") && dataColT1.equals(""))) {
				return true;
			}

			// Hive mark for Null
			if ((dataColT1.equals("\\N") && dataColT2.equals("null")) || (dataColT2.equals("\\N") && dataColT1.equals("null"))) {
				return true;
			}

			if (dataColT1.equals(dataColT2)) {
				return true;
			}

			// Numeric is Java's BigDecimal, using Double to convert from String and comparing int
			// value
			if (colType == Types.NUMERIC) {
				if (Double.valueOf(dataColT1).intValue() == Double.valueOf(dataColT2).intValue()) {

					return true;
				}
			}

			if (colType == Types.INTEGER || colType == Types.BIGINT || colType == Types.SMALLINT) {
				if (Long.valueOf(dataColT1).longValue() == Long.valueOf(dataColT2).longValue()) {
					return true;
				}
			}

			if (colType == Types.DOUBLE) {
				if (Double.valueOf(dataColT1).doubleValue() == Double.valueOf(dataColT2).doubleValue()) {

					return true;
				}
			}

			if (colType == Types.FLOAT || colType == Types.REAL) {
				if (Float.valueOf(dataColT1).floatValue() == Float.valueOf(dataColT2).floatValue()) {

					return true;
				}
			}

			if (colType == Types.BOOLEAN || colType == Types.BIT) {

				String tempDataColT1 = (dataColT1.startsWith("t") ? "true" : "false");
				String tempDataColT2 = (dataColT2.startsWith("t") ? "true" : "false");

				if (Boolean.parseBoolean(tempDataColT1) == Boolean.parseBoolean(tempDataColT2)) {

					return true;
				}
			}

			if (colType == Types.TIMESTAMP) {

				String t1FormatedTS = dataColT1.replaceAll("-", "/");
				String t2FormatedTS = dataColT2.replaceAll("-", "/");

				if (t1FormatedTS.contains(".")) {
					t1FormatedTS = t1FormatedTS.substring(0, t1FormatedTS.indexOf("."));
				}

				if (t2FormatedTS.contains(".")) {
					t2FormatedTS = t2FormatedTS.substring(0, t2FormatedTS.indexOf("."));
				}

				DateFormat dateFormater = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

				if (dateFormater.parse(t1FormatedTS).getTime() == dateFormater.parse(t2FormatedTS).getTime()) {

					return true;
				}
			}

			if (colType == Types.BINARY) {

				ReportUtils.report(report, ComparisonUtils.class, dataColT1);
				ReportUtils.report(report, ComparisonUtils.class, Arrays.toString(dataColT1.getBytes()));
				ReportUtils.report(report, ComparisonUtils.class, dataColT2);
				ReportUtils.report(report, ComparisonUtils.class, Arrays.toString(dataColT2.getBytes()));

				if ((Arrays.equals(dataColT1.getBytes(), dataColT2.getBytes("UTF-8"))) || (Arrays.equals(dataColT1.getBytes("UTF-8"), "?".getBytes())) || (Arrays.equals("?".getBytes(), dataColT2.getBytes()))) {

					return true;
				}
			}

		} catch (Exception e) {
			ReportUtils.report(report, ComparisonUtils.class.getClass(), e.getMessage());
		}
		return false;
	}

	/**
	 * Compares Table rows
	 * 
	 * @param row1Types GPDB row types
	 * @param row1Data GPDB row data
	 * @param row1HtmlReport GPDB html collector
	 * @param row2Data External data source row data
	 * @param row2HtmlReport External data source html collector
	 * @throws Exception
	 */
	private static boolean compareRowData(List<Integer> row1Types, List<String> row1Data, StringBuilder row1HtmlReport, List<String> row2Data, StringBuilder row2HtmlReport, Reporter report, String... ignoreChars) throws Exception {

		boolean result = true;

		for (int table1Index = 0, table2Index = 0; table1Index < row1Data.size(); table1Index++, table2Index++) {

			String dataColT1 = "null";
			String dataColT2 = "null";

			if (row1Data.size() > table1Index && row1Data.get(table1Index) != null) {
				dataColT1 = row1Data.get(table1Index).trim();
			}

			if (row2Data.size() > table2Index && row2Data.get(table2Index) != null) {
				dataColT2 = row2Data.get(table2Index).trim();
			}

			/**
			 * Go over ignoreChars and replace all required chars with ""
			 */
			if (ignoreChars != null && ignoreChars.length > 0) {

				for (int i = 0; i < ignoreChars.length; i++) {
					dataColT1 = dataColT1.replaceAll(ignoreChars[i], "");
					dataColT2 = dataColT2.replaceAll(ignoreChars[i], "");
				}
			}

			boolean isEqual = false;

			if (isArray(dataColT1, dataColT2)) {
				isEqual = checkColData(row1Types.get(table1Index).intValue(), dataColT1.replace(", ", ","), dataColT2, report);
				result &= isEqual;
			} else if (isStruct(dataColT1, dataColT2)) {
				isEqual = checkColData(row1Types.get(table1Index).intValue(), dataColT1.replace("[", "{").replace("]", "}").replace(", ", ","), dataColT2.replaceAll("\\p{Alnum}*:", "").replace("[", "{").replace("]", "}"), report);
				result &= isEqual;
			} else if (isMap(dataColT1)) {
				isEqual = checkColData(row1Types.get(table1Index).intValue(), dataColT1.replace(", ", ",").replace("=", ":"), dataColT2, report);
				result &= isEqual;
			} else {
				// check if type exists, else send with OTHER
				int columnType = Types.OTHER;
				if (row1Types != null && row1Types.size() >= table1Index && row1Types.size() > 0) {
					columnType = row1Types.get(table1Index).intValue();
				}
				isEqual = checkColData(columnType, dataColT1, dataColT2, report);
				result &= isEqual;
			}

			row1HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + dataColT1 + ((isEqual) ? "" : "</font>") + "</td>");

			row2HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + dataColT2 + ((isEqual) ? "" : "</font>") + "</td>");

			if (result) {
				result = isEqual;
			}
		}

		row1HtmlReport.append("</tr>");
		row2HtmlReport.append("</tr>");

		return result;
	}

	private static boolean isArray(String dataColT1, String dataColT2) {
		return dataColT1.startsWith("[") && dataColT1.endsWith("]") && !dataColT2.contains(":");
	}

	private static boolean isStruct(String dataColT1, String dataColT2) {
		return dataColT1.startsWith("[") && dataColT1.endsWith("]") && dataColT2.contains(":");
	}

	private static boolean isMap(String dataCol) {
		return dataCol.startsWith("{") && dataCol.endsWith("}");
	}

	public static void compareTables(Table t1, Table t2, Reporter report, String... ignoreChars) throws Exception {
		compareTables(t1, t2, report, -1, ignoreChars);
	}

	public static void compareTables(Table t1, Table t2, Reporter report) throws Exception {
		compareTables(t1, t2, report, -1, new String[] {});
	}

	/**
	 * Compares t1 data with t2 data, will fail the test if false.
	 * 
	 * @param t1
	 * @param t2
	 * @throws Exception
	 */
	public static void compareTables(Table t1, Table t2, Reporter report, int limit, String... ignoreChars) throws Exception {
		ReportUtils.startLevel(report, ComparisonUtils.class, "Tables Comparison Result");
		boolean comparisonResultMatch = compareTableData(t1, t2, limit, report, ignoreChars);
		String reportTitle = "Tables Comparison " + ((comparisonResultMatch) ? " Passed" : "Failed");
		if (report != null) {
			ReportUtils.reportHtml(report, ComparisonUtils.class, ComparisonUtils.getHtmlReport());
		} else {
			ReportUtils.reportTable(t1);
			ReportUtils.reportTable(t2);
		}
		ReportUtils.report(report, ComparisonUtils.class, reportTitle, ((comparisonResultMatch) ? Reporter.PASS : Reporter.FAIL));
		ReportUtils.stopLevel(report);
	}

	/**
	 * Compares CSV file data with Table data.
	 * 
	 * @param csvFile
	 * @param table
	 * @throws Exception
	 */
	public static void compareCsv(File csvFile, Table table, Reporter report) throws Exception {
		compareTables(table, CsvUtils.getTable(csvFile.getAbsolutePath()), report);
	}

	/***
	 * 
	 * @return HTML code with Comparison details
	 */
	public static String getHtmlReport() {
		return htmlReport.toString();
	}

	/**
	 * Compares given (results) with expected table's column names and column types
	 */
	public static void compareTablesMetadata(Table expected, Table results) throws Exception {
		Assert.assertEquals("table column types mismatch", expected.getColsDataType(), results.getColsDataType());
		Assert.assertEquals("table column names mismatch", expected.getColumnNames(), results.getColumnNames());
	}

    /**
     * Compares psql output for \d for given list of tables 
     * 
     * 
     * @param psqlOutput psql output of \d command
     * @param hiveTables list of Hive tables
     * @return true if psql output matches given list of Hive tables
     * @throws Exception
     */
    public static boolean comparePsqlDescribeHive(String psqlOutput,
            List<HiveTable> hiveTables) throws Exception {

        boolean isEquals = true;
        int totalFieldsNum = 0;

        // clean up
        psqlOutput = psqlOutput.replaceAll("\\r\\n\\r", "");
        psqlOutput = psqlOutput.replaceAll("\\n\\r", "\r\n");

        String psqlLines[] = psqlOutput.split("\\r?\\n");

        Collections.sort(hiveTables, new Comparator<HiveTable>() {
            public int compare(HiveTable table1, HiveTable table2) {

                int compareSchemas;

                // compare schema names
                compareSchemas = (table1.getSchema() == null ? "default"
                        : table1.getSchema())
                        .compareTo(table2.getSchema() == null ? "default"
                                : table2.getSchema());

                if (compareSchemas != 0)
                    return compareSchemas;

                // compare table names
                return table1.getName().compareTo(table2.getName());
            }
        });

        // skip table's and command header
        int i = PSQL_DESCRIBE_HEADER_OFFSET + PSQL_DESCRIBE_HEADER_TABLE_OFFSET;

        for (HiveTable hiveTable : hiveTables) {

            if (i >= psqlLines.length) {
                ReportUtils.report(null, ComparisonUtils.class,
                        "Psql output has less tables than expected.");
                isEquals = false;
                break;
            }

            for (String field : hiveTable.getFields()) {
                String hiveColumnName;
                String hiveColumnType;
                try {
                    hiveColumnName = field.substring(0, field.indexOf(' ')).trim();
                    hiveColumnType = field.substring(field.indexOf(' ') + 1).trim();
                } catch (Exception e) {
                    ReportUtils.report(null, ComparisonUtils.class,
                            e.getMessage(), Reporter.FAIL);
                    ReportUtils.report(null, ComparisonUtils.class,
                            "Cannot parse expected field type, field definition - "
                                    + field);
                    isEquals = false;
                    break;
                }

                if (!psqlLines[i].contains("|")) {
                    ReportUtils.report(null, ComparisonUtils.class,
                            "Cannot parse field from psql output - "
                                    + psqlLines[i]);
                    isEquals = false;
                    break;
                }

                String[] psqlNameAndType = psqlLines[i].split("\\|");
                psqlNameAndType[0] = psqlNameAndType[0].trim();
                psqlNameAndType[1] = psqlNameAndType[1].trim();

                isEquals = psqlNameAndType[0].equals(hiveColumnName)
                        && psqlNameAndType[1]
                                .equals(EnumHiveToGpdbType.getGpdbType(hiveColumnType));

                if (!isEquals) {
                    ReportUtils.report(null, ComparisonUtils.class, "Expected column: "
                            + hiveColumnName + " " + hiveColumnType
                            + ", actual column in psql output: "
                            + psqlNameAndType[0] + " " + psqlNameAndType[1]);
                    break;
                }

                i++;
                totalFieldsNum++;
            }

            // skip header
            i += PSQL_DESCRIBE_HEADER_TABLE_OFFSET;

        }

        if ((isEquals && PSQL_DESCRIBE_HEADER_OFFSET + hiveTables.size()
                * PSQL_DESCRIBE_HEADER_TABLE_OFFSET + totalFieldsNum
                + PSQL_DESCRIBE_FOOTER_OFFSET != psqlLines.length)
                || hiveTables.size() == 0 && psqlLines.length != 0) {

            ReportUtils.report(null, ComparisonUtils.class,
                    "Psql output has more output than expected.");
            isEquals = false;
        }

        return isEquals;
    }
}
