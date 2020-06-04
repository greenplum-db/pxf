package org.greenplum.pxf.automation.structures.tables.basic;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FileUtils;

import org.greenplum.pxf.automation.enums.EnumCompressionTypes;
import org.greenplum.pxf.automation.structures.data.DataPattern;
import org.greenplum.pxf.automation.utils.jdbc.JdbcTypesUtils;

/**
 * Represents DB table.
 */
public class Table {

    private List<String> columnsHeaders;
    private List<List<String>> data;
    private List<Integer> colsDataType;

    private String schema;
    private String name;
    private String[] fields;
    private String[] distributionFields;
    private boolean randomDistribution;

    private DataPattern dataPattern;
    private long numberOfLines = 0;

    public Table(String name, String[] fields) {
        this.name = name;
        this.randomDistribution = false;

        if (fields != null) {
            this.fields = Arrays.copyOf(fields, fields.length);
        }

        initDataStructures();
    }

    public Table(String name, String[] fields, DataPattern dataPattern) {
        this.name = name;
        this.randomDistribution = false;

        if (fields != null) {
            this.fields = Arrays.copyOf(fields, fields.length);
        }

        this.dataPattern = dataPattern;
        initDataStructures();
    }

    protected String createHeader() {
        return "CREATE TABLE " + getFullName();
    }

    protected String createFields() {

        StringBuilder sb = new StringBuilder();

        sb.append(" (");

        String prefix = "";
        for (int i = 0; i < fields.length; i++) {
            sb.append(prefix).append(fields[i]);
            prefix = ", ";
        }

        sb.append(")");

        return sb.toString();
    }

    protected String distribution() {
        if (randomDistribution) {
            return " DISTRIBUTED RANDOMLY";
        }

        StringBuilder sb = new StringBuilder();

        if (getDistributionFields() != null && getDistributionFields().length > 0) {

            int arraySize = getDistributionFields().length;

            sb.append(" DISTRIBUTED BY (");

            String prefix = "";
            for (int i = 0; i < arraySize; i++) {
                sb.append(prefix).append(distributionFields[i]);
                prefix = ", ";
            }

            sb.append(")");
        }

        return sb.toString();
    }

    protected String createLocation() {
        return "";
    }

    /**
     * Generates "Create Table" query
     *
     * @return "create Table" query
     */
    public String constructCreateStmt() {
        StringBuilder sb = new StringBuilder();

        sb.append(createHeader());
        sb.append(createFields());
        sb.append(distribution());

        return sb.toString();
    }

    /**
     * Generates "Drop Table" query
     *
     * @param cascade drop with cascade
     * @return "Drop Table" query
     */
    public String constructDropStmt(boolean cascade) {

        StringBuilder sb = new StringBuilder();

        sb.append("DROP TABLE IF EXISTS " + getFullName());
        if (cascade) {
            sb.append(" CASCADE");
        }

        return sb.toString();
    }

    /**
     * returns table full name if schema exists in the following format: "schema.table"
     *
     * @return full table name
     */
    public String getFullName() {

        String schema = getSchema();

        if (schema != null) {
            return (schema + "." + name);
        }

        return name;
    }

    /**
     * Add Row to Data List in List format.
     *
     * @param row
     */
    public void addRow(List<String> row) {
        if (data == null) {
            data = new ArrayList<>();
        }

        data.add(row);
    }

    public void setRandomDistribution() {
        randomDistribution = true;
    }

    /**
     * Add Row to Data List using String[]
     *
     * @param row
     */
    public void addRow(String[] row) {

        addRow(new ArrayList<>(Arrays.asList(row)));
    }

    /**
     * Add rows to data list using String[][]
     *
     * @param rows
     */
    public void addRows(String[][] rows) {
        if (rows == null) {
            return;
        }

        for (int i = 0; i < rows.length; ++i)
            addRow(rows[i]);
    }

    /**
     * duplicate table's data by factor times
     *
     * @param factor
     * @param pumpAsBulk if true, duplicate all table as bulk by factor times
     */
    public void pumpUpTableData(int factor, boolean pumpAsBulk) {

        if (data == null) {
            return;
        }

        List<List<String>> pumped = new ArrayList<List<String>>();

        if (pumpAsBulk) {

            for (int i = 0; i < factor; ++i) {
                for (List<String> row : data) {
                    // add new List to the pumped list
                    pumped.add(new ArrayList<>(row));
                }
            }

        } else {
            for (List<String> row : data) {
                for (int i = 0; i < factor; ++i) {
                    pumped.add(row);
                }
            }
        }

        data = pumped;
    }

    public void pumpUpTableData(int factor) {
        pumpUpTableData(factor, false);
    }

    /**
     * Add data cell to specific row in data list
     *
     * @param rowIndex
     * @param dataToAdd
     */
    public void addCellToDataRow(int rowIndex, String dataToAdd) {

        if (data == null) {
            return;
        }

        ((ArrayList<String>) data.get(rowIndex)).add(dataToAdd);
    }

    /**
     * Appends all rows from from table to this table
     *
     * @param from from table
     */
    public void appendRows(Table from) {
        List<List<String>> rows = from.getData();
        for (List<String> row : rows) {
            addRow(row);
        }
    }

    /**
     * Generates HTML table from Data List
     *
     * @return String represents the Table as HTML table
     */
    public String getDataHtml() {

        StringBuilder dataHtml = new StringBuilder();

        dataHtml.append("<table border=\"1\">");

        /**
         * Add headers
         */
        dataHtml.append("<tr bgcolor=\"#c1cdc1\">");

        for (int i = 0; i < columnsHeaders.size(); i++) {
            dataHtml.append("<td> " + columnsHeaders.get(i));

            if (colsDataType != null && colsDataType.size() == columnsHeaders.size()) {
                dataHtml.append(" (" + JdbcTypesUtils.getSqlTypeName(colsDataType.get(i)) + ")");
            }

            dataHtml.append("</td>");
        }

        dataHtml.append("</tr>");

        dataHtml.append("<tr>");

        for (int i = 0; i < data.size(); i++) {

            List<String> row = data.get(i);

            for (int j = 0; j < row.size(); j++) {
                dataHtml.append("<td> " + row.get(j) + "</td>");
            }

            dataHtml.append("</tr>");

        }

        dataHtml.append("</table>");

        return dataHtml.toString();
    }

    /**
     * Add data type to colsDataType list
     *
     * @param dataType
     */
    public void addColDataType(int dataType) {
        if (colsDataType == null) {
            colsDataType = new ArrayList<Integer>();
        }

        colsDataType.add(dataType);
    }

    public void addColumnHeader(String header) {
        if (columnsHeaders == null) {
            columnsHeaders = new ArrayList<String>();
        }

        columnsHeaders.add(header);
    }

    public void addColumn(String header, int dataType) {
        addColumnHeader(header);
        addColDataType(dataType);
    }

    /**
     * resets the data and colsDataType lists.
     */
    public void initDataStructures() {
        colsDataType = new ArrayList<Integer>();
        data = new ArrayList<List<String>>();
        columnsHeaders = new ArrayList<String>();
    }

    /**
     * Load data from Text file to Table data List according to provided delimiter. The data is
     * being read as UTF-8. If the file is compressed the compression type should be mentioned.
     *
     * @param path            file to required file to load
     * @param delimiter       for splitting data on file
     * @param sortColumnIndex for splitting data on file
     * @param encoding        text encoding type (like UTF-8...)
     * @param compressionType required compression type.
     * @throws IOException
     */
    public void loadDataFromFile(String path, String delimiter, final int sortColumnIndex, String encoding, EnumCompressionTypes compressionType, boolean appendData) throws IOException {
        File file = new File(path);

        if (!file.exists()) {
            throw new FileNotFoundException("Error Loading data from File:" + path + " File Not Found");
        }

        InputStream in = null;
        switch (compressionType) {
            case GZip:
                in = new GZIPInputStream(new FileInputStream(path));
                break;
            case BZip2:
                in = new BZip2CompressorInputStream(new FileInputStream(path));
                break;
            case None:
                break;
            default:
                break;
        }

        List<String> lines = null;
        if (in == null) {
            // get lines from file
            lines = FileUtils.readLines(file, encoding);
        } else {
            File extractedFile = new File(path + "_extracted");
            FileUtils.copyInputStreamToFile(in, extractedFile);
            // get lines from file
            lines = FileUtils.readLines(extractedFile, encoding);
        }
        if (!appendData) {
            // clean data list
            initDataStructures();
        }
        // go over lines, split it according to delimiter and put in data list
        for (String line : lines) {
            // by default only one column
            String[] columns = new String[]{line};
            // if delimiter exists, split it
            if (delimiter != null) {
                // find out if quoted values exist in line
                String[] csvSplit = line.split(delimiter + "\"|\"" + delimiter);
                String shapedLine = "";
                // if so shape it to be without quotes, add back slash to delimiter in the quoted
                // value.
                if (csvSplit != null) {
                    for (int i = 0; i < csvSplit.length; i++) {
                        if (csvSplit[i].endsWith("\"")) {
                            csvSplit[i] = csvSplit[i].replaceAll("\"", "").replaceAll(delimiter, "\\\\" + delimiter);
                        }
                        // add delimiter if not first split
                        if (i > 0) {
                            shapedLine += delimiter;
                        }
                        // collect all shaped splits to shapedLine
                        shapedLine += csvSplit[i];
                    }
                } else {
                    // if no quoted cells get the line as is
                    shapedLine = line;
                }
                // split according to delimiter without backslash before it.
                columns = shapedLine.split("(?<!\\\\)" + delimiter);
            }
            // get Array as fixed list and put it into ArrayList
            List<String> row = new ArrayList<>(Arrays.asList(columns));
            data.add(row);
        }
        // if a sort required, use local this local Comparator which compare the required sortIndex
        // and uses Collention.sort to sort
        if (sortColumnIndex >= 0) {
            Collections.sort(data, new Comparator<List<String>>() {
                @Override
                public int compare(List<String> o1, List<String> o2) {
                    return o1.get(sortColumnIndex).compareTo(o2.get(sortColumnIndex));
                }
            });
        }
    }

    /**
     * Load data from Text file to Table data List according to provided delimiter. The data is
     * being read as UTF-8. The assumption is that the file is not compressed.
     *
     * @param path            HDFS file to required file to load
     * @param delimiter       for splitting data on file
     * @param sortColumnIndex column to sort by
     * @param appendData      if true append to exists data, false create new {@link List}
     * @param encoding        text encoding type (like UTF-8...)
     */
    public void loadDataFromFile(String path, String delimiter, final int sortColumnIndex, String encoding, boolean appendData) throws IOException {
        loadDataFromFile(path, delimiter, sortColumnIndex, encoding, EnumCompressionTypes.None, appendData);
    }

    /**
     * Load data from Text file to Table data List according to provided delimiter using UTF-8.
     *
     * @param path            HDFS file to required file to load
     * @param delimiter       for splitting data on file
     * @param sortColumnIndex column to sort by
     * @throws IOException
     */
    public void loadDataFromFile(String path, String delimiter, final int sortColumnIndex, boolean appendData) throws IOException {
        loadDataFromFile(path, delimiter, sortColumnIndex, "UTF-8", EnumCompressionTypes.None, appendData);
    }

    /**
     * Load data from Text file to Table data List according to provided delimiter. The data is
     * being read as UTF-8. The assumption is that the file is not compressed.
     *
     * @param path            HDFS file to required file to load
     * @param delimiter       for splitting data on file
     * @param sortColumnIndex column to sort by
     * @throws IOException
     */
    public void loadDataFromFile(String path, String delimiter, final int sortColumnIndex) throws IOException {
        loadDataFromFile(path, delimiter, sortColumnIndex, false);
    }

    /**
     * Load data from Text file to Table data List according to provided delimiter using UTF-8.
     *
     * @param path       path HDFS file to required file to load
     * @param delimiter  for splitting data on file
     * @param appendData if true append to exists data, false create new {@link List}
     * @throws IOException
     */
    public void loadDataFromFile(String path, String delimiter, boolean appendData) throws IOException {
        loadDataFromFile(path, delimiter, -1, "UTF-8", EnumCompressionTypes.None, appendData);
    }

    /**
     * Load data from Text file to Table data List according to provided delimiter using UTF-8.
     *
     * @param path
     * @param delimiter
     * @param compressionType compression of file to load
     * @param appendData      if true append to exists data, false create new {@link List}
     * @throws IOException
     */
    public void loadDataFromFile(String path, String delimiter, EnumCompressionTypes compressionType, boolean appendData) throws IOException {
        loadDataFromFile(path, delimiter, -1, "UTF-8", compressionType, appendData);
    }

    public DataPattern getDataPattern() {
        return dataPattern;
    }

    public void setDataPattern(DataPattern dataPattern) {
        this.dataPattern = dataPattern;
    }

    public long getNumberOfLines() {
        return numberOfLines;
    }

    public void setNumberOfLines(long numberOfLines) {
        this.numberOfLines = numberOfLines;
    }

    public void setColsDataType(List<Integer> colsDataType) {
        this.colsDataType = colsDataType;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public List<Integer> getColsDataType() {
        return colsDataType;
    }

    public List<String> getColumnNames() {
        return columnsHeaders;
    }

    public List<List<String>> getData() {
        return data;
    }

    public void setData(List<List<String>> data) {
        this.data = data;
    }

    public String[] getDistributionFields() {
        return distributionFields;
    }

    public void setDistributionFields(String[] distributionFields) {
        randomDistribution = false;
        this.distributionFields = Arrays.copyOf(distributionFields, distributionFields.length);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {

        this.fields = null;

        if (fields != null) {
            this.fields = Arrays.copyOf(fields, fields.length);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(System.lineSeparator()).append(getClass().getSimpleName()).append(": ").append(" Name: ").append(getName()).append(System.lineSeparator());
        sb.append("Data:").append(System.lineSeparator());
        if (columnsHeaders != null) {
            sb.append(columnsHeaders.toString()).append(System.lineSeparator());
        }
        if (data != null) {
            for (List<String> line : data) {
                sb.append(line.toString()).append(System.lineSeparator());
            }
        }
        return sb.toString();
    }

    public String constructInsertStmt() {
        StringBuilder insert = new StringBuilder("INSERT INTO " + name + " VALUES(");
        for (List<String> row : data) {
            insert.append(String.join(",", row));
            insert.append("),(");
        }
        insert.setLength(insert.length() - 2);
        insert.append(";");
        return insert.toString();
    }
}
