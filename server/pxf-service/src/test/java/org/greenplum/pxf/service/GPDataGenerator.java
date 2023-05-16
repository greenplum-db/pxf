package org.greenplum.pxf.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.codec.binary.Hex;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is a utility class that has a main method and assertion functions to work with sample datasets.
 *
 * The main method generates a 'sample_data.sql' file in 'src/test/resources' directory. The file contains:
 * - a DDL statement for a definition of the Greenplum table 'sample_data' to hold sample data
 * - DML statements to insert the sample dataset into the 'sample_data' table
 * - DML statements to COPY the sample dataset to a set of CSV and TEXT files in 'src/test/resources'
 *
 * The sample dataset includes all types supported by PXF. There is at least one row for every datatype with
 * NULL value.
 *
 * The assertion methods compare values of a deserialized dataset with the original values produced by the class.
 */
public class GPDataGenerator {

    // {project}/server/pxf-service/build/resources/test/data ==> {project}/server/pxf-service/src/test/resources/data
    private static final String dir = GPDataGenerator.class.getClassLoader().getResource("data").getPath()
            .replace("/test", "")
            .replace("build", "src/test");

    /**
     * Enum representing different formats used to export the sample data set into a file using Greenplum COPY command.
     */
    public enum FORMAT {
        CSV("sample_data.csv", ',', null, '"', '"'),
        // for TEXT quote is not a concept, leave double quote character for constructor parameter
        TEXT("sample_data.txt", '\t', "\\N",'"', '\\'),
        CSV_PIPE("sample_data_pipe.csv", '|', null,'"', '"');

        @Getter
        private final String filename;
        @Getter
        private final Character delimiter;
        @Getter
        private final String nil;
        @Getter
        private final Character quote;
        @Getter
        private final Character escape;

        /**
         * Constructor, creates a new instance of the enum
         * @param filename name of the file where data will be exported to
         * @param delimiter field delimiter character
         * @param nil string representation of value of NULL
         * @param quote quote character
         * @param escape escape character
         */
        FORMAT(String filename, Character delimiter, String nil, Character quote, Character escape) {
            this.filename = filename;
            this.delimiter = delimiter;
            this.nil = nil;
            this.quote = quote;
            this.escape = escape;
        }
    }

    /**
     * A POJO to capture all metadata about a column of a dataset.
     */
    @Data
    @AllArgsConstructor
    private static class Column {
        String name;               // name     of the column            , used to create SQL DDL
        String sqlType;            // SQL type of the column            , used to create SQL DDL
        DataType type;             // DataType of the column            , used to produce List<ColumnDescriptor> for mocking
        DataType deserializedType; // DataType of the deserialized value, used in verification of deserialized OneField types
        Class clazz;               // Java class of the value           , used in verification of deserialized OneField values
    }

    // the schema of the sample dataset as an array of Column types
    private static final Column[] COLUMNS = {
            new Column("id"          ,"integer"                    , DataType.INTEGER                 , DataType.INTEGER , Integer.class   ),
            new Column("name"        ,"text"                       , DataType.TEXT                    , DataType.TEXT    , String.class    ),
            new Column("sml"         ,"smallint"                   , DataType.SMALLINT                , DataType.SMALLINT, Short.class     ),
            new Column("integ"       ,"integer"                    , DataType.INTEGER                 , DataType.INTEGER , Integer.class   ),
            new Column("bg"          ,"bigint"                     , DataType.BIGINT                  , DataType.BIGINT  , Long.class      ),
            new Column("r"           ,"real"                       , DataType.REAL                    , DataType.REAL    , Float.class     ),
            new Column("dp"          ,"double precision"           , DataType.FLOAT8                  , DataType.FLOAT8  , Double.class    ),
            new Column("dec"         ,"numeric"                    , DataType.NUMERIC                 , DataType.TEXT    , String.class    ),
            new Column("bool"        ,"boolean"                    , DataType.BOOLEAN                 , DataType.BOOLEAN , Boolean.class   ),
            new Column("cdate"       ,"date"                       , DataType.DATE                    , DataType.TEXT    , String.class    ),
            new Column("ctime"       ,"time"                       , DataType.TIME                    , DataType.TEXT    , String.class    ),
            new Column("tm"          ,"timestamp without time zone", DataType.TIMESTAMP               , DataType.TEXT    , String.class    ),
            new Column("tmz"         ,"timestamp with time zone"   , DataType.TIMESTAMP_WITH_TIME_ZONE, DataType.TEXT    , String.class    ),
            new Column("c1"          ,"character(3)"               , DataType.BPCHAR                  , DataType.TEXT    , String.class    ),
            new Column("vc1"         ,"character varying(5)"       , DataType.VARCHAR                 , DataType.TEXT    , String.class    ),
            new Column("bin"         ,"bytea"                      , DataType.BYTEA                   , DataType.BYTEA   , ByteBuffer.class),
            new Column("bool_arr"    ,"boolean[]"                  , DataType.BOOLARRAY               , DataType.TEXT    , String.class    ),
            new Column("int2_arr"    ,"smallint[]"                 , DataType.INT2ARRAY               , DataType.TEXT    , String.class    ),
            new Column("int_arr"     ,"int[]"                      , DataType.INT4ARRAY               , DataType.TEXT    , String.class    ),
            new Column("int8_arr"    ,"bigint[]"                   , DataType.INT8ARRAY               , DataType.TEXT    , String.class    ),
            new Column("float_arr"   ,"real[]"                     , DataType.FLOAT4ARRAY             , DataType.TEXT    , String.class    ),
            new Column("float8_arr"  ,"float[]"                    , DataType.FLOAT8ARRAY             , DataType.TEXT    , String.class    ),
            new Column("numeric_arr" ,"numeric[]"                  , DataType.NUMERICARRAY            , DataType.TEXT    , String.class    ),
            new Column("text_arr"    ,"text[]"                     , DataType.TEXTARRAY               , DataType.TEXT    , String.class    ),
            new Column("bytea_arr"   ,"bytea[]"                    , DataType.BYTEAARRAY              , DataType.TEXT    , String.class    ),
            new Column("char_arr"    ,"bpchar(5)[]"                , DataType.BPCHARARRAY             , DataType.TEXT    , String.class    ),
            new Column("varchar_arr" ,"varchar(5)[]"               , DataType.VARCHARARRAY            , DataType.TEXT    , String.class    )
    };

    // the schema of the sample dataset as a list of ColumnDescriptor objects to use in mocking the RequestContext
    public static final List<ColumnDescriptor> COLUMN_DESCRIPTORS = IntStream
            .range(0, COLUMNS.length)
            .mapToObj(i -> new ColumnDescriptor(COLUMNS[i].getName(), COLUMNS[i].getType().getOID(), i, COLUMNS[i].sqlType, null))
            .collect(Collectors.toList());

    private final List<List<Object>> table; // rows and columns that contain the sample dataset

    /**
     * Constructor, creates a sample dataset in memory and holds it in the private field 'table'.
     */
    public GPDataGenerator() {
        this.table = new LinkedList<>();
        addRow(0, -1); // add the first row without any nulls, id column is never null
        for (int row = 1; row < COLUMNS.length; row++) {
            addRow(row, row);                 // add a row with null value for the same column index as the row index
        }
    }

    /**
     * Helper method that generates values for all the columns of a new row with a given index and then
     * adds the created row to the sample dataset.
     * @param rowIndex index of the row
     * @param nullColumnIndex index of the column to set to NULL value
     */
    private void addRow(int rowIndex, int nullColumnIndex) {
        // the values represent the result of CSV parsing of a row produced by Greenplum COPY command
        // short/int/long/float/double and boolean values will be native Java types, bytea will be ByteBuffer
        // the rest including NUMERIC and all arrays will be Strings
        List<Object> row = new ArrayList<>(COLUMNS.length);
        row.add(rowIndex);                                                                 //  0 "id    integer"
        row.add(String.format("row-\"|%02d|\"", rowIndex));                                //  1 "name  text"
        row.add((short) rowIndex);                                                         //  2 "sml   smallint"
        row.add(1000000 + rowIndex);                                                       //  3 "integ integer"
        row.add(5555500000L + rowIndex);                                                   //  4 "bg    bigint"
        row.add(rowIndex + 0.0001f);                                                       //  5 "r     real"
        row.add(3.14159265358979d);                                                        //  6 "dp    double precision"
        row.add(String.format("12345678900000.00000%s", rowIndex));                        //  7 "dec   numeric"
        row.add(rowIndex % 2 != 0);                                                        //  8 "bool  boolean"
        row.add(String.format("2010-01-%02d", (rowIndex % 30) + 1));                       //  9 "cdate date"
        row.add(String.format("10:11:%02d", rowIndex % 60));                               // 10 "ctime time"
        row.add(String.format("2013-07-13 21:00:05.%03d456", rowIndex % 1000));            // 11 "tm    timestamp without time zone"
        row.add(String.format("2013-07-13 21:00:05.%03d123-07", rowIndex % 1000));         // 12 "tmz   timestamp with time zone"
        row.add("abc");                                                                    // 13 "c1    character(3)"
        row.add(" def ");                                                                  // 14 "vc1   character varying(5)"
        row.add(ByteBuffer.wrap(("b-" + rowIndex).getBytes(StandardCharsets.UTF_8)));      // 15 "bin   bytea"

        row.add(String.format("{t,f}", rowIndex));                                         // 16 "bool_arr     boolean[]"
        row.add(String.format("{1,2,3}", rowIndex));                                       // 17 "int2_arr     smallint[]"
        row.add(String.format("{1000000,2000000}", rowIndex));                             // 18 "int_arr      int[]"
        row.add(String.format("{7777700000,7777700001}", rowIndex));                       // 19 "int8_arr     bigint[]"
        row.add(String.format("{123.456,789.012}", rowIndex));                             // 20 "float_arr    real[]"
        row.add(String.format("{123.456789,789.123456}", rowIndex));                       // 21 "float8_arr   float[]"
        row.add(String.format("{12345678900000.000001,12345678900000.000001}", rowIndex)); // 22 "numeric_arr  numeric[]"
        row.add(String.format("{hello,world}", rowIndex));                                 // 23 "text_arr     text[]"
        row.add(String.format("{11,12}", rowIndex));                                       // 24 "bytea_arr    bytea[]"
        row.add(String.format("{abc,defij}", rowIndex));                                   // 25 "char_arr     bpchar(5)[]"
        row.add(String.format("{abcde,fijkl}", rowIndex));                                 // 26 "varchar_arr  varchar(5)[]"

        // overwrite null index
        if (nullColumnIndex >=0) {
            row.set(nullColumnIndex, null);
        }
        table.add(row);
    }

    /**
     * Asserts that a given set of deserialized data represented by a collection of OneField objects contains all data
     * in the sample dataset with correct values.
     * @param rows a list of lists of OneField object - list of rows where each row is a list of column values
     *             where each column value is a OneField object with type and value produced by a deserializer
     */
    public static void assertDataSet(List<List<OneField>> rows) {
        // produce the sample dataset in memory and compare its size with the dataset provided
        GPDataGenerator generator = new GPDataGenerator();
        assertEquals(generator.table.size(), rows.size(), "Sizes of the datasets do not match");

        // iterate over rows of the provided dataset
        Iterator<List<Object>> tableIterator = generator.table.iterator();
        int row = 0;
        for (List<OneField> serializedRow : rows) {
            List<Object> cells = tableIterator.next(); // pick a corresponding row from a sample dataset
            // iterate over columns of a row of the provided dataset
            int col = 0;
            for (OneField field : serializedRow) {
                // check the deserialized datatype
                assertEquals(COLUMNS[col].getDeserializedType().getOID(), field.type, String.format("Type mismatch row=%d col=%d", row, col));
                // check Java type / value
                if (cells.get(col) == null) {
                    assertNull(field.val, String.format("Expected null in row=%d col=%d", row, col));
                } else {
                    if (COLUMNS[col].getClazz() == ByteBuffer.class) {
                        assertTrue(ByteBuffer.class.isAssignableFrom(field.val.getClass()));
                    } else {
                        assertSame(COLUMNS[col].getClazz(), field.val.getClass(), String.format("Java class mismatch row=%d col=%d", row, col));
                    }
                    // make adjustments to expected values
                    Object expected = cells.get(col);
                    if (col == 25) {
                        expected = "{\"abc  \",defij}";
                    }
                    assertEquals(expected, field.val, String.format("Value mismatch row=%d col=%d", row, col));
                }
                col++;
            }
            row++;
        }
    }

    /**
     * Main method that generates '{project}/server/pxf-service/src/test/resources/data/sample_data.sql' file
     * with DDL and DML statements to create the sample dataset in a Greenplum table and copy it to a set of files
     * in different formats using the Greenplum COPY command.
     * @param args program arguments, not used
     */
    public static void main(String[] args) {
        GPDataGenerator generator = new GPDataGenerator();
        try (PrintStream output = new PrintStream(dir + "/sample_data.sql")) {
            generator.printTableDDL(output);
            generator.printInsertDataDML(output);
            generator.printCopyDataDML(output);
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper function to print the table DDL to the provided PrintStream.
     * @param out print stream to print output to
     */
    private void printTableDDL(PrintStream out) {
        // print out SQL DDL statements
        out.println("DROP TABLE IF EXISTS sample_data;");
        StringJoiner createTable = new StringJoiner(", ", "CREATE TABLE sample_data (", ") DISTRIBUTED BY (id);");
        for (Column column : COLUMNS) {
            createTable.add(column.getName() + " " + column.getSqlType());
        }
        out.println(createTable);
    }

    /**
     * Helper function to print the table INSERT DML to the provided PrintStream.
     * @param out print stream to print output to
     */
    private void printInsertDataDML(PrintStream out) {
        // print out SQL DML statements
        for (List<Object> row : table) {
            StringJoiner insertRow = new StringJoiner(", ", "INSERT INTO sample_data VALUES (", ");");
            for (Object column : row) {
                if (column == null) {
                    insertRow.add("NULL");
                } else if (column instanceof String) {
                    insertRow.add("'" + column + "'");
                } else if (column instanceof ByteBuffer) {
                    insertRow.add(String.format("'\\x%s'", Hex.encodeHexString((ByteBuffer) column)));
                } else {
                    insertRow.add(column.toString());
                }
            }
            out.println(insertRow);
        }
    }

    /**
     * Helper function to print the table COPY DDL to the provided PrintStream.
     * @param out print stream to print output to
     */
    private void printCopyDataDML(PrintStream out) {
        // print out SQL DML statements
        // use CTAS with ORDER BY to ensure an ordered set of rows in the output file
        out.println(String.format("COPY (SELECT * FROM sample_data ORDER BY id) TO '%s/sample_data.txt';", dir));
        out.println(String.format("COPY (SELECT * FROM sample_data ORDER BY id) TO '%s/sample_data.csv' CSV;", dir));
        out.println(String.format("COPY (SELECT * FROM sample_data ORDER BY id) TO '%s/sample_data_pipe.csv' CSV DELIMITER '|';", dir));
    }

}
