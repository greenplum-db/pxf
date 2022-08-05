package org.greenplum.pxf.plugins.hdfs.orc;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;

public class ORCVectorizedBaseTest {

    protected List<ColumnDescriptor> columnDescriptors;
    protected List<ColumnDescriptor> columnDescriptorsCompound;

    protected List<ColumnDescriptor> twoColumnDescriptors;

    // From resources/orc/orc_types.csv
    static final String[] COL1 = {"row1", "row2", "row3", "row4", "row5", "row6", "row7", "row8", "row9", "row10", "row11", "row12_text_null", "row13_int_null", "row14_double_null", "row15_decimal_null", "row16_timestamp_null", "row17_real_null", "row18_bigint_null", "row19_bool_null", "row20_tinyint_null", "row21_smallint_null", "row22_date_null", "row23_varchar_null", "row24_char_null", "row25_binary_null"};
    static final String[] COL2 = {"s_6", "s_7", "s_8", "s_9", "s_10", "s_11", "s_12", "s_13", "s_14", "s_15", "s_16", null, "s_16", "s_16", "s_17", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16"};
    static final Integer[] COL3 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, null, 11, 12, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11};
    static final Double[] COL4 = {6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 37.0, 37.0, 37.0, null, 38.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0};
    static final String[] COL5 = {"1.23456", "+1.23456", "-1.23456", "123456789.123456789", "0.000000000001", "00001234.8889999111", "0.0001", "45678.00002340089", "23457.1", "+45678.00002340089", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", null, "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679"};
    static final String[] COL6 = {"2013-07-13 21:00:05", "2013-07-13 21:00:05", "2013-07-15 21:00:05", "2013-07-16 21:00:05", "2013-07-17 21:00:05", "2013-07-18 21:00:05", "2013-07-19 21:00:05", "2013-07-20 21:00:05", "2013-07-21 21:00:05", "2013-07-22 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-24 21:00:05", null, "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05"};
    static final String[] COL7 = {"2020-06-28 11:30:00-07", "2020-06-27 19:00:00-07", "2020-06-27 23:00:00-07", "2020-06-27 18:00:00-07", "2020-06-27 17:30:00-07", "2020-06-27 22:00:00-07", "2020-06-27 15:45:00-07", "2020-06-27 14:45:00-07", "2020-06-28 08:00:00-07", "2020-06-28 07:00:00-07", "2020-06-28 00:00:00-07", "2020-06-27 22:45:00-07", "2020-06-28 14:00:00-07", "2020-06-28 05:30:00-07", "2020-06-28 03:30:00-07", "2020-06-28 08:30:00-07", "2020-06-28 09:30:00-07", "2020-06-28 10:30:00-07", "2020-06-27 21:30:00-07", "2020-06-28 12:30:00-07", "2020-06-27 20:30:00-07", "2020-06-28 14:30:00-07", "2020-06-28 15:30:00-07", "2020-06-28 16:30:00-07", null};
    static final Float[] COL8 = {7.7f, 8.7f, 9.7f, 10.7f, 11.7f, 12.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, null, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f};
    static final Long[] COL9 = {23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, null, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L};
    static final Boolean[] COL10 = {false, true, false, true, false, true, false, true, false, true, false, false, false, false, false, false, false, false, null, false, false, false, false, false, false};
    static final Short[] COL11 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, null, 11, 11, 11, 11, 1};
    static final Short[] COL12 = {10, 20, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100, null, 1100, 1100, 1100, 1100};
    static final String[] COL13 = {"2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", null, "2015-03-06", "2015-03-06", "2015-03-06"};
    static final String[] COL14 = {"abcd", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", null, "abcde", "abcde"};
    static final String[] COL15 = {"abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", null, "ab"};
    static final Byte[] COL16 = {0b00110001, 0b00110010, 0b00110011, 0b00110100, 0b00110101, 0b00110110, 0b00110111, 0b00111000, 0b00111001, 0b00110000, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, null};
    static final int[] ALL_ROWS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
    static final int[] NO_ROWS = {};
    static final Object[][] ORC_TYPES_DATASET = {COL1, COL2, COL3, COL4, COL5, COL6, COL7, COL8, COL9, COL10, COL11, COL12, COL13, COL14, COL15, COL16};
    // From resources/orc/orc_types_unordered_subset.csv
    static final String[] COL1_SUBSET = {"row1_file2", "row2_file2", "row3_file2", "row4_file2", "row5_file2", "row6_file2", "row7_file2", "row8_file2", "row9_file2", "row10_file2", "row11_file2", "row12_file2", "row13_int_null_file2", "row14_decimal_null_file2", "row15_timestamp_null_file2", "row16_bool_null_file2", "row17_varchar_null_file2"};
    static final Integer[] COL3_SUBSET = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, null, 12, 11, 11, 11};
    static final String[] COL5_SUBSET = {"1.23456", "+1.23456", "-1.23456", "123456789.123456789", "0.000000000001", "00001234.8889999111", "0.0001", "45678.00002340089", "23457.1", "+45678.00002340089", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", null, "0.123456789012345679", "0.123456789012345679", "0.123456789012345679"};
    static final String[] COL6_SUBSET = {"2013-07-13 21:00:05", "2013-07-13 21:00:05", "2013-07-15 21:00:05", "2013-07-16 21:00:05", "2013-07-17 21:00:05", "2013-07-18 21:00:05", "2013-07-19 21:00:05", "2013-07-20 21:00:05", "2013-07-21 21:00:05", "2013-07-22 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-24 21:00:05", null, "2013-07-23 21:00:05", "2013-07-23 21:00:05"};
    static final String[] COL7_SUBSET = {"2020-06-28 11:30:00-07", "2020-06-27 19:00:00-07", "2020-06-27 23:00:00-07", "2020-06-27 18:00:00-07", "2020-06-27 17:30:00-07", "2020-06-27 22:00:00-07", "2020-06-27 15:45:00-07", "2020-06-27 14:45:00-07", "2020-06-28 08:00:00-07", "2020-06-28 07:00:00-07", "2020-06-28 00:00:00-07", "2020-06-27 22:45:00-07", "2020-06-28 14:00:00-07", "2020-06-28 05:30:00-07", "2020-06-28 03:30:00-07", "2020-06-28 08:30:00-07", null};
    static final Boolean[] COL10_SUBSET = {false, true, false, true, false, true, false, true, false, true, false, false, false, false, false, null, false};
    static final Short[] COL12_SUBSET = {10, 20, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1100, 1100, 1100, 1100, 1100, 1100};
    static final String[] COL14_SUBSET = {"abcd", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", null};
    // From resources/orc/orc_types_repeated.csv
    static final String[] COL1_REPEATED = {"row1", "row2", "row3"};
    static final String[] COL2_REPEATED = {"s_6", "s_6", "s_6"};
    static final Integer[] COL3_REPEATED = {1, 1, 1};
    static final Double[] COL4_REPEATED = {6.0, 6.0, 6.0};
    static final String[] COL5_REPEATED = {"1.23456", "1.23456", "1.23456"};
    static final String[] COL6_REPEATED = {"2013-07-13 21:00:05", "2013-07-13 21:00:05", "2013-07-13 21:00:05"};
    static final String[] COL7_REPEATED = {"2020-06-28 11:30:00-07", "2020-06-28 11:30:00-07", "2020-06-28 11:30:00-07"};
    static final Float[] COL8_REPEATED = {7.7f, 7.7f, 7.7f};
    static final Long[] COL9_REPEATED = {23456789L, 23456789L, 23456789L};
    static final Boolean[] COL10_REPEATED = {true, true, true};
    static final Short[] COL11_REPEATED = {null, null, null};
    static final Short[] COL12_REPEATED = {10, 10, 10};
    static final String[] COL13_REPEATED = {"2015-03-06", "2015-03-06", "2015-03-06"};
    static final String[] COL14_REPEATED = {"abcd", "abcd", "abcd"};
    static final String[] COL15_REPEATED = {"abc", "abc", "abc"};
    static final Byte[] COL16_REPEATED = {0b00110001, 0b00110001, 0b00110001};
    static final Object[][] ORC_TYPES_REPEATED_DATASET = {COL1_REPEATED, COL2_REPEATED, COL3_REPEATED, COL4_REPEATED, COL5_REPEATED, COL6_REPEATED, COL7_REPEATED, COL8_REPEATED, COL9_REPEATED, COL10_REPEATED, COL11_REPEATED, COL12_REPEATED, COL13_REPEATED, COL14_REPEATED, COL15_REPEATED, COL16_REPEATED};
    // From resources/orc/generate_orc_types_compound.hql
    static final Integer[] COL1_COMPOUND = {1, 2, 3, 4, 5, 6};
    static final String[] BOOL_LIST = {"{}", "{0,1,1,0}", "{1}", null, "{1,0}", "{1,0,null}"}; // orc uses long to store bool values
    static final String[] INT2_LIST = {"{50}", "{}", "{-128}", "{10,20}", null, "{0,127,-128}"};
    static final String[] INTEGER_LIST = {"{1}", "{2,3}", null, "{7,null,8}", "{}", "{2147483647,-2147483648}"};
    static final String[] INT8_LIST = {"{1}", null, "{}", "{-9223372036854775808,0}", "{null,9223372036854775807}", "{1,null,300}"};
    static final String[] FLOAT_LIST = {null, "{}", "{-123456.984375,9.007199254740992E15}", "{2.299999952316284,4.5}", "{6.699999809265137,-8.0,null}", "{9.9999998245167E-15}"};
    static final String[] FLOAT8_LIST = {"{1.7E308}", "{1.0}", "{5.678,9.10234}", null, "{}", "{null,8.431,-1.56}"};
    // for text, there is a bug in Hive which inserts in an empty array from another table as an array with an empty string.
    static final String[] TEXT_LIST = {"{\"this is a test string\"}", "{\"this is a string with \\\"special\\\" characters\",\"this is a string without\"}", "{hello,\"the next element is a string that says null\",\"null\"}", "{NULL,\"\"}", null, "{\"this is a test string with \\\\ and \\\"\",NULL}"};
    static final String[] BYTEA_LIST = {null, "{}", "{\"\\\\xdeadbeef\"}", "{NULL,\"\\\\x5c22\"}", "{\"\\\\x5c5c5c\",NULL}", "{\"\\\\x313233\",\"\\\\x343536\"}"};
    // for bpchar and varchar as well, there is a bug in Hive which inserts in an empty array from another table as an array with an empty string.
    // for bpchar and varchar, ORC knows about the character limit in the schema, but will only store the original string in the file (without the additional whitespaces appended)
    static final String[] BPCHAR_LIST = {"{hello}", "{\"this is exactly\",\" fifteen chars.\"}", "{\"\"}", null, "{\"specials \\\\ \\\"\"}", "{\"test string\",NULL}"};
    static final String[] VARCHAR_LIST = {"{hello}", "{\"this is exactly\",\" fifteen chars.\"}", "{\"\"}", null, "{\"specials \\\\ \\\"\"}", "{\"test string\",NULL}"};
    static final String[] DATE_LIST = {"{2015-03-06}", "{2015-03-06,2015-03-07}", "{2015-03-08,2015-03-09,2015-03-10}", "{2015-03-11,NULL,2015-03-12}", "{}", null};
    static final String[] TIMESTAMP_LIST = {null, "{\"2013-07-13 21:00:05\"}", "{\"2013-07-13 21:00:05\",\"2013-07-15 22:00:05\"}", "{}", "{\"2013-07-17 23:00:05\",\"2013-07-18 20:00:05\",NULL}", "{\"2013-07-17 09:00:05\",NULL,\"2013-07-18 10:00:05\"}"};
    static final String[] TIMESTAMP_WITH_TIMEZONE_LIST = {null, "{\"2013-07-13 21:00:05-07\"}", "{\"2013-07-13 21:00:05-07\",\"2013-07-15 22:00:05-07\"}", "{}", "{\"2013-07-17 23:00:05-07\",\"2013-07-18 20:00:05-07\",NULL}", "{\"2013-07-17 09:00:05-07\",NULL,\"2013-07-18 10:00:05-07\"}"};

    static final Object[][] ORC_COMPOUND_TYPES_DATASET = {COL1_COMPOUND, BOOL_LIST, INT2_LIST, INTEGER_LIST, INT8_LIST, FLOAT_LIST, FLOAT8_LIST, TEXT_LIST, BYTEA_LIST, BPCHAR_LIST, VARCHAR_LIST, DATE_LIST, TIMESTAMP_LIST, TIMESTAMP_WITH_TIMEZONE_LIST};

    // From resources/orc/generate_orc_types_compound_multi.hql
    // postgres cannot support multi-dimensional arrays with subarrays of different sizes nor can it support nulls in the form of {{2,3},null,{4,5}}
    // while PXF can handle that sort of data, we will just allow it to error out on the GPDB side
    static final Integer[] COL1_COMPOUND_MULTI = {1, 2, 3, 4, 5, 6};
    static final String[] BOOL_LIST_MULTI = {"{}", "{{0,1},{1,0}}", "{{1}}", null, "{{1,0}}", "{{1,0},null}"}; // orc uses long to store bool values
    static final String[] INT2_LIST_MULTI = {"{{50}}", "{}", "{{-128}}", "{{10,20}}", null, "{{0,127},null}"};
    static final String[] INTEGER_LIST_MULTI = {"{{1}}", "{{2,3},null,{4,5}}", null, "{{7,null},{8}}", "{}", "{{2147483647,-2147483648}}"};
    static final String[] INT8_LIST_MULTI = {"{{1}}", null, "{}", "{{-9223372036854775808,0}}", "{null,{9223372036854775807}}", "{{1,null},{300}}"};
    static final String[] FLOAT_LIST_MULTI = {null, "{}", "{{-123456.984375,9.007199254740992E15}}", "{{2.299999952316284},{4.5}}", "{{6.699999809265137,-8.0},null}", "{{9.9999998245167E-15}}"};
    static final String[] FLOAT8_LIST_MULTI = {"{{1.7E308}}", "{{1.0}}", "{{5.678},{9.10234}}", null, "{}", "{{null,8.431},{-1.56,0.001}}"};
    static final String[] TEXT_LIST_MULTI = {"{{\"this is a test string\"}}", "{{\"this is a string with \\\"special\\\" characters\"},{\"this is a string without\"}}", "{{hello,world},{\"the next element is a string that says null\",\"null\"}}", "{}", null, "{{\"this is a test string with \\\\ and \\\"\",NULL}}"};
    static final String[] BYTEA_LIST_MULTI = {null, "{}", "{{\"\\\\xdeadbeef\"}}", "{{NULL,\"\\\\x5c22\"}}", "{{\"\\\\x5c5c5c\",\"\\\\x5b48495d\"},null}", "{{\"\\\\x313233\"},{\"\\\\x343536\"}}"};
    static final String[] BPCHAR_LIST_MULTI = {"{{hello}}", "{{\"this is exactly\"},{\" fifteen chars.\"}}", "{}", null, "{{\"specials \\\\ \\\"\"},null}", "{{\"test string\",NULL},{\"2 whitespace\",\"no whitespace\"}}"};
    static final String[] VARCHAR_LIST_MULTI = {"{{hello}}", "{{\"this is exactly\"},{\" fifteen chars.\"}}", "{}", null, "{{\"specials \\\\ \\\"\"},null}", "{{\"test string\",NULL},{\"2 whitespace  \",\"no whitespace\"}}"};
    static final String[] DATE_LIST_MULTI = {"{{2015-03-06}}", "{{2015-03-06},{2015-03-07}}", "{{2015-03-08,2015-03-09},{2015-03-10,NULL}}", "{{2015-03-11},{NULL},{2015-03-12}}", "{{}}", null};
    static final String[] TIMESTAMP_LIST_MULTI = {null, "{{\"2013-07-13 21:00:05\"}}", "{{\"2013-07-13 21:00:05\"},{\"2013-07-15 22:00:05\"}}", "{{}}", "{{NULL,\"2013-07-17 23:00:05\"},{\"2013-07-18 20:00:05\",NULL}}", "{{\"2013-07-17 09:00:05\"},{NULL},{\"2013-07-18 10:00:05\"}}"};
    static final String[] TIMESTAMP_WITH_TIMEZONE_LIST_MULTI = {null, "{{\"2013-07-13 21:00:05-07\"}}", "{{\"2013-07-13 21:00:05-07\"},{\"2013-07-15 22:00:05-07\"}}", "{{}}", "{{NULL,\"2013-07-17 23:00:05-07\"},{\"2013-07-18 20:00:05-07\",NULL}}", "{{\"2013-07-17 09:00:05-07\"},{NULL},{\"2013-07-18 10:00:05-07\"}}"};

    static final Object[][] ORC_COMPOUND_MULTI_TYPES_DATASET = {COL1_COMPOUND_MULTI, BOOL_LIST_MULTI, INT2_LIST_MULTI, INTEGER_LIST_MULTI, INT8_LIST_MULTI, FLOAT_LIST_MULTI, FLOAT8_LIST_MULTI, TEXT_LIST_MULTI, BYTEA_LIST_MULTI, BPCHAR_LIST_MULTI, VARCHAR_LIST_MULTI, DATE_LIST_MULTI, TIMESTAMP_LIST_MULTI, TIMESTAMP_WITH_TIMEZONE_LIST_MULTI};


    @BeforeEach
    public void setup() {
        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("t1", DataType.TEXT.getOID(), 0, "text", null));
        columnDescriptors.add(new ColumnDescriptor("t2", DataType.TEXT.getOID(), 1, "text", null));
        columnDescriptors.add(new ColumnDescriptor("num1", DataType.INTEGER.getOID(), 2, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("dub1", DataType.FLOAT8.getOID(), 3, "float8", null));
        columnDescriptors.add(new ColumnDescriptor("dec1", DataType.NUMERIC.getOID(), 4, "numeric", new Integer[]{38, 18}));
        columnDescriptors.add(new ColumnDescriptor("tm", DataType.TIMESTAMP.getOID(), 5, "timestamp", null));
        columnDescriptors.add(new ColumnDescriptor("tmtz", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), 6, "timestamp with time zone", null));
        columnDescriptors.add(new ColumnDescriptor("r", DataType.REAL.getOID(), 7, "real", null));
        columnDescriptors.add(new ColumnDescriptor("bg", DataType.BIGINT.getOID(), 8, "int8", null));
        columnDescriptors.add(new ColumnDescriptor("b", DataType.BOOLEAN.getOID(), 9, "bool", null));
        columnDescriptors.add(new ColumnDescriptor("tn", DataType.SMALLINT.getOID(), 10, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("sml", DataType.SMALLINT.getOID(), 11, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("dt", DataType.DATE.getOID(), 12, "date", null));
        columnDescriptors.add(new ColumnDescriptor("vc1", DataType.VARCHAR.getOID(), 13, "varchar", new Integer[]{5}));
        columnDescriptors.add(new ColumnDescriptor("c1", DataType.BPCHAR.getOID(), 14, "bpchar", new Integer[]{3}));
        columnDescriptors.add(new ColumnDescriptor("bin", DataType.BYTEA.getOID(), 15, "bin", null));

        columnDescriptorsCompound = new ArrayList<>();
        columnDescriptorsCompound.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("bool_arr", DataType.BOOLARRAY.getOID(), 1, "bool[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("int2_arr", DataType.INT2ARRAY.getOID(), 2, "smallint[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("int_arr", DataType.INT4ARRAY.getOID(), 3, "int[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("int8_arr", DataType.INT8ARRAY.getOID(), 4, "bigint[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("float_arr", DataType.FLOAT4ARRAY.getOID(), 5, "float[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("float8_arr", DataType.FLOAT8ARRAY.getOID(), 6, "float8[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("text_arr", DataType.TEXTARRAY.getOID(), 7, "text[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("bytea_arr", DataType.BYTEAARRAY.getOID(), 8, "bytea[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("char_arr", DataType.BPCHARARRAY.getOID(), 9, "bpchar(15)[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("varchar_arr", DataType.VARCHARARRAY.getOID(), 10, "varchar(15)[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("date_arr", DataType.DATEARRAY.getOID(), 11, "date[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("timestamp_arr", DataType.TIMESTAMPARRAY.getOID(), 12, "timestamp[]", null));
        columnDescriptorsCompound.add(new ColumnDescriptor("tmtz_arr", DataType.TIMESTAMP_WITH_TIMEZONE_ARRAY.getOID(), 13, "timestamp with time zone[]", null));

        twoColumnDescriptors = new ArrayList<>();
        twoColumnDescriptors.add(new ColumnDescriptor("col0", DataType.TEXT.getOID(), 0, "text", null));
        twoColumnDescriptors.add(new ColumnDescriptor("col1", DataType.INTEGER.getOID(), 1, "int4", null));

    }

}
