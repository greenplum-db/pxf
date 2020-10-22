package org.greenplum.pxf.plugins.hdfs.orc;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;

public class ORCVectorizedBaseTest {

    protected List<ColumnDescriptor> columnDescriptors;

    // From resources/orc/orc_types.csv
    static final String[] COL1 = {"row1", "row2", "row3", "row4", "row5", "row6", "row7", "row8", "row9", "row10", "row11", "row12_text_null", "row13_int_null", "row14_double_null", "row15_decimal_null", "row16_timestamp_null", "row17_real_null", "row18_bigint_null", "row19_bool_null", "row20_tinyint_null", "row21_smallint_null", "row22_date_null", "row23_varchar_null", "row24_char_null", "row25_binary_null"};
    static final String[] COL2 = {"s_6", "s_7", "s_8", "s_9", "s_10", "s_11", "s_12", "s_13", "s_14", "s_15", "s_16", null, "s_16", "s_16", "s_17", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16", "s_16"};
    static final Integer[] COL3 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, null, 11, 12, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11};
    static final Double[] COL4 = {6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 37.0, 37.0, 37.0, null, 38.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0, 37.0};
    static final String[] COL5 = {"1.23456", "+1.23456", "-1.23456", "123456789.123456789", "0.000000000001", "00001234.8889999111", "0.0001", "45678.00002340089", "23457.1", "+45678.00002340089", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", null, "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679"};
    static final String[] COL6 = {"2013-07-13 21:00:05", "2013-07-13 21:00:05", "2013-07-15 21:00:05", "2013-07-16 21:00:05", "2013-07-17 21:00:05", "2013-07-18 21:00:05", "2013-07-19 21:00:05", "2013-07-20 21:00:05", "2013-07-21 21:00:05", "2013-07-22 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-24 21:00:05", null, "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05"};
    static final Float[] COL7 = {7.7f, 8.7f, 9.7f, 10.7f, 11.7f, 12.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, null, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f, 7.7f};
    static final Long[] COL8 = {23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, null, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L, 23456789L};
    static final Boolean[] COL9 = {false, true, false, true, false, true, false, true, false, true, false, false, false, false, false, false, false, false, null, false, false, false, false, false, false};
    static final Short[] COL10 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, null, 11, 11, 11, 11, 1};
    static final Short[] COL11 = {10, 20, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100, null, 1100, 1100, 1100, 1100};
    static final String[] COL12 = {"2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", "2015-03-06", null, "2015-03-06", "2015-03-06", "2015-03-06"};
    static final String[] COL13 = {"abcd", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", null, "abcde", "abcde"};
    static final String[] COL14 = {"abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "abc", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", null, "ab"};
    static final Byte[] COL15 = {0b00110001, 0b00110010, 0b00110011, 0b00110100, 0b00110101, 0b00110110, 0b00110111, 0b00111000, 0b00111001, 0b00110000, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, null};
    static final int[] ALL_ROWS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
    static final int[] NO_ROWS = {};

    // From resources/orc/orc_types_unordered_subset.csv
    static final String[] COL1_SUBSET = {"row1_file2", "row2_file2", "row3_file2", "row4_file2", "row5_file2", "row6_file2", "row7_file2", "row8_file2", "row9_file2", "row10_file2", "row11_file2", "row12_file2", "row13_int_null_file2", "row14_decimal_null_file2", "row15_timestamp_null_file2", "row16_bool_null_file2", "row17_varchar_null_file2"};
    static final Integer[] COL3_SUBSET = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, null, 12, 11, 11, 11};
    static final String[] COL5_SUBSET = {"1.23456", "+1.23456", "-1.23456", "123456789.123456789", "0.000000000001", "00001234.8889999111", "0.0001", "45678.00002340089", "23457.1", "+45678.00002340089", "0.123456789012345679", "0.123456789012345679", "0.123456789012345679", null, "0.123456789012345679", "0.123456789012345679", "0.123456789012345679"};
    static final String[] COL6_SUBSET = {"2013-07-13 21:00:05", "2013-07-13 21:00:05", "2013-07-15 21:00:05", "2013-07-16 21:00:05", "2013-07-17 21:00:05", "2013-07-18 21:00:05", "2013-07-19 21:00:05", "2013-07-20 21:00:05", "2013-07-21 21:00:05", "2013-07-22 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-23 21:00:05", "2013-07-24 21:00:05", null, "2013-07-23 21:00:05", "2013-07-23 21:00:05"};
    static final Boolean[] COL9_SUBSET = {false, true, false, true, false, true, false, true, false, true, false, false, false, false, false, null, false};
    static final Short[] COL11_SUBSET = {10, 20, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1100, 1100, 1100, 1100, 1100, 1100};
    static final String[] COL13_SUBSET = {"abcd", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", "abcde", null};

    @BeforeEach
    public void setup() {
        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("t1", DataType.TEXT.getOID(), 0, "text", null));
        columnDescriptors.add(new ColumnDescriptor("t2", DataType.TEXT.getOID(), 1, "text", null));
        columnDescriptors.add(new ColumnDescriptor("num1", DataType.INTEGER.getOID(), 2, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("dub1", DataType.FLOAT8.getOID(), 3, "float8", null));
        columnDescriptors.add(new ColumnDescriptor("dec1", DataType.NUMERIC.getOID(), 4, "numeric", new Integer[]{38, 18}));
        columnDescriptors.add(new ColumnDescriptor("tm", DataType.TIMESTAMP.getOID(), 5, "timestamp", null));
        columnDescriptors.add(new ColumnDescriptor("r", DataType.REAL.getOID(), 6, "real", null));
        columnDescriptors.add(new ColumnDescriptor("bg", DataType.BIGINT.getOID(), 7, "int8", null));
        columnDescriptors.add(new ColumnDescriptor("b", DataType.BOOLEAN.getOID(), 8, "bool", null));
        columnDescriptors.add(new ColumnDescriptor("tn", DataType.SMALLINT.getOID(), 9, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("sml", DataType.SMALLINT.getOID(), 10, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("dt", DataType.DATE.getOID(), 11, "date", null));
        columnDescriptors.add(new ColumnDescriptor("vc1", DataType.VARCHAR.getOID(), 12, "varchar", new Integer[]{5}));
        columnDescriptors.add(new ColumnDescriptor("c1", DataType.BPCHAR.getOID(), 13, "bpchar", new Integer[]{3}));
        columnDescriptors.add(new ColumnDescriptor("bin", DataType.BYTEA.getOID(), 14, "bin", null));
    }
}
