package org.greenplum.pxf.plugins.json;

import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonResolverTest {

    private JsonResolver resolver;
    private RequestContext context;
    private final ArrayList<ColumnDescriptor> schema = generateJsonSchema();

    @BeforeEach
    public void setUp() {
        PgUtilities pgUtilities = new PgUtilities();
        resolver = new JsonResolver(pgUtilities);
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setTupleDescription(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
    }

    @Test
    public void testInitialize() {
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
    }

    @Test
    public void testGetFieldsWithUnquotedValues() throws Exception {

        //language=JSON
        String jsonStr = "{" +
                "\"type_int\":100000001," +
                "\"type_bigint\":10101010101," +
                "\"type_smallint\":13," +
                "\"type_float\":1.1," +
                "\"type_double\":1.1," +
                "\"type_string1\":\"testing all supported types in JsonResolver 1\"," +
                "\"type_string2\":\"testing all supported types in JsonResolver 2\"," +
                "\"type_string3\":\"testing all supported types in JsonResolver 3\"," +
                "\"type_char\":\"z\"," +
                "\"type_boolean\":true," +
                "\"type_array\":[\"member 1\", \"member 2\"]," +
                "\"type_int_arr_as_text\":[1,2,3]," +
                "\"type_bigint_arr_as_text\":[null,10101010101]," +
                "\"type_smallint_arr_as_text\":[5,null]," +
                "\"type_float_arr_as_text\":[1.1]," +
                "\"type_double_arr_as_text\":[1.1,2.2]," +
                "\"type_string_arr_as_text\":[\"hello\",\"world\"]," +
                "\"type_boolean_arr_as_text\":[true,false,null]," +
                "\"type_string_arr_arr_as_text\":[[\"good\",\"morning\"],null,[\"guten\",\"morgen\"]]," +
                "\"type_object_arr_as_text\":[{\"id\":1,\"val\":\"a\"},{\"id\":2,\"val\":\"b\"}]," +
                "\"type_object_as_text\":{\"id\":1,\"val\":\"a\",\"arr\":[1,2],\"obj\":{\"a\":55}}," +
                "\"type_null_as_text\":null," +
                "\"type_empty_arr_as_text\":[]," +
                "\"type_empty_object_as_text\":{}," +
                "\"type_array_as_textarray\":[\"member 1\", \"member 2\"]," +
                "\"type_int_arr_as_textarray\":[1,2,3]," +
                "\"type_bigint_arr_as_textarray\":[null,10101010101]," +
                "\"type_smallint_arr_as_textarray\":[5,null]," +
                "\"type_float_arr_as_textarray\":[1.1]," +
                "\"type_double_arr_as_textarray\":[1.1,2.2]," +
                "\"type_string_arr_as_textarray\":[\"hello\",\"world\"]," +
                "\"type_boolean_arr_as_textarray\":[true,false,null]," +
                "\"type_string_arr_arr_as_textarray\":[[\"good\",\"morning\"],null,[\"guten\",\"morgen\"]]," +
                "\"type_object_arr_as_textarray\":[{\"id\":1,\"val\":\"a\"},{\"id\":2,\"val\":\"b\"}]," +
                "\"type_null_as_textarray\":null," +
                "\"type_empty_arr_as_textarray\":[]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        List<OneField> fields = assertRow(row, 38);
        assertField(fields, 0, 100000001, DataType.INTEGER);
        assertField(fields, 1, 10101010101L, DataType.BIGINT);
        assertField(fields, 2, (short) 13, DataType.SMALLINT);
        assertField(fields, 3, (float) 1.1, DataType.REAL);
        assertField(fields, 4, 1.1, DataType.FLOAT8);
        assertField(fields, 5, "testing all supported types in JsonResolver 1", DataType.TEXT);
        assertField(fields, 6, "testing all supported types in JsonResolver 2", DataType.VARCHAR);
        assertField(fields, 7, "testing all supported types in JsonResolver 3", DataType.BPCHAR);
        assertField(fields, 8, "z", DataType.BPCHAR);
        assertField(fields, 9, true, DataType.BOOLEAN);
        assertField(fields, 10, "member 1", DataType.TEXT);
        assertField(fields, 11, "member 2", DataType.TEXT);
        assertField(fields, 12, "[\"member 1\",\"member 2\"]", DataType.TEXT);
        assertField(fields, 13, "[1,2,3]", DataType.TEXT);
        assertField(fields, 14, "[null,10101010101]", DataType.TEXT);
        assertField(fields, 15, "[5,null]", DataType.TEXT);
        assertField(fields, 16, "[1.1]", DataType.TEXT);
        assertField(fields, 17, "[1.1,2.2]", DataType.TEXT);
        assertField(fields, 18, "[\"hello\",\"world\"]", DataType.TEXT);
        assertField(fields, 19, "[true,false,null]", DataType.TEXT);
        assertField(fields, 20, "[[\"good\",\"morning\"],null,[\"guten\",\"morgen\"]]", DataType.TEXT);
        assertField(fields, 21, "[{\"id\":1,\"val\":\"a\"},{\"id\":2,\"val\":\"b\"}]", DataType.TEXT);
        assertField(fields, 22, "{\"id\":1,\"val\":\"a\",\"arr\":[1,2],\"obj\":{\"a\":55}}", DataType.TEXT);
        assertField(fields, 23, null, DataType.TEXT);
        assertField(fields, 24, "[]", DataType.TEXT);
        assertField(fields, 25, "{}", DataType.TEXT);
        assertField(fields, 26, "{\"member 1\",\"member 2\"}", DataType.TEXTARRAY);
        assertField(fields, 27, "{1,2,3}", DataType.TEXTARRAY);
        assertField(fields, 28, "{NULL,10101010101}", DataType.TEXTARRAY);
        assertField(fields, 29, "{5,NULL}", DataType.TEXTARRAY);
        assertField(fields, 30, "{1.1}", DataType.TEXTARRAY);
        assertField(fields, 31, "{1.1,2.2}", DataType.TEXTARRAY);
        assertField(fields, 32, "{hello,world}", DataType.TEXTARRAY);
        assertField(fields, 33, "{true,false,NULL}", DataType.TEXTARRAY);
        assertField(fields, 34, "{{good,morning},NULL,{guten,morgen}}", DataType.TEXTARRAY);
        assertField(fields, 35, "{\"{\\\"id\\\":1,\\\"val\\\":\\\"a\\\"}\",\"{\\\"id\\\":2,\\\"val\\\":\\\"b\\\"}\"}", DataType.TEXTARRAY);
        assertField(fields, 36, null, DataType.TEXTARRAY);
        assertField(fields, 37, "{}", DataType.TEXTARRAY);
    }

    @Test
    public void testGetFieldsWithQuotedValues() throws Exception {

        //language=JSON
        String jsonStr = "{" +
                "\"type_int\":\"-200000002\"," +
                "\"type_bigint\":\"20202020202\"," +
                "\"type_smallint\":\"26\"," +
                "\"type_float\":\"-2.2\"," +
                "\"type_double\":\"-2.2\"," +
                "\"type_string1\":\"testing all supported types in JsonResolver A\"," +
                "\"type_string2\":\"testing all supported types in JsonResolver B\"," +
                "\"type_string3\":\"testing all supported types in JsonResolver C\"," +
                "\"type_char\":\"1\"," +
                "\"type_boolean\":\"false\"," +
                "\"type_array\":[\"member 1\", \"member 2\"]," +
                "\"type_int_arr_as_text\":[\"1\",\"2\",\"3\"]," +
                "\"type_bigint_arr_as_text\":[null,\"10101010101\"]," +
                "\"type_smallint_arr_as_text\":[\"5\",\"null\"]," +
                "\"type_float_arr_as_text\":[\"1.1\"]," +
                "\"type_double_arr_as_text\":[\"1.1\",\"2.2\"]," +
                "\"type_string_arr_as_text\":[\"hello\",\"world\"]," +
                "\"type_boolean_arr_as_text\":[\"true\",\"false\",null]," +
                "\"type_string_arr_arr_as_text\":[[\"good\",\"morning\"],null,[\"guten\",\"morgen\"]]," +
                "\"type_object_arr_as_text\":[{\"id\":1,\"val\":\"a\"},{\"id\":2,\"val\":\"b\"}]," +
                "\"type_object_as_text\":{\"id\":1,\"val\":\"a\",\"arr\":[1,2],\"obj\":{\"a\":55}}," +
                "\"type_null_as_text\":\"null\"," +
                "\"type_empty_arr_as_text\":[]," +
                "\"type_empty_object_as_text\":{}," +
                "\"type_array_as_textarray\":[\"member 1\", \"member 2\"]," +
                "\"type_int_arr_as_textarray\":[\"1\",\"2\",\"3\"]," +
                "\"type_bigint_arr_as_textarray\":[null,\"10101010101\"]," +
                "\"type_smallint_arr_as_textarray\":[\"5\",\"null\"]," +
                "\"type_float_arr_as_textarray\":[\"1.1\"]," +
                "\"type_double_arr_as_textarray\":[\"1.1\",\"2.2\"]," +
                "\"type_string_arr_as_textarray\":[\"hello\",\"world\"]," +
                "\"type_boolean_arr_as_textarray\":[\"true\",\"false\",null]," +
                "\"type_string_arr_arr_as_textarray\":[[\"good\",\"morning\"],null,[\"guten\",\"morgen\"]]," +
                "\"type_object_arr_as_textarray\":[{\"id\":1,\"val\":\"a\"},{\"id\":2,\"val\":\"b\"}]," +
                "\"type_null_as_textarray\":null," + // quoted null doesn't really make sense
                "\"type_empty_arr_as_textarray\":[]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        List<OneField> fields = assertRow(row, 38);
        assertField(fields, 0, -200000002, DataType.INTEGER);
        assertField(fields, 1, 20202020202L, DataType.BIGINT);
        assertField(fields, 2, (short) 26, DataType.SMALLINT);
        assertField(fields, 3, (float) -2.2, DataType.REAL);
        assertField(fields, 4, -2.2, DataType.FLOAT8);
        assertField(fields, 5, "testing all supported types in JsonResolver A", DataType.TEXT);
        assertField(fields, 6, "testing all supported types in JsonResolver B", DataType.VARCHAR);
        assertField(fields, 7, "testing all supported types in JsonResolver C", DataType.BPCHAR);
        assertField(fields, 8, "1", DataType.BPCHAR);
        assertField(fields, 9, false, DataType.BOOLEAN);
        assertField(fields, 10, "member 1", DataType.TEXT);
        assertField(fields, 11, "member 2", DataType.TEXT);
        assertField(fields, 12, "[\"member 1\",\"member 2\"]", DataType.TEXT);
        assertField(fields, 13, "[\"1\",\"2\",\"3\"]", DataType.TEXT);
        assertField(fields, 14, "[null,\"10101010101\"]", DataType.TEXT);
        assertField(fields, 15, "[\"5\",\"null\"]", DataType.TEXT); // that is not quite correct, but input is incorrect as well
        assertField(fields, 16, "[\"1.1\"]", DataType.TEXT);
        assertField(fields, 17, "[\"1.1\",\"2.2\"]", DataType.TEXT);
        assertField(fields, 18, "[\"hello\",\"world\"]", DataType.TEXT);
        assertField(fields, 19, "[\"true\",\"false\",null]", DataType.TEXT);
        assertField(fields, 20, "[[\"good\",\"morning\"],null,[\"guten\",\"morgen\"]]", DataType.TEXT);
        assertField(fields, 21, "[{\"id\":1,\"val\":\"a\"},{\"id\":2,\"val\":\"b\"}]", DataType.TEXT);
        assertField(fields, 22, "{\"id\":1,\"val\":\"a\",\"arr\":[1,2],\"obj\":{\"a\":55}}", DataType.TEXT);
        assertField(fields, 23, "null", DataType.TEXT);
        assertField(fields, 24, "[]", DataType.TEXT);
        assertField(fields, 25, "{}", DataType.TEXT);
        assertField(fields, 26, "{\"member 1\",\"member 2\"}", DataType.TEXTARRAY);
        assertField(fields, 27, "{1,2,3}", DataType.TEXTARRAY);
        assertField(fields, 28, "{NULL,10101010101}", DataType.TEXTARRAY);
        assertField(fields, 29, "{5,\"null\"}", DataType.TEXTARRAY); // that is not quite correct, but input is incorrect as well
        assertField(fields, 30, "{1.1}", DataType.TEXTARRAY);
        assertField(fields, 31, "{1.1,2.2}", DataType.TEXTARRAY);
        assertField(fields, 32, "{hello,world}", DataType.TEXTARRAY);
        assertField(fields, 33, "{true,false,NULL}", DataType.TEXTARRAY);
        assertField(fields, 34, "{{good,morning},NULL,{guten,morgen}}", DataType.TEXTARRAY);
        assertField(fields, 35, "{\"{\\\"id\\\":1,\\\"val\\\":\\\"a\\\"}\",\"{\\\"id\\\":2,\\\"val\\\":\\\"b\\\"}\"}", DataType.TEXTARRAY);
        assertField(fields, 36, null, DataType.TEXTARRAY);
        assertField(fields, 37, "{}", DataType.TEXTARRAY);
    }

    @Test
    public void testGetFieldsShouldFailOnMismatchedInt() {

        String jsonStr = "{" +
                "\"type_int\":\"[\"," +
                "\"type_bigint\":\"20202020202\"," +
                "\"type_smallint\":\"26\"," +
                "\"type_float\":\"-2.2\"," +
                "\"type_double\":\"-2.2\"," +
                "\"type_string1\":\"testing all supported types in JsonResolver A\"," +
                "\"type_string2\":\"testing all supported types in JsonResolver B\"," +
                "\"type_string3\":\"testing all supported types in JsonResolver C\"," +
                "\"type_char\":\"1\"," +
                "\"type_boolean\":\"false\"," +
                "\"type_array\":[\"member 1\", \"member 2\"]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> assertRow(row, 12));
        assertEquals("invalid INTEGER input value '\"[\"'", e.getMessage());
    }

    @Test
    public void testGetFieldsShouldFailOnMismatchedBigInt() {

        String jsonStr = "{" +
                "\"type_int\":\"1234\"," +
                "\"type_bigint\":\"garbage number\"," +
                "\"type_smallint\":\"26\"," +
                "\"type_float\":\"-2.2\"," +
                "\"type_double\":\"-2.2\"," +
                "\"type_string1\":\"testing all supported types in JsonResolver A\"," +
                "\"type_string2\":\"testing all supported types in JsonResolver B\"," +
                "\"type_string3\":\"testing all supported types in JsonResolver C\"," +
                "\"type_char\":\"1\"," +
                "\"type_boolean\":\"false\"," +
                "\"type_array\":[\"member 1\", \"member 2\"]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> assertRow(row, 12));
        assertEquals("invalid BIGINT input value '\"garbage number\"'", e.getMessage());
    }

    @Test
    public void testGetFieldsShouldFailOnMismatchedSmallInt() {

        String jsonStr = "{" +
                "\"type_int\":\"1234\"," +
                "\"type_bigint\":\"20202020202\"," +
                "\"type_smallint\":\"not small int\"," +
                "\"type_float\":\"-2.2\"," +
                "\"type_double\":\"-2.2\"," +
                "\"type_string1\":\"testing all supported types in JsonResolver A\"," +
                "\"type_string2\":\"testing all supported types in JsonResolver B\"," +
                "\"type_string3\":\"testing all supported types in JsonResolver C\"," +
                "\"type_char\":\"1\"," +
                "\"type_boolean\":\"false\"," +
                "\"type_array\":[\"member 1\", \"member 2\"]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> assertRow(row, 12));
        assertEquals("invalid SMALLINT input value '\"not small int\"'", e.getMessage());
    }

    @Test
    public void testGetFieldsShouldFailOnMismatchedFloat() {

        String jsonStr = "{" +
                "\"type_int\":\"1234\"," +
                "\"type_bigint\":\"20202020202\"," +
                "\"type_smallint\":\"26\"," +
                "\"type_float\":\"root beer float\"," +
                "\"type_double\":\"-2.2\"," +
                "\"type_string1\":\"testing all supported types in JsonResolver A\"," +
                "\"type_string2\":\"testing all supported types in JsonResolver B\"," +
                "\"type_string3\":\"testing all supported types in JsonResolver C\"," +
                "\"type_char\":\"1\"," +
                "\"type_boolean\":\"false\"," +
                "\"type_array\":[\"member 1\", \"member 2\"]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> assertRow(row, 12));
        assertEquals("invalid REAL input value '\"root beer float\"'", e.getMessage());
    }

    @Test
    public void testGetFieldsShouldFailOnMismatchedDouble() {

        String jsonStr = "{" +
                "\"type_int\":\"1234\"," +
                "\"type_bigint\":\"20202020202\"," +
                "\"type_smallint\":\"26\"," +
                "\"type_float\":\"-2.2\"," +
                "\"type_double\":\"2 root beer floats\"," +
                "\"type_string1\":\"testing all supported types in JsonResolver A\"," +
                "\"type_string2\":\"testing all supported types in JsonResolver B\"," +
                "\"type_string3\":\"testing all supported types in JsonResolver C\"," +
                "\"type_char\":\"1\"," +
                "\"type_boolean\":\"false\"," +
                "\"type_array\":[\"member 1\", \"member 2\"]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> assertRow(row, 12));
        assertEquals("invalid FLOAT8 input value '\"2 root beer floats\"'", e.getMessage());
    }

    @Test
    public void testGetFieldsShouldFailOnMismatchedBoolean() {

        String jsonStr = "{" +
                "\"type_int\":\"1234\"," +
                "\"type_bigint\":\"20202020202\"," +
                "\"type_smallint\":\"26\"," +
                "\"type_float\":\"-2.2\"," +
                "\"type_double\":\"-2.2\"," +
                "\"type_string1\":\"testing all supported types in JsonResolver A\"," +
                "\"type_string2\":\"testing all supported types in JsonResolver B\"," +
                "\"type_string3\":\"testing all supported types in JsonResolver C\"," +
                "\"type_char\":\"1\"," +
                "\"type_boolean\":\"true lies\"," +
                "\"type_array\":[\"member 1\", \"member 2\"]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> assertRow(row, 12));
        assertEquals("invalid BOOLEAN input value '\"true lies\"'", e.getMessage());
    }

    @Test
    public void testGetFieldsShouldFailOnMalformedJson() {

        String jsonStr = "{" +
                "\"type_int\":\"1234\"," +
                "\"type_bigint\":\"20202020202\"," +
                "\"type_smallint\":\"26\"," +
                "\"type_float\":\"-2.2\"," +
                "\"type_double\":\"-2.2\"," +
                "\"type_string1\":\"testing all supported types in JsonResolver A\"," +
                "\"type_string2\":\"testing all supported types in JsonResolver B\"," +
                "\"type_string3\":\"testing all supported types in JsonResolver C\"," +
                "\"type_char\":\"1\"," +
                "\"type_boolean\":\"true lies\"," +
                "\"type_array\":[\"member 1\", \"member 2\"]," + // <- this comma is bad
                "}";

        OneRow row = new OneRow(123, jsonStr);

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> assertRow(row, 12));
        assertTrue(e.getMessage().contains("error while parsing json record 'Unexpected character ('}' (code 125))"));
    }

    @Test
    public void testGetFieldsShouldFailOnEmptyRow() {
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        assertThrows(BadRecordException.class, () -> resolver.getFields(new OneRow()));
    }

    @Test
    public void testGetFieldsShouldFailForNonArrays() {
        //language=JSON
        String jsonStr = "{" +
                "\"type_array_as_textarray\":{\"id\": 1}" +
                "}";
        OneRow row = new OneRow(123, jsonStr);

        BadRecordException badRecordException = assertThrows(BadRecordException.class, () -> assertRow(row, 12));
        assertEquals("error while reading column 'type_array_as_textarray': invalid array value '{\"id\":1}'", badRecordException.getMessage());
    }

    @Test
    public void testSetFieldsShouldFail() throws UnsupportedOperationException {

        context.setMetadata(null);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        Exception e = assertThrows(UnsupportedOperationException.class, () -> resolver.setFields(null));
        assertEquals("JSON resolver does not support write operation.", e.getMessage());
    }

    // helper functions for testing
    private List<OneField> assertRow(OneRow row, int numFields) throws Exception {
        List<OneField> fields = resolver.getFields(row);
        assertEquals(numFields, fields.size());
        return fields;
    }

    private void assertField(List<OneField> fields, int index, Object value, DataType type) {
        assertEquals(type.getOID(), fields.get(index).type);
        Object fieldValue = fields.get(index).val;
        if (value == null) {
            assertNull(fieldValue);
        } else if (type == DataType.BYTEA) {
            assertArrayEquals((byte[]) value, (byte[]) fieldValue);
        } else {
            assertEquals(value, fieldValue);
        }
    }

    private ArrayList<ColumnDescriptor> generateJsonSchema() {
        ArrayList<ColumnDescriptor> cd = new ArrayList<>();
        cd.add(new ColumnDescriptor("type_int", DataType.INTEGER.getOID(), 0, "int4", null, true));
        cd.add(new ColumnDescriptor("type_bigint", DataType.BIGINT.getOID(), 1, "int8", null, true));
        cd.add(new ColumnDescriptor("type_smallint", DataType.SMALLINT.getOID(), 2, "int2", null, true));
        cd.add(new ColumnDescriptor("type_float", DataType.REAL.getOID(), 3, "float4", null, true));
        cd.add(new ColumnDescriptor("type_double", DataType.FLOAT8.getOID(), 4, "float8", null, true));
        cd.add(new ColumnDescriptor("type_string1", DataType.TEXT.getOID(), 5, "text", null, true));
        cd.add(new ColumnDescriptor("type_string2", DataType.VARCHAR.getOID(), 6, "varchar", null, true));
        cd.add(new ColumnDescriptor("type_string3", DataType.BPCHAR.getOID(), 7, "bpchar", null, true));
        cd.add(new ColumnDescriptor("type_char", DataType.BPCHAR.getOID(), 8, "bpchar", null, true));
        cd.add(new ColumnDescriptor("type_boolean", DataType.BOOLEAN.getOID(), 9, "bool", null, true));
        cd.add(new ColumnDescriptor("type_array[0]", DataType.TEXT.getOID(), 10, "text", null, true));
        cd.add(new ColumnDescriptor("type_array[1]", DataType.TEXT.getOID(), 11, "text", null, true));
        // complex types serialized as strings for Greenplum
        cd.add(new ColumnDescriptor("type_array", DataType.TEXT.getOID(), 12, "text", null, true));
        cd.add(new ColumnDescriptor("type_int_arr_as_text", DataType.TEXT.getOID(), 13, "text", null, true));
        cd.add(new ColumnDescriptor("type_bigint_arr_as_text", DataType.TEXT.getOID(), 14, "text", null, true));
        cd.add(new ColumnDescriptor("type_smallint_arr_as_text", DataType.TEXT.getOID(), 15, "text", null, true));
        cd.add(new ColumnDescriptor("type_float_arr_as_text", DataType.TEXT.getOID(), 16, "text", null, true));
        cd.add(new ColumnDescriptor("type_double_arr_as_text", DataType.TEXT.getOID(), 17, "text", null, true));
        cd.add(new ColumnDescriptor("type_string_arr_as_text", DataType.TEXT.getOID(), 18, "text", null, true));
        cd.add(new ColumnDescriptor("type_boolean_arr_as_text", DataType.TEXT.getOID(), 19, "text", null, true));
        cd.add(new ColumnDescriptor("type_string_arr_arr_as_text", DataType.TEXT.getOID(), 20, "text", null, true));
        cd.add(new ColumnDescriptor("type_object_arr_as_text", DataType.TEXT.getOID(), 21, "text", null, true));
        cd.add(new ColumnDescriptor("type_object_as_text", DataType.TEXT.getOID(), 22, "text", null, true));
        cd.add(new ColumnDescriptor("type_null_as_text", DataType.TEXT.getOID(), 23, "text", null, true));
        cd.add(new ColumnDescriptor("type_empty_arr_as_text", DataType.TEXT.getOID(), 24, "text", null, true));
        cd.add(new ColumnDescriptor("type_empty_object_as_text", DataType.TEXT.getOID(), 25, "text", null, true));
        // complex types serialized as text array for Greenplum
        cd.add(new ColumnDescriptor("type_array_as_textarray", DataType.TEXTARRAY.getOID(), 26, "text", null, true));
        cd.add(new ColumnDescriptor("type_int_arr_as_textarray", DataType.TEXTARRAY.getOID(), 27, "text", null, true));
        cd.add(new ColumnDescriptor("type_bigint_arr_as_textarray", DataType.TEXTARRAY.getOID(), 28, "text", null, true));
        cd.add(new ColumnDescriptor("type_smallint_arr_as_textarray", DataType.TEXTARRAY.getOID(), 29, "text", null, true));
        cd.add(new ColumnDescriptor("type_float_arr_as_textarray", DataType.TEXTARRAY.getOID(), 30, "text", null, true));
        cd.add(new ColumnDescriptor("type_double_arr_as_textarray", DataType.TEXTARRAY.getOID(), 31, "text", null, true));
        cd.add(new ColumnDescriptor("type_string_arr_as_textarray", DataType.TEXTARRAY.getOID(), 32, "text", null, true));
        cd.add(new ColumnDescriptor("type_boolean_arr_as_textarray", DataType.TEXTARRAY.getOID(), 33, "text", null, true));
        cd.add(new ColumnDescriptor("type_string_arr_arr_as_textarray", DataType.TEXTARRAY.getOID(), 34, "text", null, true));
        cd.add(new ColumnDescriptor("type_object_arr_as_textarray", DataType.TEXTARRAY.getOID(), 35, "text", null, true));
        cd.add(new ColumnDescriptor("type_null_as_textarray", DataType.TEXTARRAY.getOID(), 36, "text", null, true));
        cd.add(new ColumnDescriptor("type_empty_arr_as_textarray", DataType.TEXTARRAY.getOID(), 37, "text", null, true));

        return cd;
    }
}
