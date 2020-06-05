package org.greenplum.pxf.plugins.json;

import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonResolverTest {

    private JsonResolver resolver;
    private RequestContext context;
    private final ArrayList<ColumnDescriptor> schema = generateJsonSchema();

    @BeforeEach
    public void setUp() {
        resolver = new JsonResolver();
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
                "\"type_array\":[\"member 1\", \"member 2\"]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        List<OneField> fields = assertRow(row, 12);
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
    }

    @Test
    public void testGetFieldsWithQuotedValues() throws Exception {

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
                "\"type_array\":[\"member 1\", \"member 2\"]" +
                "}";

        OneRow row = new OneRow(123, jsonStr);

        List<OneField> fields = assertRow(row, 12);
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
    public void testSetFieldsShouldFail() throws UnsupportedOperationException {

        context.setMetadata(null);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        assertThrows(UnsupportedOperationException.class, () -> resolver.setFields(null));
    }

    // helper functions for testing
    private List<OneField> assertRow(OneRow row, int numFields) throws Exception {
        List<OneField> fields = resolver.getFields(row);
        assertEquals(numFields, fields.size());
        return fields;
    }

    private void assertField(List<OneField> fields, int index, Object value, DataType type) {
        assertEquals(type.getOID(), fields.get(index).type);
        if (type == DataType.BYTEA) {
            assertArrayEquals((byte[]) value, (byte[]) fields.get(index).val);
        } else {
            assertEquals(value, fields.get(index).val);
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
        return cd;
    }
}
