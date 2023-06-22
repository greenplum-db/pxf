package org.greenplum.pxf.plugins.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.EnumSet;

import static org.greenplum.pxf.api.io.DataType.BOOLARRAY;
import static org.greenplum.pxf.api.io.DataType.BPCHARARRAY;
import static org.greenplum.pxf.api.io.DataType.BYTEAARRAY;
import static org.greenplum.pxf.api.io.DataType.DATEARRAY;
import static org.greenplum.pxf.api.io.DataType.FLOAT4ARRAY;
import static org.greenplum.pxf.api.io.DataType.FLOAT8ARRAY;
import static org.greenplum.pxf.api.io.DataType.INT2ARRAY;
import static org.greenplum.pxf.api.io.DataType.INT4ARRAY;
import static org.greenplum.pxf.api.io.DataType.INT8ARRAY;
import static org.greenplum.pxf.api.io.DataType.NUMERICARRAY;
import static org.greenplum.pxf.api.io.DataType.TEXTARRAY;
import static org.greenplum.pxf.api.io.DataType.TIMEARRAY;
import static org.greenplum.pxf.api.io.DataType.TIMESTAMPARRAY;
import static org.greenplum.pxf.api.io.DataType.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static org.greenplum.pxf.api.io.DataType.VARCHARARRAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonUtilitiesTest {

    private static final JsonFactory jsonFactory = new JsonFactory();
    private JsonGenerator generator;
    private StringWriter writer;
    private JsonUtilities jsonUtilities;

    @BeforeEach
    private void before() throws IOException {
        writer = new StringWriter();
        generator = jsonFactory.createGenerator(writer);
        jsonUtilities = new JsonUtilities();
        jsonUtilities.setPgUtilities(new PgUtilities());
    }

    // ================= PRIMITIVE TYPES =================

    @Test
    public void testWriteBytea() throws IOException {
        // ascii 1234, gets base64 encoded into the expected value MTIzNA==
        runScenarioString(DataType.BYTEA, DataType.BYTEA, new byte[]{0x31,0x32,0x33,0x34}, "MTIzNA==");
    }

    @Test
    public void testWriteBoolean() throws IOException {
        runScenario(DataType.BOOLEAN, DataType.BOOLEAN, true, "true");
    }

    @Test
    public void testWriteShort() throws IOException {
        runScenario(DataType.SMALLINT, DataType.SMALLINT, (short) 1, "1");
    }

    @Test
    public void testWriteInt() throws IOException {
        runScenario(DataType.INTEGER, DataType.INTEGER, 1000, "1000");
    }

    @Test
    public void testWriteLong() throws IOException {
        runScenario(DataType.BIGINT, DataType.BIGINT, (long) 1000000000, "1000000000");
    }

    @Test
    public void testWriteFloat() throws IOException {
        // the value will get rounded to the max precision supported by Java float
        runScenario(DataType.REAL, DataType.REAL, 1.23456789f, "1.2345679");
    }

    @Test
    public void testWriteDouble() throws IOException {
        // the value will get truncated to the max precision supported by Java double
        runScenario(DataType.FLOAT8, DataType.FLOAT8, 1.234567890123456789d, "1.2345678901234567");
    }

    @Test
    public void testWriteNumeric() throws IOException {
        runScenario(DataType.NUMERIC, DataType.TEXT, "123456789012345678901234567890.1234567890123456789012345", "123456789012345678901234567890.1234567890123456789012345");
    }

    @Test
    public void testWriteChar() throws IOException {
        runScenarioString(DataType.BPCHAR, DataType.TEXT, "abc", "abc");
    }

    @Test
    public void testWriteVarchar() throws IOException {
        runScenarioString(DataType.VARCHAR, DataType.TEXT, "def", "def");
    }

    @Test
    public void testWriteText() throws IOException {
        runScenarioString(DataType.TEXT, DataType.TEXT, "hij", "hij");
    }

    @Test
    public void testWriteDate() throws IOException {
        runScenarioString(DataType.DATE, DataType.TEXT, "2010-01-01", "2010-01-01");
    }

    @Test
    public void testWriteTime() throws IOException {
        runScenarioString(DataType.TIME, DataType.TEXT, "10:11:00", "10:11:00");
    }

    @Test
    public void testWriteTimestamp() throws IOException {
        runScenarioString(DataType.TIMESTAMP, DataType.TEXT, "21:00:05.000456", "21:00:05.000456");
    }

    @Test
    public void testWriteTimestampWithTimezone() throws IOException {
        runScenarioString(DataType.TIMESTAMP_WITH_TIME_ZONE, DataType.TEXT,
                "2013-07-13 21:00:05.000123-07", "2013-07-13 21:00:05.000123-07");
    }

    // ================= ARRAY TYPES =================

    @Test
    public void testWriteByteaArray() throws IOException {
        // ascii 1234, gets base64 encoded into the expected value MTIzNA==
        // ascii 5678, gets base64 encoded into the expected value NTY3OA==
        runScenario(BYTEAARRAY, DataType.TEXT,
                "{\"\\\\x31323334\",NULL,\"\\\\x35363738\"}","[\"MTIzNA==\",null,\"NTY3OA==\"]");
    }

    @Test
    public void testWriteBooleanArray() throws IOException {
        runScenario(BOOLARRAY, DataType.TEXT, "{t,NULL,f}", "[true,null,false]");
    }

    @Test
    public void testWriteShortArray() throws IOException {
        runScenario(INT2ARRAY, DataType.TEXT, "{1,NULL,-1}", "[1,null,-1]");
    }

    @Test
    public void testWriteIntArray() throws IOException {
        runScenario(INT4ARRAY, DataType.TEXT, "{1000,NULL,-1000}", "[1000,null,-1000]");
    }

    @Test
    public void testWriteLongArray() throws IOException {
        runScenario(INT8ARRAY, DataType.TEXT, "{1000000000,NULL,-1000000000}", "[1000000000,null,-1000000000]");
    }

    @Test
    public void testWriteFloatArray() throws IOException {
        // the values will NOT get rounded to the max precision supported by Java float as no conversion takes place
        runScenario(FLOAT4ARRAY, DataType.TEXT, "{1.23456789,NULL,-1.23456789}", "[1.23456789,null,-1.23456789]");
    }

    @Test
    public void testWriteDoubleArray() throws IOException {
        // the value will NOT get truncated to the max precision supported by Java double as no conversion takes place
        runScenario(FLOAT8ARRAY, DataType.TEXT,
                "{1.234567890123456789,NULL,-1.234567890123456789}",
                "[1.234567890123456789,null,-1.234567890123456789]");
    }

    @Test
    public void testWriteNumericArray() throws IOException {
        runScenario(NUMERICARRAY, DataType.TEXT,
                "{123456789012345678901234567890.1234567890123456789012345,NULL,-123456789012345678901234567890.1234567890123456789012345}",
                "[123456789012345678901234567890.1234567890123456789012345,null,-123456789012345678901234567890.1234567890123456789012345]");
    }

    @Test
    public void testWriteCharArray() throws IOException {
        runScenario(DataType.BPCHARARRAY, DataType.TEXT, "{abc,NULL,\"d e\"}", "[\"abc\",null,\"d e\"]");
    }

    @Test
    public void testWriteVarcharArray() throws IOException {
        runScenario(DataType.VARCHARARRAY, DataType.TEXT, "{abc,NULL,\"d e\"}", "[\"abc\",null,\"d e\"]");
    }

    @Test
    public void testWriteTextArray() throws IOException {
        runScenario(DataType.TEXTARRAY, DataType.TEXT, "{abc,NULL,\"d e\"}", "[\"abc\",null,\"d e\"]");
    }

    @Test
    public void testWriteDateArray() throws IOException {
        runScenario(DATEARRAY, DataType.TEXT, "{2010-01-01,NULL,2010-01-02}", "[\"2010-01-01\",null,\"2010-01-02\"]");
    }

    @Test
    public void testWriteTimeArray() throws IOException {
        runScenario(TIMEARRAY, DataType.TEXT, "{10:11:00,NULL,10:11:01}", "[\"10:11:00\",null,\"10:11:01\"]");
    }

    @Test
    public void testWriteTimestampArray() throws IOException {
        runScenario(TIMESTAMPARRAY, DataType.TEXT,
                "{21:00:05.000456,NULL,21:00:05.000789}", "[\"21:00:05.000456\",null,\"21:00:05.000789\"]");
    }

    @Test
    public void testWriteTimestampWithTimezoneArray() throws IOException {
        runScenario(TIMESTAMP_WITH_TIMEZONE_ARRAY, DataType.TEXT,
                "{2013-07-13 21:00:05.000123-07,NULL,2013-07-13 21:00:05.000456-07}",
                "[\"2013-07-13 21:00:05.000123-07\",null,\"2013-07-13 21:00:05.000456-07\"]");
    }

    // ---------- ARRAY SPECIAL CASES ----------
    @Test
    public void testWriteEmptyArray() throws IOException {
        // handling of empty arrays does not depend on datatypes, so a single test case is sufficient
        runScenario(INT4ARRAY, DataType.TEXT, "{}", "[]");
    }

    @Test
    public void testMultiDimensionalArrayError() {
        // we do not really support multidimensional arrays as Greenplum does not have metadata about array dimensionality,
        // but we can error out only on non-string types

        // test all types in the same test case not to blow out the number of individual test cases, the actual value
        // should not matter as the parser will fail on the second { character
        EnumSet<DataType> dataTypes = EnumSet.of(INT2ARRAY,INT4ARRAY,INT8ARRAY,BOOLARRAY,FLOAT4ARRAY,
                FLOAT8ARRAY,BYTEAARRAY,DATEARRAY,NUMERICARRAY,TIMEARRAY,TIMESTAMPARRAY, TIMESTAMP_WITH_TIMEZONE_ARRAY);
        for (DataType dataType : dataTypes) {
            PxfRuntimeException exception = assertThrows(PxfRuntimeException.class, () ->
                    runScenario(dataType, DataType.TEXT, "{{1,2},{3,4}}", "does not matter")
            );
            assertEquals("Error parsing array element: {1,2} was not of expected type " + dataType.getTypeElem(), exception.getMessage());
            assertEquals("Column value \"{{1,2},{3,4}}\" is a multi-dimensional array; PXF does not support writing multi-dimensional arrays to Json files.", exception.getHint());
        }
    }

    @Test
    public void testMultiDimensionalArrayNoErrorCharArray() throws IOException {
        // no error will be reported for textual types, but the multidimensional structure will not be properly preserved
        runScenario(BPCHARARRAY, DataType.TEXT, "{{abc,def},{hij,klm}}", "[\"{abc,def}\",\"{hij,klm}\"]");
    }

    @Test
    public void testMultiDimensionalArrayNoErrorVarcharArray() throws IOException {
        // no error will be reported for textual types, but the multidimensional structure will not be properly preserved
        runScenario(VARCHARARRAY, DataType.TEXT, "{{abc,def},{hij,klm}}", "[\"{abc,def}\",\"{hij,klm}\"]");
    }

    @Test
    public void testMultiDimensionalArrayNoErrorTextArray() throws IOException {
        // no error will be reported for textual types, but the multidimensional structure will not be properly preserved
        runScenario(TEXTARRAY, DataType.TEXT, "{{abc,def},{hij,klm}}", "[\"{abc,def}\",\"{hij,klm}\"]");
    }

    /**
     * Runs a scenario of writing a field of a given Greenplum type with a given serde type with a given value
     * into Json format with a given expected value being a string.
     *
     * @param columnType    Greenplum column type
     * @param serdeType     Greenplum type reported in OneField object, basically a serde type for a given Greenplum type
     * @param value         value to write
     * @param expectedValue expected value that should be written to Json
     * @throws IOException
     */
    private void runScenarioString(DataType columnType, DataType serdeType, Object value, String expectedValue) throws IOException {
        runScenario(columnType, serdeType, value, "\"" + expectedValue + "\"");
    }

    /**
     * Runs a scenario of writing a field of a given Greenplum type with a given serde type with a given value
     * into Json format with a given expected value.
     *
     * @param columnType    Greenplum column type
     * @param serdeType     Greenplum type reported in OneField object, basically a serde type for a given Greenplum type
     * @param value         value to write
     * @param expectedValue expected value that should be written to Json
     * @throws IOException
     */
    private void runScenario(DataType columnType, DataType serdeType, Object value, String expectedValue) throws IOException {
        ColumnDescriptor nullColumn = new ColumnDescriptor("nada", columnType.getOID(), 0, null, null);
        ColumnDescriptor column = new ColumnDescriptor("foo", columnType.getOID(), 0, null, null);
        generator.writeStartObject();
        jsonUtilities.writeField(generator, nullColumn, new OneField(serdeType.getOID(), null));
        jsonUtilities.writeField(generator, column, new OneField(serdeType.getOID(), value));
        generator.writeEndObject();
        generator.close();
        String expected = String.format("{\"nada\":null,\"foo\":%s}", expectedValue);
        assertEquals(expected, writer.toString());
    }

}
