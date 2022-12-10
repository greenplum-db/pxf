package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ParquetUtilitiesTest {
    private ParquetUtilities parquetUtilities;


    @BeforeEach
    public void setup() {
        PgUtilities pgUtilities = new PgUtilities();
        parquetUtilities = new ParquetUtilities(pgUtilities);
    }

    @Test
    public void testParsePostgresArrayIntegerArray() {
        // parquet SHORT is signed INT32 with INT annotation
        List<Object> result = parquetUtilities.parsePostgresArray("{1,2,-3}", PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.IntLogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(16,true));
        assertIterableEquals(Arrays.asList((short) 1, (short) 2, (short) -3), result);

        // parquet INT is INT32 with no annotation
        result = parquetUtilities.parsePostgresArray("{1,2,3}", PrimitiveType.PrimitiveTypeName.INT32, null);
        assertIterableEquals(Arrays.asList(1, 2, 3), result);
        // parquet LONG is an INT64 with no annotation
        result = parquetUtilities.parsePostgresArray("{1,2,3}", PrimitiveType.PrimitiveTypeName.INT64, null);
        assertIterableEquals(Arrays.asList(1L, 2L, 3L), result);
    }

    @Test
    public void testParsePostgresArrayIntegerArrayWithNulls() {
        List<Object> result = parquetUtilities.parsePostgresArray("{1,NULL,3}", PrimitiveType.PrimitiveTypeName.INT32, null);
        assertIterableEquals(Arrays.asList(1, null, 3), result);

        result = parquetUtilities.parsePostgresArray("{1,NULL,3}", PrimitiveType.PrimitiveTypeName.INT64, null);
        assertIterableEquals(Arrays.asList(1L, null, 3L), result);
    }

    @Test
    public void testParsePostgresArrayDoubleArray() {
        // parquet Double is a Double with no annotation
        String value = "{-1.79769E+308,-2.225E-307,0,2.225E-307,1.79769E+308}";

        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.DOUBLE, null);
        assertIterableEquals(Arrays.asList(-1.79769E308, -2.225E-307, 0.0, 2.225E-307, 1.79769E308), result);
    }

    @Test
    public void testParsePostgresArrayStringArray() {
        // parquet string is a BINARY primitive type with String annotation
        String value = "{fizz,buzz,fizzbuzz}";

        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType());
        assertIterableEquals(Arrays.asList("fizz", "buzz", "fizzbuzz"), result);
    }

    @Test
    public void testParsePostgresArrayDateArray() {
        // parquet date is an INT64 primitive type with String annotation
        String value = "{\"1985-01-01\",\"1990-04-30\"}";

        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType());
        assertIterableEquals(Arrays.asList("1985-01-01", "1990-04-30"), result);
    }

    @Test
    public void testParsePostgresArrayTimestampArray() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{\"2013-07-13 21:00:05-07\",\"2013-07-13 21:00:05-07\"}";

        // nothing is thrown here because we don't decode strings and check for multi-dimensional-ness. This check is done later
        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.INT96, null);
        assertEquals(2, result.size());
        assertEquals(Arrays.asList("2013-07-13 21:00:05-07","2013-07-13 21:00:05-07"), result);
    }
    @Test
    public void testParsePostgresArrayByteaArrayEscapeOutput() {
        // parquet bytea is a BINARY with no annotation
        String value = "{\"\\\\001\",\"\\\\001#\"}";

        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BINARY, null);
        assertEquals(2, result.size());

        ByteBuffer buffer1 = (ByteBuffer) result.get(0);
        assertEquals(ByteBuffer.wrap(new byte[]{0x01}), buffer1);
        ByteBuffer buffer2 = (ByteBuffer) result.get(1);
        assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x23}), buffer2);
    }

    @Test
    public void testParsePostgresArrayByteaArrayEscapeOutputContainsQuote() {
        String value = "{\"\\\"#$\"}";

        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BINARY, null);
        assertEquals(1, result.size());

        ByteBuffer buffer1 = (ByteBuffer) result.get(0);
        assertEquals(ByteBuffer.wrap(new byte[]{0x22, 0x23, 0x24}), buffer1);
    }

    @Test
    public void testParsePostgresArrayByteaArrayHexOutput() {
        String value = "{\"\\\\x01\",\"\\\\x0123\"}";

        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BINARY, null);
        assertEquals(2, result.size());

        ByteBuffer buffer1 = (ByteBuffer) result.get(0);
        assertArrayEquals(new byte[]{0x01}, buffer1.array());
        ByteBuffer buffer2 = (ByteBuffer) result.get(1);
        assertArrayEquals(new byte[]{0x01, 0x23}, buffer2.array());
    }

    @Test
    public void testParsePostgresArrayByteaInvalidHexFormat() {
        String value = "{\\\\xGG}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BINARY, null));
        assertEquals("Error parsing array element: \\xGG was not of expected type BINARY", exception.getMessage());
    }

    @Test
    public void testParsePostgresArrayValidBooleanArray() {
        String value = "{t,f,t}";

        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BOOLEAN, null);
        assertEquals(Arrays.asList(true, false, true), result);
    }

    @Test
    public void testParsePostgresArrayInValidBooleanArrayPXFGeneratedSchema() {
        // this situation should never happen as the toString method of booleans (boolout in GPDB) should not return a string in this format
        String value = "{true,false,true}";

        PxfRuntimeException exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BOOLEAN, null));
        assertEquals("Error parsing array element: true was not of expected type BOOLEAN", exception.getMessage());
        assertEquals("Unexpected state since PXF generated the Parquet schema.", exception.getHint());
    }

    @Test
    public void testParsePostgresArrayDimensionMismatch() {
        String value = "1";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BINARY, null));
        assertEquals("array dimension mismatch, rawData: 1", exception.getMessage());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayInt() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{1,2},{3,4}}";

        //Integer
        PxfRuntimeException exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.INT32, null));
        assertEquals("Error parsing array element: {1,2} was not of expected type INT32", exception.getMessage());
        assertEquals("Column value \"{{1,2},{3,4}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing Parquet files.", exception.getHint());
        // Short
        exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, true)));
        assertEquals("Error parsing array element: {1,2} was not of expected type INT32", exception.getMessage());
        assertEquals("Column value \"{{1,2},{3,4}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing Parquet files.", exception.getHint());
        // Long
        exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.INT64, null));
        assertEquals("Error parsing array element: {1,2} was not of expected type INT64", exception.getMessage());
        assertEquals("Column value \"{{1,2},{3,4}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing Parquet files.", exception.getHint());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayFloat() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{1.1,2.1},{3.1,4.1}}";

        PxfRuntimeException exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.FLOAT, null));
        assertEquals("Error parsing array element: {1.1,2.1} was not of expected type FLOAT", exception.getMessage());
        assertEquals("Column value \"{{1.1,2.1},{3.1,4.1}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing Parquet files.", exception.getHint());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayDouble() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{1.01,2.01},{3.01,4.01}}";

        PxfRuntimeException exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.DOUBLE, null));
        assertEquals("Error parsing array element: {1.01,2.01} was not of expected type DOUBLE", exception.getMessage());
        assertEquals("Column value \"{{1.01,2.01},{3.01,4.01}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing Parquet files.", exception.getHint());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayBool() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{t,f},{f,t}}";

        PxfRuntimeException exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BOOLEAN, null));
        assertEquals("Error parsing array element: {t,f} was not of expected type BOOLEAN", exception.getMessage());
        assertEquals("Column value \"{{t,f},{f,t}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing Parquet files.", exception.getHint());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayBytea() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{\\\\x0001, \\\\x0002},{\\\\x4041, \\\\x4142}}";

        PxfRuntimeException exception = assertThrows(PxfRuntimeException.class, () -> parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.BINARY, null));
        assertEquals("Error parsing array element: {\\x0001, \\x0002} was not of expected type BINARY", exception.getMessage());
        assertEquals("Column value \"{{\\\\x0001, \\\\x0002},{\\\\x4041, \\\\x4142}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing Parquet files.", exception.getHint());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayDate() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{\"1985-01-01\", \"1990-04-30\"},{\"1995-08-14\", \"2020-12-05\"}}";

        // nothing is thrown here because we don't decode strings and check for multi-dimensional-ness. This check is done later
        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType());
        assertEquals(2, result.size());
        assertEquals(Arrays.asList("{\"1985-01-01\", \"1990-04-30\"}", "{\"1995-08-14\", \"2020-12-05\"}"), result);
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayTimestamp() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{\"2013-07-13 21:00:05-07\"},{\"2013-07-13 21:00:05-07\"}}";

        // nothing is thrown here because we don't decode strings and check for multi-dimensional-ness. This check is done later
        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.INT96, null);
        assertEquals(2, result.size());
        assertEquals(Arrays.asList("{\"2013-07-13 21:00:05-07\"}","{\"2013-07-13 21:00:05-07\"}"), result);    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayNumeric() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{\"1.11\",\"2.22\"},{\"3.33\",\"4.44\"}}";

        // nothing is thrown here because we don't decode strings and check for multi-dimensional-ness. This check is done later
        List<Object> result = parquetUtilities.parsePostgresArray(value, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, null);
        assertEquals(2, result.size());
        assertEquals(Arrays.asList("{\"1.11\",\"2.22\"}, {\"3.33\",\"4.44\"}").toString(), result.toString());
    }
}
