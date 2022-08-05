package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.orc.TypeDescription;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OrcUtilitiesTest {
    private OrcUtilities orcUtilities;
    private PgUtilities pgUtilities;


    @BeforeEach
    public void setup() {
        pgUtilities = new PgUtilities();
        orcUtilities = new OrcUtilities(pgUtilities);
    }

    @Test
    public void testParsePostgresArrayIntegerArray() {
        // for ORC, SHORT, INT and LONG types all return long values
        List<Object> result = orcUtilities.parsePostgresArray( "{1,2,3}", TypeDescription.Category.INT);
        assertIterableEquals(Arrays.asList(1L, 2L, 3L), result);
    }

    @Test
    public void testParsePostgresArrayIntegerArrayWithNulls() {
        List<Object> result = orcUtilities.parsePostgresArray( "{1,NULL,3}", TypeDescription.Category.INT);
        assertIterableEquals(Arrays.asList(1L, null, 3L), result);
    }

    @Test
    public void testParsePostgresArrayDoubleArray() {
        String value = "{-1.79769E+308,-2.225E-307,0,2.225E-307,1.79769E+308}";

        List<Object> result = orcUtilities.parsePostgresArray(value, TypeDescription.Category.DOUBLE);
        assertIterableEquals(Arrays.asList(-1.79769E308, -2.225E-307, 0.0, 2.225E-307, 1.79769E308), result);
    }

    @Test
    public void testParsePostgresArrayStringArray() {
        String value = "{fizz,buzz,fizzbuzz}";

        List<Object> result = orcUtilities.parsePostgresArray(value, TypeDescription.Category.STRING);
        assertIterableEquals(Arrays.asList("fizz", "buzz", "fizzbuzz"), result);
    }

    @Test
    public void testParsePostgresArrayByteaArrayEscapeOutput() {
        String value = "{\"\\\\001\",\"\\\\001#\"}";

        List<Object> result = orcUtilities.parsePostgresArray(value, TypeDescription.Category.BINARY);
        assertEquals(2, result.size());

        ByteBuffer buffer1 = (ByteBuffer) result.get(0);
        assertEquals(ByteBuffer.wrap(new byte[] {0x01}), buffer1);
        ByteBuffer buffer2 = (ByteBuffer) result.get(1);
        assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x23}), buffer2);
    }

    @Test
    public void testParsePostgresArrayByteaArrayEscapeOutputContainsQuote() {
        String value = "{\"\\\"#$\"}";

        List<Object> result = orcUtilities.parsePostgresArray(value, TypeDescription.Category.BINARY);
        assertEquals(1, result.size());

        ByteBuffer buffer1 = (ByteBuffer) result.get(0);
        assertEquals(ByteBuffer.wrap(new byte[]{0x22, 0x23, 0x24}), buffer1);
    }

    @Test
    public void testParsePostgresArrayByteaArrayHexOutput() {
        String value = "{\"\\\\x01\",\"\\\\x0123\"}";

        List<Object> result = orcUtilities.parsePostgresArray(value, TypeDescription.Category.BINARY);
        assertEquals(2, result.size());

        ByteBuffer buffer1 = (ByteBuffer) result.get(0);
        assertArrayEquals(new byte[]{0x01}, buffer1.array());
        ByteBuffer buffer2 = (ByteBuffer) result.get(1);
        assertArrayEquals(new byte[]{0x01, 0x23}, buffer2.array());
    }

    @Test
    public void testParsePostgresArrayByteaInvalidHexFormat() {
        String value = "{\\\\xGG}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> orcUtilities.parsePostgresArray(value, TypeDescription.Category.BINARY));
        assertEquals("Error parsing array element: \\xGG was not of expected type BINARY", exception.getMessage());
    }

    @Test
    public void testParsePostgresArrayValidBooleanArray() {
        String value = "{t,f,t}";

        List<Object> result = orcUtilities.parsePostgresArray(value, TypeDescription.Category.BOOLEAN);
        assertEquals(Arrays.asList(true, false, true), result);
    }

    @Test
    public void testParsePostgresArrayInValidBooleanArrayPXFGeneratedSchema() {
        // this situation should never happen as the toString method of booleans (boolout in GPDB) should not return a string in this format
        String value = "{true,false,true}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> orcUtilities.parsePostgresArray(value, TypeDescription.Category.BOOLEAN));
        assertEquals("Error parsing array element: true was not of expected type BOOLEAN", exception.getMessage());
        assertEquals("Unexpected state since PXF generated the ORC schema.", ((PxfRuntimeException) exception).getHint());
    }

    @Test
    public void testParsePostgresArrayDimensionMismatch() {
        String value = "1";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> orcUtilities.parsePostgresArray(value, TypeDescription.Category.BINARY));
        assertEquals("array dimension mismatch, rawData: 1", exception.getMessage());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayInt() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{1,2},{3,4}}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> orcUtilities.parsePostgresArray(value, TypeDescription.Category.INT));
        assertEquals("Error parsing array element: {1,2} was not of expected type INT", exception.getMessage());
        assertEquals("Column value \"{{1,2},{3,4}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing ORC files.", ((PxfRuntimeException) exception).getHint());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayBool() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{t,f},{f,t}}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> orcUtilities.parsePostgresArray(value, TypeDescription.Category.BOOLEAN));
        assertEquals("Error parsing array element: {t,f} was not of expected type BOOLEAN", exception.getMessage());
        assertEquals("Column value \"{{t,f},{f,t}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing ORC files.", ((PxfRuntimeException) exception).getHint());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayBytea() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{\\\\x0001, \\\\x0002},{\\\\x4041, \\\\x4142}}";

        Exception exception = assertThrows(PxfRuntimeException.class, () -> orcUtilities.parsePostgresArray(value, TypeDescription.Category.BINARY));
        assertEquals("Error parsing array element: {\\x0001, \\x0002} was not of expected type BINARY", exception.getMessage());
        assertEquals("Column value \"{{\\\\x0001, \\\\x0002},{\\\\x4041, \\\\x4142}}\" is a multi-dimensional array, PXF does not support multi-dimensional arrays for writing ORC files.", ((PxfRuntimeException) exception).getHint());
    }

    @Test
    public void testParsePostgresArrayMultiDimensionalArrayDate() {
        // test the underlying decode string: we expect it to fail and the failure to be caught by parsePostgresArray
        // as we currently do not support writing multi-dimensional arrays
        String value = "{{\"1985-01-01\", \"1990-04-30\"},{\"1995-08-14\", \"2020-12-05\"}}";

        // nothing is thrown here because we don't decode strings and check for multi-dimensional-ness. This check is done later
        List<Object> result = orcUtilities.parsePostgresArray(value, TypeDescription.Category.DATE);
        assertEquals(2, result.size());
        assertEquals(Arrays.asList("{\"1985-01-01\", \"1990-04-30\"}", "{\"1995-08-14\", \"2020-12-05\"}"), result);
    }
}
