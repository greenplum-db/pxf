package org.greenplum.pxf.plugins.hdfs.parquet;

import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParquetRecordFilterBuilderTest extends ParquetBaseTest {

    @Test
    public void testUnsupportedOperationError() {
        // a16 in (11, 12)
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a16m1007s2d11s2d12o10"));
        assertEquals("not supported IN", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96EqualsFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o5"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96LessThanFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o1"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96GreaterThanFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o2"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96LessThanOrEqualsFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o3"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96GreaterThanOrEqualsFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o4"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96NotEqualsFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o6"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96IsNullFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6o8"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96IsNotNullFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6o9"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedFixedLenByteArrayEqualsFilter() {
        // dec2 = 0
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a14c23s1d0o5"));
        assertEquals("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedFixedLenByteArrayLessThanFilter() {
        // dec2 = 0
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a14c23s1d0o1"));
        assertEquals("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedFixedLenByteArrayGreaterThanFilter() {
        // dec2 = 0
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a14c23s1d0o2"));
        assertEquals("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedFixedLenByteArrayLessThanOrEqualsFilter() {
        // dec2 = 0
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a14c23s1d0o3"));
        assertEquals("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedFixedLenByteArrayGreaterThanOrEqualsFilter() {
        // dec2 = 0
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a14c23s1d0o4"));
        assertEquals("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedFixedLenByteArrayNotEqualsFilter() {
        // dec2 = 0
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a14c23s1d0o6"));
        assertEquals("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedFixedLenByteArrayIsNullFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a14o8"));
        assertEquals("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedFixedLenByteArrayIsNotNullFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a14o9"));
        assertEquals("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported", e.getMessage());
    }

    private ParquetRecordFilterBuilder filterBuilderFromFilterString(String filterString) throws Exception {

        ParquetRecordFilterBuilder filterBuilder = new ParquetRecordFilterBuilder(columnDescriptors, originalFieldsMap);

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        // traverse the tree with the ParquetRecordFilterBuilder to
        // produce a record filter for parquet
        TRAVERSER.traverse(root, filterBuilder);

        return filterBuilder;
    }
}
