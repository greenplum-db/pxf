package org.greenplum.pxf.plugins.jdbc.partitioning;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionTypeTest {

    @Test
    public void testErrorIfColumnNameIsNotProvided() {
        Exception ex = assertThrows(RuntimeException.class,
            () -> PartitionType.INT.getFragmentsMetadata(null, "range", "interval"));
        assertEquals("The column name must be provided", ex.getMessage());
    }

    @Test
    public void testErrorIfRangeIsNotProvidedForINT() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> PartitionType.INT.getFragmentsMetadata("foo", null, "interval"));
        assertEquals("The parameter 'RANGE' must be specified for partition of type 'INT'", ex.getMessage());
    }

    @Test
    public void testErrorIfRangeIsNotProvidedForENUM() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> PartitionType.ENUM.getFragmentsMetadata("foo", null, "interval"));
        assertEquals("The parameter 'RANGE' must be specified for partition of type 'ENUM'", ex.getMessage());
    }

    @Test
    public void testErrorIfRangeIsNotProvidedForDATE() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> PartitionType.DATE.getFragmentsMetadata("foo", null, "interval"));
        assertEquals("The parameter 'RANGE' must be specified for partition of type 'DATE'", ex.getMessage());
    }

    @Test
    public void testErrorIfIntervalIsNotProvidedForINT() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> PartitionType.INT.getFragmentsMetadata("foo", "bar", null));
        assertEquals("The parameter 'INTERVAL' must be specified for partition of type 'INT'", ex.getMessage());
    }

    @Test
    public void testErrorIfIntervalIsNotProvidedForDATE() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> PartitionType.DATE.getFragmentsMetadata("foo", "bar", null));
        assertEquals("The parameter 'INTERVAL' must be specified for partition of type 'DATE'", ex.getMessage());
    }

    @Test
    public void testSucceedsIfIntervalIsNotProvidedForENUM() {
        PartitionType.ENUM.getFragmentsMetadata("foo", "bar", null);
    }

    @Test
    public void unsupportedCreatePartitionForEnum() {
        assertThrows(UnsupportedOperationException.class,
            () -> PartitionType.ENUM.createPartition(null, null, null));
    }

    @Test
    public void unsupportedParseRangeForEnum() {
        assertThrows(UnsupportedOperationException.class,
            () -> PartitionType.ENUM.parseRange(null));
    }

    @Test
    public void unsupportedParseInterval() {
        assertThrows(UnsupportedOperationException.class,
            () -> PartitionType.ENUM.parseInterval(null));

    }

    @Test
    public void unsupportedIsLessThan() {
        assertThrows(UnsupportedOperationException.class,
            () -> PartitionType.ENUM.isLessThan(null, null));
    }

    @Test
    public void unsupportedNext() {
        assertThrows(UnsupportedOperationException.class,
            () -> PartitionType.ENUM.next(null, null, null));
    }

    @Test
    public void unsupportedGetValidIntervalFormat() {
        assertThrows(UnsupportedOperationException.class, PartitionType.ENUM::getValidIntervalFormat);
    }

    @Test
    public void testInvalidRangeForInt() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> PartitionType.INT.getFragmentsMetadata("foo", "1", "1"));
        assertEquals("The parameter 'RANGE' has incorrect format. The correct format for partition of type 'INT' is '<start_value>:<end_value>'", ex.getMessage());
    }

    @Test
    public void testInvalidRange2ForInt() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> PartitionType.INT.getFragmentsMetadata("foo", "1:2:3", "1"));
        assertEquals("The parameter 'RANGE' has incorrect format. The correct format for partition of type 'INT' is '<start_value>:<end_value>'", ex.getMessage());
    }

    @Test
    public void testInvalidRangeValuesForInt() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> PartitionType.INT.getFragmentsMetadata("foo", "a:b", "1"));
        assertEquals("The parameter 'RANGE' is invalid. The correct format for partition of type 'INT' is 'Integer'", ex.getMessage());
    }

    @Test
    public void testInvalidRangeValuesForDate() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> PartitionType.DATE.getFragmentsMetadata("foo", "a:b", "1"));
        assertEquals("The parameter 'RANGE' is invalid. The correct format for partition of type 'DATE' is 'yyyy-mm-dd'", ex.getMessage());
    }
}
