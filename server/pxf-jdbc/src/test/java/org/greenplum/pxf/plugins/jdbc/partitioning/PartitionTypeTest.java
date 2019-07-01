package org.greenplum.pxf.plugins.jdbc.partitioning;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PartitionTypeTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testErrorIfColumnNameIsNotProvided() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("The column name must be provided");

        PartitionType.INT.getFragmentsMetadata(null, "range", "interval");
    }

    @Test
    public void testErrorIfRangeIsNotProvidedForINT() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The parameter 'RANGE' must be specified for partition of type 'INT'");

        PartitionType.INT.getFragmentsMetadata("foo", null, "interval");
    }

    @Test
    public void testErrorIfRangeIsNotProvidedForENUM() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The parameter 'RANGE' must be specified for partition of type 'ENUM'");

        PartitionType.ENUM.getFragmentsMetadata("foo", null, "interval");
    }

    @Test
    public void testErrorIfRangeIsNotProvidedForDATE() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The parameter 'RANGE' must be specified for partition of type 'DATE'");

        PartitionType.DATE.getFragmentsMetadata("foo", null, "interval");
    }

    @Test
    public void testErrorIfIntervalIsNotProvidedForINT() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The parameter 'INTERVAL' must be specified for partition of type 'INT'");

        PartitionType.INT.getFragmentsMetadata("foo", "bar", null);
    }

    @Test
    public void testErrorIfIntervalIsNotProvidedForDATE() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The parameter 'INTERVAL' must be specified for partition of type 'DATE'");

        PartitionType.DATE.getFragmentsMetadata("foo", "bar", null);
    }

    @Test
    public void testSucceedsIfIntervalIsNotProvidedForENUM() {
        PartitionType.ENUM.getFragmentsMetadata("foo", "bar", null);
    }

    @Test
    public void unsupportedCreatePartitionForEnum() {
        expectedException.expect(UnsupportedOperationException.class);
        PartitionType.ENUM.createPartition(null, null, null);
    }

    @Test
    public void unsupportedParseRangeForEnum() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        PartitionType.ENUM.parseRange(null);
    }

    @Test
    public void unsupportedParseInterval() {
        expectedException.expect(UnsupportedOperationException.class);
        PartitionType.ENUM.parseInterval(null);
    }

    @Test
    public void unsupportedIsLessThan() {
        expectedException.expect(UnsupportedOperationException.class);
        PartitionType.ENUM.isLessThan(null, null);
    }

    @Test
    public void unsupportedNext() {
        expectedException.expect(UnsupportedOperationException.class);
        PartitionType.ENUM.next(null, null, null);
    }

    @Test
    public void unsupportedGetValidIntervalFormat() {
        expectedException.expect(UnsupportedOperationException.class);
        PartitionType.ENUM.getValidIntervalFormat();
    }

    @Test
    public void testInvalidRangeForInt() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The parameter 'RANGE' has incorrect format. The correct format for partition of type 'INT' is '<start_value>:<end_value>'");

        PartitionType.INT.getFragmentsMetadata("foo", "1", "1");
    }

    @Test
    public void testInvalidRange2ForInt() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The parameter 'RANGE' has incorrect format. The correct format for partition of type 'INT' is '<start_value>:<end_value>'");

        PartitionType.INT.getFragmentsMetadata("foo", "1:2:3", "1");
    }

    @Test
    public void testInvalidRangeValuesForInt() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The parameter 'RANGE' is invalid. The correct format for partition of type 'INT' is 'Integer'");

        PartitionType.INT.getFragmentsMetadata("foo", "a:b", "1");
    }

    @Test
    public void testInvalidRangeValuesForDate() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The parameter 'RANGE' is invalid. The correct format for partition of type 'DATE' is 'yyyy-mm-dd'");

        PartitionType.DATE.getFragmentsMetadata("foo", "a:b", "1");
    }
}