package org.greenplum.pxf.plugins.hdfs.utilities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DecimalOverflowOptionTest {
    @Test
    public void testDecimalOverflowOptionValidity() {
        assertEquals(DecimalOverflowOption.ERROR, DecimalOverflowOption.valueOf("ERROR"));
        assertEquals(DecimalOverflowOption.ROUND, DecimalOverflowOption.valueOf("ROUND"));
        assertEquals(DecimalOverflowOption.IGNORE, DecimalOverflowOption.valueOf("IGNORE"));

        Exception e = assertThrows(IllegalArgumentException.class, () -> DecimalOverflowOption.valueOf("error"));
        assertEquals("No enum constant org.greenplum.pxf.plugins.hdfs.utilities.DecimalOverflowOption.error", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> DecimalOverflowOption.valueOf("round"));
        assertEquals("No enum constant org.greenplum.pxf.plugins.hdfs.utilities.DecimalOverflowOption.round", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> DecimalOverflowOption.valueOf("ignore"));
        assertEquals("No enum constant org.greenplum.pxf.plugins.hdfs.utilities.DecimalOverflowOption.ignore", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> DecimalOverflowOption.valueOf("foo"));
        assertEquals("No enum constant org.greenplum.pxf.plugins.hdfs.utilities.DecimalOverflowOption.foo", e.getMessage());
    }
}
