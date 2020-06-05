package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OutputFormatTest {

    @Test
    public void testUnsupportedOutputFormat() {
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> OutputFormat.getOutputFormat("foo"));
        assertEquals("Unable to find output format by given class name: foo", e.getMessage());
    }

    @Test
    public void testGetTextOutputFormat() {
        OutputFormat format = OutputFormat.getOutputFormat("org.greenplum.pxf.api.io.Text");
        assertEquals(OutputFormat.TEXT, format);
        assertEquals("TEXT", format.name());
        assertEquals("org.greenplum.pxf.api.io.Text", format.getClassName());
    }

    @Test
    public void testGetGPDBWritableOutputFormat() {
        OutputFormat format = OutputFormat.getOutputFormat("org.greenplum.pxf.api.io.GPDBWritable");
        assertEquals(OutputFormat.GPDBWritable, format);
        assertEquals("GPDBWritable", format.name());
        assertEquals("org.greenplum.pxf.api.io.GPDBWritable", format.getClassName());
    }

}
