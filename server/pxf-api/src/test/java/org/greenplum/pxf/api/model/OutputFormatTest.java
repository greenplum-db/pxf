package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.UnsupportedTypeException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class OutputFormatTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testUnsupportedOutputFormat() {
        expectedException.expect(UnsupportedTypeException.class);
        expectedException.expectMessage("Unable to find output format by given class name: foo");

        OutputFormat.getOutputFormat("foo");
    }

    @Test
    public void testGetTextOutputFormat() {
        OutputFormat format = OutputFormat.getOutputFormat("org.greenplum.pxf.service.io.Text");
        assertEquals(OutputFormat.TEXT, format);
        assertEquals("TEXT", format.name());
        assertEquals("org.greenplum.pxf.service.io.Text", format.getClassName());
    }

    @Test
    public void testGetGPDBWritableOutputFormat() {
        OutputFormat format = OutputFormat.getOutputFormat("org.greenplum.pxf.service.io.GPDBWritable");
        assertEquals(OutputFormat.GPDBWritable, format);
        assertEquals("GPDBWritable", format.name());
        assertEquals("org.greenplum.pxf.service.io.GPDBWritable", format.getClassName());
    }

}
