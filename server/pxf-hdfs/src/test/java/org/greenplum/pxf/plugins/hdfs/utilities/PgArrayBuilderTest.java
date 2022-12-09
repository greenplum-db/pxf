package org.greenplum.pxf.plugins.hdfs.utilities;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PgArrayBuilderTest {
    private PgArrayBuilder pgArrayBuilder;

    @BeforeEach
    public void setup() {
        pgArrayBuilder = new PgArrayBuilder(new PgUtilities());
    }

    @Test
    public void testEmptyArray() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.endArray();
        assertEquals("{}", pgArrayBuilder.toString());
    }

    @Test
    public void testAddElementIsFirst() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElementNoEscaping("test");
        pgArrayBuilder.endArray();
        assertEquals("{test}", pgArrayBuilder.toString());
    }

    @Test
    public void testAddElementMultipleElementsNoEscaping() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElementNoEscaping("test");
        pgArrayBuilder.addElementNoEscaping("elem2");
        pgArrayBuilder.endArray();
        assertEquals("{test,elem2}", pgArrayBuilder.toString());
    }

    @Test
    public void testAddMultipleElementNeedsEscaping() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("test element");
        pgArrayBuilder.addElement("\"escape me\" she said");
        pgArrayBuilder.endArray();
        assertEquals("{\"test element\",\"\\\"escape me\\\" she said\"}", pgArrayBuilder.toString());
    }

    @Test
    public void testAddNullElementNeedsEscaping() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement((String) null);
        pgArrayBuilder.endArray();
        assertEquals("{NULL}", pgArrayBuilder.toString());
    }

    @Test
    public void testAddEmptyStringNoEscaping() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElementNoEscaping("");
        pgArrayBuilder.endArray();
        assertEquals("{}", pgArrayBuilder.toString());
    }

    @Test
    public void testAddTwoEmptyStringNoEscaping() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElementNoEscaping("");
        pgArrayBuilder.addElementNoEscaping("");
        pgArrayBuilder.endArray();
        assertEquals("{,}", pgArrayBuilder.toString());
    }

    @Test
    public void testAddTwoEmptyStringNeedsEscaping() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("");
        pgArrayBuilder.addElement("");
        pgArrayBuilder.endArray();
        assertEquals("{\"\",\"\"}", pgArrayBuilder.toString());
    }

    @Test
    public void testAddEmptyStringNeedsEscaping() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("");
        pgArrayBuilder.endArray();
        assertEquals("{\"\"}", pgArrayBuilder.toString());
    }

    @Test
    public void testAddElementUsingLambda() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement(buf -> buf.append("1"));
        pgArrayBuilder.addElement(buf -> buf.append("2"));
        pgArrayBuilder.endArray();
        assertEquals("{1,2}", pgArrayBuilder.toString());
    }
}
