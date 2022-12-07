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
    public void testAddElementIsFirstWithEscapingOption() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("test",false);
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
    public void testAddElementMultipleElementsNoEscapingWithEscapingOption() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("test",false);
        pgArrayBuilder.addElement("elem2",false);
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
    public void testAddMultipleElementNeedsEscapingWithEscapingOption() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("test element",true);
        pgArrayBuilder.addElement("\"escape me\" she said",true);
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
    public void testAddNullElementNeedsEscapingWithEscapingOption() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement((String) null,true);
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
    public void testAddEmptyStringNoEscapingWithEscapingOption() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("",false);
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
    public void testAddTwoEmptyStringNoEscapingWithEscapingOption() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("",false);
        pgArrayBuilder.addElement("",false);
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
    public void testAddTwoEmptyStringNeedsEscapingWithEscapingOption() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("",true);
        pgArrayBuilder.addElement("",true);
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
    public void testAddEmptyStringNeedsEscapingWithEscapingOption() {
        pgArrayBuilder.startArray();
        pgArrayBuilder.addElement("",true);
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
