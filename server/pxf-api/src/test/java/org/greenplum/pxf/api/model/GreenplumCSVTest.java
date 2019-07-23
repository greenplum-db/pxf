package org.greenplum.pxf.api.model;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class GreenplumCSVTest {

    private GreenplumCSV gpCSV;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        gpCSV = new GreenplumCSV();
    }

    @Test
    public void testToCsvTextNull() {
        assertNull(gpCSV.toCsvText(null, false, false));
        assertNull(gpCSV.toCsvText(null, false, true));
        assertNull(gpCSV.toCsvText(null, true, false));
        assertNull(gpCSV.toCsvText(null, true, true));
    }

    @Test
    public void testToCsvTextEmptyString() {
        String input = "";
        assertSame(input, gpCSV.toCsvText(input, false, false));
        assertEquals("\"" + input, gpCSV.toCsvText(input, true, false));
        assertEquals(input + "\"", gpCSV.toCsvText(input, false, true));
        assertEquals("\"" + input + "\"", gpCSV.toCsvText(input, true, true));
    }

    @Test
    public void testToCsvHandlesNullInputs() {
        String input = "a";
        gpCSV.withNewline(null);
        assertSame(input, gpCSV.toCsvText(input, true, true, true));

        gpCSV.withQuoteChar("\"");
        gpCSV.withEscapeChar("\"");
        gpCSV.withNewline("\n");
        gpCSV.withDelimiter(null);
        assertSame(input, gpCSV.toCsvText(input, true, true, true));

        gpCSV.withQuoteChar("\"");
        gpCSV.withEscapeChar("\"");
        gpCSV.withNewline(null);
        gpCSV.withDelimiter(null);
        assertSame(input, gpCSV.toCsvText(input, true, true, true));
    }

    @Test
    public void testToCsvTestInputNewline() {
        String newline1 = "\n";
        String newline2 = "\r";
        String newline3 = "\r\n";

        // should not skip quoting
        assertEquals("\"\n\"", gpCSV.toCsvText(newline1, true, true, true)); // GreenplumCSV default newline character is \n
        gpCSV.withNewline("\r");
        assertEquals("\"\r\"", gpCSV.toCsvText(newline2, true, true, true));
        gpCSV.withNewline("\r\n");
        assertEquals("\"\r\n\"", gpCSV.toCsvText(newline3, true, true, true));
    }

    @Test
    public void testToCsvTextStringWithCharacterThatNeedToBeQuoted() {
        String input1 = "a";

        // make sure no new object is created
        assertSame(input1, gpCSV.toCsvText(input1, true, true, true));

        String input2 = "\""; // quote
        assertEquals("\"\"\"\"", gpCSV.toCsvText(input2, true, true, true));

        String input3 = ","; // delimiter
        assertEquals("\",\"", gpCSV.toCsvText(input3, true, true, true));

        String input4 = "\n\",sample"; // newline, quote and delimiter
        assertEquals("\"\n\"\",sample\"", gpCSV.toCsvText(input4, true, true, true));
    }

    @Test
    public void testToCsvTextStringWithoutQuoteCharacter() {
        String input = "aábcdefghijklmnñopqrstuvwxyz";

        assertSame(input, gpCSV.toCsvText(input, false, false));
        assertEquals("\"" + input, gpCSV.toCsvText(input, true, false));
        assertEquals(input + "\"", gpCSV.toCsvText(input, false, true));
        assertEquals("\"" + input + "\"",
                gpCSV.toCsvText(input, true, true));

        input = "aábcdefghijklm\"nñopqrstuvwxyz";

        gpCSV.withQuoteChar("|");
        gpCSV.withNewline("\r\n");
        assertSame(input, gpCSV.toCsvText(input, false, false, false));
        assertEquals("|" + input, gpCSV.toCsvText(input, true, false, false));
        assertEquals(input + "|", gpCSV.toCsvText(input, false, true, false));
        assertEquals("|" + input + "|", gpCSV.toCsvText(input, true, true, false));
    }

    @Test
    public void testToCsvDoesNotAddPrefixAndSuffixWhenSkipIfQuotingIsNotNeeded() {
        String input = "aábcdefghijklmnñopqrstuvwxyz";

        assertSame(input, gpCSV.toCsvText(input, false, false, true));
        assertSame(input, gpCSV.toCsvText(input, false, true, true));
        assertSame(input, gpCSV.toCsvText(input, true, false, true));
        assertSame(input, gpCSV.toCsvText(input, true, true, true));
    }

    @Test
    public void testToCsvTextEscapesQuotes() {
        String input = "{\"key\": \"value\", \"foo\": \"bar\"}";
        String expected = "{\"\"key\"\": \"\"value\"\", \"\"foo\"\": \"\"bar\"\"}";

        assertEquals(expected, gpCSV.toCsvText(input, false, false));
        assertEquals("\"" + expected, gpCSV.toCsvText(input, true, false));
        assertEquals(expected + "\"", gpCSV.toCsvText(input, false, true));
        assertEquals("\"" + expected + "\"",
                gpCSV.toCsvText(input, true, true));
    }

    @Test
    public void testToCsvTextEscapesQuoteChar() {
        char quoteChar = '|';
        String input = "a|b|c|d\ne|f|g|h";
        String expected = "a||b||c||d\ne||f||g||h";

        gpCSV.withQuoteChar("|");
        gpCSV.withEscapeChar("|");
        gpCSV.withNewline("\r\n");

        assertEquals(quoteChar + expected, gpCSV.toCsvText(input, true, false, false));
        assertEquals(expected, gpCSV.toCsvText(input, false, false, false));
        assertEquals(expected + quoteChar, gpCSV.toCsvText(input, false, true, false));
        assertEquals(quoteChar + expected + quoteChar,
                gpCSV.toCsvText(input, true, true, false));
    }

    @Test
    public void testWithValueOfNullValidDefaultInput() {
        assertEquals("", gpCSV.getValueOfNull());

        gpCSV.withValueOfNull("");
        assertEquals("", gpCSV.getValueOfNull());
    }

    @Test
    public void testWithValueOfNullCustomizedInput() {
        gpCSV.withValueOfNull("a");
        assertEquals("a", gpCSV.getValueOfNull());
    }

    @Test
    public void testWithQuoteCharValidInput() {
        gpCSV.withQuoteChar("\"");
        assertEquals('\"', gpCSV.getQuote());
    }

    @Test
    public void testWithQuoteCharEmptyInput() {
        gpCSV.withQuoteChar("");
        assertEquals('"', gpCSV.getQuote());
    }

    @Test
    public void testWithQuoteCharInvalidLength() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("invalid QUOTE character '\"\"'. Only single character is allowed for QUOTE.");
        gpCSV.withQuoteChar("\"\"");
    }

    @Test
    public void testWithEscapeCharValidInput() {
        gpCSV.withEscapeChar("\\");
        assertEquals('\\', gpCSV.getEscape());
    }

    @Test
    public void testWithEscapeCharEmptyInput() {
        gpCSV.withEscapeChar("");
        assertEquals('"', gpCSV.getEscape());
    }

    @Test
    public void testWithEscapeCharInvalidLength() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("invalid ESCAPE character '\\\\'. Only single character is allowed for ESCAPE.");
        gpCSV.withEscapeChar("\\\\");
    }

    @Test
    public void testWithNewlineValidInput() {
        gpCSV.withNewline("\n");
        assertEquals("\n", gpCSV.getNewline());
        gpCSV.withNewline("\r");
        assertEquals("\r", gpCSV.getNewline());
        gpCSV.withNewline("\r\n");
        assertEquals("\r\n", gpCSV.getNewline());
    }

    @Test
    public void testWithNewlineEmptyInput() {
        gpCSV.withNewline("");
        assertEquals("\n", gpCSV.getNewline());
    }

    @Test
    public void testWithNewlineInvalidInput() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("invalid newline character '\\\\'. Only LF, CR, or CRLF are supported for newline.");
        gpCSV.withNewline("\\\\");
    }

    @Test
    public void testWithDelimiterValidInput() {
        gpCSV.withDelimiter("|");
        assertEquals(new Character('|'), gpCSV.getDelimiter());
    }

    @Test
    public void testWithDelimiterEmptyInput() {
        gpCSV.withDelimiter("");
        assertEquals(new Character(','), gpCSV.getDelimiter());
    }

    @Test
    public void testWithDelimiterInvalidInput() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("invalid DELIMITER character '\\\\'. Only single character is allowed for DELIMITER.");
        gpCSV.withDelimiter("\\\\");
    }
}
