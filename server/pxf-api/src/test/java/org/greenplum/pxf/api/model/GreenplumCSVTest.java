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
    public void testToCsvFieldNull() {
        assertNull(gpCSV.toCsvField(null, false, false));
        assertNull(gpCSV.toCsvField(null, false, true));
        assertNull(gpCSV.toCsvField(null, true, false));
        assertNull(gpCSV.toCsvField(null, true, true));
    }

    @Test
    public void testToCsvFieldEmptyString() {
        String input = "";
        assertSame(input, gpCSV.toCsvField(input, false, false));
        assertEquals("\"" + input, gpCSV.toCsvField(input, true, false));
        assertEquals(input + "\"", gpCSV.toCsvField(input, false, true));
        assertEquals("\"" + input + "\"", gpCSV.toCsvField(input, true, true));
    }

    @Test
    public void testToCsvHandlesNullInputs() {
        String input = "a";
        gpCSV.withNewline(null);
        assertSame(input, gpCSV.toCsvField(input, true, true, true));

        gpCSV.withQuoteChar("\"");
        gpCSV.withEscapeChar("\"");
        gpCSV.withNewline("\n");
        gpCSV.withDelimiter(null);
        assertSame(input, gpCSV.toCsvField(input, true, true, true));

        gpCSV.withQuoteChar("\"");
        gpCSV.withEscapeChar("\"");
        gpCSV.withNewline(null);
        gpCSV.withDelimiter(null);
        assertSame(input, gpCSV.toCsvField(input, true, true, true));
    }

    @Test
    public void testToCsvTestInputNewline() {
        String newline1 = "\n";
        String newline2 = "\r";
        String newline3 = "\r\n";

        // should not skip quoting
        assertEquals("\"\n\"", gpCSV.toCsvField(newline1, true, true, true)); // GreenplumCSV default newline character is \n
        gpCSV.withNewline("\r");
        assertEquals("\"\r\"", gpCSV.toCsvField(newline2, true, true, true));
        gpCSV.withNewline("\r\n");
        assertEquals("\"\r\n\"", gpCSV.toCsvField(newline3, true, true, true));
    }

    @Test
    public void testToCsvFieldStringWithCharacterThatNeedToBeQuoted() {
        String input1 = "a";

        // make sure no new object is created
        assertSame(input1, gpCSV.toCsvField(input1, true, true, true));

        String input2 = "\""; // quote
        assertEquals("\"\"\"\"", gpCSV.toCsvField(input2, true, true, true));

        String input3 = ","; // delimiter
        assertEquals("\",\"", gpCSV.toCsvField(input3, true, true, true));

        String input4 = "\n\",sample"; // newline, quote and delimiter
        assertEquals("\"\n\"\",sample\"", gpCSV.toCsvField(input4, true, true, true));
    }

    @Test
    public void testNewLines() {
        String input = "abc\ndef\n";
        gpCSV.withNewline("\n");

        assertEquals("\"abc\ndef\n\"", gpCSV.toCsvField(input, true, true, true));

        gpCSV.withNewline("\r\n");
        assertSame(input, gpCSV.toCsvField(input, true, true, true));
    }

    @Test
    public void testToCsvFieldStringWithoutQuoteCharacter() {
        String input = "aábcdefghijklmnñopqrstuvwxyz";

        assertSame(input, gpCSV.toCsvField(input, false, false));
        assertEquals("\"" + input, gpCSV.toCsvField(input, true, false));
        assertEquals(input + "\"", gpCSV.toCsvField(input, false, true));
        assertEquals("\"" + input + "\"",
                gpCSV.toCsvField(input, true, true));

        input = "aábcdefghijklm\"nñopqrstuvwxyz";

        gpCSV.withQuoteChar("|");
        gpCSV.withNewline("\r\n");
        assertSame(input, gpCSV.toCsvField(input, false, false, false));
        assertEquals("|" + input, gpCSV.toCsvField(input, true, false, false));
        assertEquals(input + "|", gpCSV.toCsvField(input, false, true, false));
        assertEquals("|" + input + "|", gpCSV.toCsvField(input, true, true, false));
    }

    @Test
    public void testToCsvDoesNotAddPrefixAndSuffixWhenSkipIfQuotingIsNotNeeded() {
        String input = "aábcdefghijklmnñopqrstuvwxyz";

        assertSame(input, gpCSV.toCsvField(input, false, false, true));
        assertSame(input, gpCSV.toCsvField(input, false, true, true));
        assertSame(input, gpCSV.toCsvField(input, true, false, true));
        assertSame(input, gpCSV.toCsvField(input, true, true, true));
    }

    @Test
    public void testToCsvFieldEscapesQuotes() {
        String input = "{\"key\": \"value\", \"foo\": \"bar\"}";
        String expected = "{\"\"key\"\": \"\"value\"\", \"\"foo\"\": \"\"bar\"\"}";

        assertEquals(expected, gpCSV.toCsvField(input, false, false));
        assertEquals("\"" + expected, gpCSV.toCsvField(input, true, false));
        assertEquals(expected + "\"", gpCSV.toCsvField(input, false, true));
        assertEquals("\"" + expected + "\"",
                gpCSV.toCsvField(input, true, true));
    }

    @Test
    public void testToCsvFieldSkipEscapesQuotes() {
        String input = "b,\"b";
        String expected = "b,\"b";

        gpCSV.withEscapeChar("off");
        gpCSV.withDelimiter(",");
        assertEquals(expected, gpCSV.toCsvField(input, false, false));
        assertEquals(expected, gpCSV.toCsvField(input, false, false, true));
        assertEquals("\"" + expected, gpCSV.toCsvField(input, true, false));
        assertEquals("\"" + expected, gpCSV.toCsvField(input, true, false, true));
        assertEquals(expected + "\"", gpCSV.toCsvField(input, false, true));
        assertEquals(expected + "\"", gpCSV.toCsvField(input, false, true, true));
        assertEquals("\"" + expected + "\"", gpCSV.toCsvField(input, true, true));
        assertEquals("\"" + expected + "\"", gpCSV.toCsvField(input, true, true, true));

        input = "b,\nb";
        expected = "b,\nb";

        gpCSV.withEscapeChar("off");
        gpCSV.withDelimiter(",");
        assertEquals(expected, gpCSV.toCsvField(input, false, false, true));
        assertEquals("\"" + expected, gpCSV.toCsvField(input, true, false));
        assertEquals("\"" + expected, gpCSV.toCsvField(input, true, false, true));
        assertEquals(expected + "\"", gpCSV.toCsvField(input, false, true));
        assertEquals(expected + "\"", gpCSV.toCsvField(input, false, true, true));
        assertEquals("\"" + expected + "\"", gpCSV.toCsvField(input, true, true));
        assertEquals("\"" + expected + "\"", gpCSV.toCsvField(input, true, true, true));
    }

    @Test
    public void testToCsvFieldSkipEscapeWhenDelimiterIsOff() {
        String input = "b,b";
        String expected = "b,b";

        gpCSV.withEscapeChar("\"");
        gpCSV.withDelimiter("off");
        assertEquals(expected, gpCSV.toCsvField(input, false, false));
        assertEquals("\"" + expected, gpCSV.toCsvField(input, true, false));
        assertEquals(expected + "\"", gpCSV.toCsvField(input, false, true));
        assertEquals("\"" + expected + "\"",
                gpCSV.toCsvField(input, true, true));
    }

    @Test
    public void testToCsvFieldEscapesQuoteChar() {
        char quoteChar = '|';
        String input = "a|b|c|d\ne|f|g|h";
        String expected = "a||b||c||d\ne||f||g||h";

        gpCSV.withQuoteChar("|");
        gpCSV.withEscapeChar("|");
        gpCSV.withNewline("\r\n");

        assertEquals(quoteChar + expected, gpCSV.toCsvField(input, true, false, false));
        assertEquals(expected, gpCSV.toCsvField(input, false, false, false));
        assertEquals(expected + quoteChar, gpCSV.toCsvField(input, false, true, false));
        assertEquals(quoteChar + expected + quoteChar,
                gpCSV.toCsvField(input, true, true, false));
    }

    @Test
    public void testCsvOptionWithValueOfNullDefault() {
        assertEquals("", gpCSV.getValueOfNull());

        gpCSV.withValueOfNull("");
        assertEquals("", gpCSV.getValueOfNull());
    }

    @Test
    public void testCsvOptionWithValueOfNullCustomized() {
        gpCSV.withValueOfNull("a");
        assertEquals("a", gpCSV.getValueOfNull());
    }

    @Test
    public void testCsvOptionWithQuoteCharValid() {
        gpCSV.withQuoteChar("\"");
        assertEquals('\"', gpCSV.getQuote());
    }

    @Test
    public void testCsvOptionWithQuoteCharEmpty() {
        gpCSV.withQuoteChar("");
        assertEquals('"', gpCSV.getQuote());
    }

    @Test
    public void testCsvOptionWithQuoteCharInvalidLength() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("invalid QUOTE character '\"\"'. Only single character is allowed for QUOTE.");
        gpCSV.withQuoteChar("\"\"");
    }

    @Test
    public void testCsvOptionWithEscapeCharValid() {
        gpCSV.withEscapeChar("\\");
        assertEquals(new Character('\\'), gpCSV.getEscape());
    }

    @Test
    public void testCsvOptionWithEscapeCharEmpty() {
        gpCSV.withEscapeChar("");
        assertEquals(new Character('"'), gpCSV.getEscape());
    }

    @Test
    public void testCsvOptionWithEscapeCharInvalidLength() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("invalid ESCAPE character '\\\\'. Only single character is allowed for ESCAPE.");
        gpCSV.withEscapeChar("\\\\");
    }

    @Test
    public void testCsvOptionWithNewlineValid() {
        gpCSV.withNewline("\n");
        assertEquals("\n", gpCSV.getNewline());
        gpCSV.withNewline("\r");
        assertEquals("\r", gpCSV.getNewline());
        gpCSV.withNewline("\r\n");
        assertEquals("\r\n", gpCSV.getNewline());
        gpCSV.withNewline("LF");
        assertEquals("\n", gpCSV.getNewline());
        gpCSV.withNewline("CR");
        assertEquals("\r", gpCSV.getNewline());
        gpCSV.withNewline("CRLF");
        assertEquals("\r\n", gpCSV.getNewline());
    }

    @Test
    public void testCsvOptionWithNewlineEmpty() {
        gpCSV.withNewline("");
        assertEquals("\n", gpCSV.getNewline());
    }

    @Test
    public void testCsvOptionWithNewlineInvalid() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("invalid newline character '\\\\'. Only LF, CR, or CRLF are supported for newline.");
        gpCSV.withNewline("\\\\");
    }

    @Test
    public void testCsvOptionWithDelimiterValid() {
        gpCSV.withDelimiter("|");
        assertEquals(new Character('|'), gpCSV.getDelimiter());
    }

    @Test
    public void testCsvOptionWithDelimiterEmpty() {
        gpCSV.withDelimiter("");
        assertEquals(new Character(','), gpCSV.getDelimiter());
    }

    @Test
    public void testCsvOptionWithDelimiterInvalid() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("invalid DELIMITER character '\\\\'. Only single character is allowed for DELIMITER.");
        gpCSV.withDelimiter("\\\\");
    }

    @Test
    public void testShouldDisableDelimiterWithOffDelimiter() {
        gpCSV.withDelimiter("OFF");
        assertNull(gpCSV.getDelimiter());
    }

    @Test
    public void testShouldDisableEscapeWithOffEscape() {
        gpCSV.withEscapeChar("OFF");
        assertNull(gpCSV.getEscape());
    }
}
