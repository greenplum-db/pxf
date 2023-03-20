package org.greenplum.pxf.api.model;

import org.apache.commons.lang.StringUtils;

/**
 * Greenplum CSV Default
 */
public class GreenplumCSV {

    /**
     * Greenplum CSV Defaults
     */
    private static final char QUOTE = '"';
    private static final char ESCAPE = '"';
    private static final char DELIMITER = ',';
    private static final String NEWLINE = "\n";
    private static final String VALUE_OF_NULL = "";

    private String valueOfNull;
    private char quote;
    private Character escape;
    private String newline;
    private Character delimiter;

    private int newlineLength;

    /**
     * Initialize with Greenplum CSV defaults
     */
    public GreenplumCSV() {
        withQuoteChar(QUOTE);
        withEscapeChar(ESCAPE);
        withNewline(NEWLINE);
        withDelimiter(DELIMITER);
        withValueOfNull(VALUE_OF_NULL);
    }

    public String getValueOfNull() {
        return valueOfNull;
    }

    public char getQuote() {
        return quote;
    }

    public Character getEscape() {
        return escape;
    }

    public String getNewline() {
        return newline;
    }

    public Character getDelimiter() {
        return delimiter;
    }

    /**
     * Set value of null string for parsing CSV. If the string input for customization is
     * null, it will be ignored.
     *
     * @param valueOfNull the null character to be set
     * @return GreenplumCSV object for builder pattern
     */
    public GreenplumCSV withValueOfNull(String valueOfNull) {
        if (valueOfNull != null) {
            this.valueOfNull = valueOfNull;
        }
        return this;
    }

    /**
     * Set quote character for parsing CSV with customized character. Will raise
     * IllegalArgumentException if the input cannot be converted to char type.
     * Empty string input will be ignored.
     *
     * @param quoteString the quote character to be set
     * @return GreenplumCSV object for builder pattern
     */
    public GreenplumCSV withQuoteChar(String quoteString) {
        if (StringUtils.isNotEmpty(quoteString)) {
            validateSingleCharacter(quoteString, "QUOTE");
            withQuoteChar(quoteString.charAt(0));
        }
        return this;
    }

    /**
     * Set quote character for parsing CSV with customized character.
     *
     * @param quoteChar the quote character to be set
     * @return GreenplumCSV object for builder pattern
     */
    public GreenplumCSV withQuoteChar(char quoteChar) {
        this.quote = quoteChar;
        return this;
    }

    /**
     * Set escape character for parsing CSV with customized character. Will raise
     * IllegalArgumentException if the input cannot be converted to char type.
     * Empty string input will be ignored.
     *
     * @param escapeString the escape character to be set
     * @return GreenplumCSV object for builder pattern
     */
    public GreenplumCSV withEscapeChar(String escapeString) {
        if (StringUtils.equalsIgnoreCase("OFF", escapeString)) {
            escape = null;
        } else if (StringUtils.isNotEmpty(escapeString)) {
            validateSingleCharacter(escapeString, "ESCAPE");
            withEscapeChar(escapeString.charAt(0));
        }
        return this;
    }

    /**
     * Set escape character for parsing CSV with customized character.
     *
     * @param escapeChar the escape character to be set
     * @return GreenplumCSV object for builder pattern
     */
    public GreenplumCSV withEscapeChar(char escapeChar) {
        escape = escapeChar;
        return this;
    }

    /**
     * Set newline character for parsing CSV with designated newline character.
     * Empty string input will be ignored.
     *
     * @param newline the newline character to be set
     * @return GreenplumCSV object for builder pattern
     */
    public GreenplumCSV withNewline(String newline) {
        if (StringUtils.isNotEmpty(newline)) {
            // translate literal representation (CR. LF, CRLF) for (\r , \n , \r\n)
            // Greenplum TEXT / CSV formatter only supports CR (Carriage return, 0x0D), LF (Line feed, 0x0A), or
            // CRLF (Carriage return plus line feed, 0x0D 0x0A) as newline characters
            // but the "fixedwidth" formatter supports custom line delimiters

            switch (newline.toLowerCase()) {
                case "cr":
                    this.newline = "\r";
                    break;

                case "lf":
                    this.newline = "\n";
                    break;

                case "crlf":
                    this.newline = "\r\n";
                    break;

                default:
                    this.newline = newline;
            }
        }
        this.newlineLength = newline != null ? newline.length() : 0;
        return this;
    }

    /**
     * Set delimiter character for parsing CSV with customized character. Will raise
     * IlligalArugmentException if the input cannot be converted to char type.
     * Empty string input will be ignored.
     *
     * @param delimiterString the delimiter to be set
     * @return GreenplumCSV object for builder pattern
     */
    public GreenplumCSV withDelimiter(String delimiterString) {
        if (StringUtils.equalsIgnoreCase("OFF", delimiterString)) {
            delimiter = null;
        } else if (StringUtils.isNotEmpty(delimiterString)) {
            validateSingleCharacter(delimiterString, "DELIMITER");
            withDelimiter(delimiterString.charAt(0));
        }
        return this;
    }

    /**
     * Set delimiter character for parsing CSV with customized character.
     *
     * @param delimiterChar the delimiter to be set
     * @return GreenplumCSV object for builder pattern
     */
    public GreenplumCSV withDelimiter(char delimiterChar) {
        delimiter = delimiterChar;
        return this;
    }

    private void validateSingleCharacter(String s, String name) {
        if (s.length() > 1) {
            throw new IllegalArgumentException(String.format(
                    "invalid %s character '%s'. Only single character is allowed for %s.", name, s, name));
        }
    }

    /**
     * Escapes CSV quotes (") to form a valid CSV string
     *
     * @param s                the input string
     * @param prependQuoteChar true to prepend quotes (") to s, false otherwise
     * @param appendQuoteChar  true to append quotes (") to s, false otherwise
     * @return an escaped CSV string
     */
    public String toCsvField(String s, boolean prependQuoteChar, boolean appendQuoteChar) {
        return toCsvField(s, prependQuoteChar, appendQuoteChar, false);
    }


    /**
     * Escapes the provided quote char to form a valid CSV string
     *
     * @param s                        the input string
     * @param prependQuoteChar         true to prepend the quote char to s, false otherwise
     * @param appendQuoteChar          true to append the quote char to s, false otherwise
     * @param skipIfQuotingIsNotNeeded skip if quoting is not needed
     * @return an escaped CSV string
     */
    public String toCsvField(String s,
                             boolean prependQuoteChar,
                             boolean appendQuoteChar,
                             boolean skipIfQuotingIsNotNeeded) {
        if (s == null) return null;

        final int length = s.length();
        int i, j, quotes = 0, specialChars = 0, pos = 0, total = length;

        // count all the quotes
        for (i = 0; i < length; i++) {
            char curr = s.charAt(i);
            if (escape != null && curr == quote) quotes++;
            if (delimiter != null && curr == delimiter)
                specialChars++;
            if (newlineLength > 0) {
                j = 0;

                // let's say we have input asd\r\nacd
                // and newline \r\n then we need to
                // increase the specialChars count by 1

                while (i < length && j < newlineLength
                        && newline.charAt(j) == s.charAt(i)) {
                    j++;
                    if (j < newlineLength) i++;
                }

                if (j == newlineLength) specialChars++;
            }
        }

        if (prependQuoteChar) total += 1;
        if (appendQuoteChar) total += 1;
        total += quotes;

        // if there are QUOTE, DELIMITER, NEWLINE characters
        // in the string we also need to quote the CSV field
        if (length == total || (skipIfQuotingIsNotNeeded && quotes == 0 && specialChars == 0))
            return s;

        char[] chars = new char[total];

        if (prependQuoteChar) chars[pos++] = quote;

        for (i = 0; i < length; i++) {
            if (escape != null && quotes > 0 && s.charAt(i) == quote)
                chars[pos++] = escape; // escape quote char
            chars[pos++] = s.charAt(i);
        }

        if (appendQuoteChar) chars[pos] = quote;

        return new String(chars);
    }
}
