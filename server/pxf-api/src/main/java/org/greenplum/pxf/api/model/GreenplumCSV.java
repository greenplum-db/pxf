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
    private char escape;
    private String newline;
    private Character delimiter;

    /**
     * Initialize with Greenplum CSV defaults
     */
    public GreenplumCSV() {
        quote = QUOTE;
        escape = ESCAPE;
        newline = NEWLINE;
        delimiter = DELIMITER;
        valueOfNull = VALUE_OF_NULL;
    }

    public String getValueOfNull() {
        return valueOfNull;
    }

    public char getQuote() {
        return quote;
    }

    public char getEscape() {
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
            quote = quoteString.charAt(0);
        }
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
        if (StringUtils.isNotEmpty(escapeString)) {
            validateSingleCharacter(escapeString, "ESCAPE");
            escape = escapeString.charAt(0);
        }
        return this;
    }

    /**
     * Set newline character for parsing CSV with designated newline character. Will
     * raise IllegalArgumentException if the input is not supported newline character.
     * Empty string input will be ignored.
     *
     * @param newline the newline character to be set
     * @return GreenplumCSV object for builder pattern
     */
    public GreenplumCSV withNewline(String newline) {
        if (StringUtils.isNotEmpty(newline)) {
            // validate that it is \n or \r or \r\n
            // Greenplum only support LF (Line feed, 0x0A), CR (Carriage return, 0x0D), or
            // CRLF (Carriage return plus line feed, 0x0D 0x0A) as newline character
            if (newline.equals("\n") || newline.equals("\r") || newline.equals("\r\n")) {
                this.newline = newline;
            } else {
                throw new IllegalArgumentException(String.format(
                        "invalid newline character '%s'. Only LF, CR, or CRLF are supported for newline.", newline));
            }
        }
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
        if (StringUtils.isNotEmpty(delimiterString)) {
            validateSingleCharacter(delimiterString, "DELIMITER");
            delimiter = delimiterString.charAt(0);
        }
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
            if (curr == quote) quotes++;
            if (delimiter != null && curr == delimiter) specialChars++;
            if (newline != null && newline.length() > 0) {

                j = 0;

                // let's say we have input asd\r\nacd
                // and newline \r\n then we need to
                // increase the specialChars count by 1

                while (i < length && j < newline.length()
                        && newline.charAt(j) == s.charAt(i)) {
                    j++;
                    if (j < newline.length()) i++;
                }

                if (j == newline.length()) specialChars++;
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
            if (quotes > 0 && s.charAt(i) == quote)
                chars[pos++] = escape; // escape quote char
            chars[pos++] = s.charAt(i);
        }

        if (appendQuoteChar) chars[pos] = quote;

        return new String(chars);
    }
}
