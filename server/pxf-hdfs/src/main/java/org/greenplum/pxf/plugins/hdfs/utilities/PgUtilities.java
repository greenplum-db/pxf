package org.greenplum.pxf.plugins.hdfs.utilities;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.springframework.stereotype.Component;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for converting between Java types and Postgres types text format
 */
@Component
public class PgUtilities {

    private static String QUOTED_ARRAY_ELEMENT_FORMAT = "\"%s\"";
    private static String UNQUOTED_ARRAY_ELEMENT_FORMAT = "%s";

    /**
     * Split the outer-most Postgres array into its constituent elements using the default delimiter
     *
     * Re-used from GPHDFS, commit 3b0bfdc169
     * @param str string with a Postgres array
     * @return array of strings for each element
     */
    public String[] splitArray(String str) {
        /* For most Postgres types, the array delimiter is `,` and currently only `box` uses something else (`;`)
         * gpadmin=# select typdelim, count(*) from pg_type group by typdelim;
         *  typdelim | count
         * ----------+-------
         *  ,        |   444
         *  ;        |     2
         * gpadmin=# select typname from pg_type where typdelim = ';';
         *  typname
         * ---------
         *  box
         *  _box
         * Since PXF doesn't currently support the box type, we just assume a default of ','.
         */
        char DEFAULT_ARRAY_DELIMITER = ',';
        return splitArray(str.toCharArray(), DEFAULT_ARRAY_DELIMITER);
    }

    /**
     * Split the outer-most Postgres array into its constituent elements
     *
     * Re-used from GPHDFS, commit 3b0bfdc169
     * @param value string with a Postgres array
     * @param delimiter character separating array elements in value
     * @return array of strings for each element
     */
    private String[] splitArray(char[] value, char delimiter) {
        List<Integer> posList = new ArrayList<>();
        posList.add(0);

        if (value[0] != '[' || value[value.length - 1] != ']') {
            throw new PxfRuntimeException(String.format("array dimension mismatch, rawData: %s", new String(value)));
        }

        // handle the empty array {} case
        if (isEmptyArray(value)) {
            return new String[0];
        }

        int depth = 0;
        boolean inQuoted = false;
        for (int i = 0; i < value.length; i++) {
            if (value[i] == delimiter) {
                if (depth == 1 && !inQuoted) {
                    posList.add(i);
                }

                continue;
            }
            switch (value[i]) {
                case '[':
                    if (!inQuoted) {
                        depth++;
                    }
                    break;
                case ']':
                    if (!inQuoted) {
                        depth--;
                        if (depth == 0) {
                            posList.add(i);
                        }
                    }
                    break;
                case '\"':
                    if (isQuote(value, i)) {
                        inQuoted = !inQuoted;
                    }
                    break;
                default:
                    break;
            }
        }

        String[] subStrings = new String[posList.size() - 1];
        for (int i = 0; i < posList.size() - 1; i++) {
            subStrings[i] = unescapeArrayElement(new String(value, posList.get(i) + 1, posList.get(i + 1) - posList.get(i) - 1));
        }
        return subStrings;

    }

    /**
     * Returns a <code>String</code> for an unescaped Postgres array element.
     *
     * If the value is enclosed in double quotes, they are removed. Backslash-escaped double quotes and backslashes
     * embedded in the string will unescaped.
     * @param str
     * @return string representing Postgres array
     */
    public String unescapeArrayElement(String str) {
        if (StringUtils.equals(str, "NULL")) {
            return null;
        }

        int beginIndx = 0;
        int endIndex = str.length();
        /*
         * The array output routine will put double quotes around element values if they are empty strings, contain
         * curly braces, delimiter characters, double quotes, backslashes, or white space, or match the word `NULL`.
         */
        if (str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"') {
            beginIndx += 1;
            endIndex -= 1;
        }

        return StringEscapeUtils.unescapeJava(str.substring(beginIndx, endIndex));
    }

    /**
     * Escapes the characters in a <code>String</code> using Postgres array element rules.
     *
     * Puts double quotes around element values if they are empty strings, contain curly braces, delimiter
     * characters, double quotes, backslashes, or white space, or match the word NULL. Double quotes and
     * backslashes embedded in the string will be backslash-escaped
     *
     * We cannot use StringEscapeUtils.escapeJava() because it will escape characters like `\t` which Postgres does not
     * do for array elements.
     * @param str String to escape values in, may be null
     * @return String with escaped values
     */
    public String escapeArrayElement(String str) {
        if (str == null) {
            return "NULL";
        } else if (StringUtils.equalsIgnoreCase(str, "NULL")) {
            // force quotes for literal NULL
            return "\"" + str + "\"";
        } else if (StringUtils.isEmpty(str)) {
            // force quotes for empty string
            return "\"\"";
        }

        boolean needsQuote = false;
        StringWriter writer = new StringWriter(str.length() * 2);

        char[] chars = str.toCharArray();
        for (char c : chars) {
            switch (c) {
                case '"':
                case '\\':
                    needsQuote = true;
                    writer.write('\\');
                    writer.write(c);
                    break;
                case '{':
                case '}':
                case ',':
                    needsQuote = true;
                    writer.write(c);
                    break;
                default:
                    if (arrayIsSpace(c)) {
                        needsQuote = true;
                    }
                    writer.write(c);
                    break;
            }
        }

        return String.format(needsQuote ? QUOTED_ARRAY_ELEMENT_FORMAT : UNQUOTED_ARRAY_ELEMENT_FORMAT, writer);
    }

    /**
     * Converts a byte buffer into a String containing Postgres bytea hex format.
     * @param data a byte buffer to convert to bytea hex format
     * @return a string containing Postgres bytea hex format
     */
    public String encodeAndEscapeByteaHex(ByteBuffer data) {
        return escapeArrayElement(encodeByteaHex(data));
    }

    /**
     * Converts a byte buffer into a String containing bytea hex format.
     * @param data a byte buffer to convert to bytea hex format
     * @return a string containing bytea hex format
     */
    public String encodeByteaHex(ByteBuffer data) {
        return String.format("\\x%s", Hex.encodeHexString(data));
    }

    /**
     * Converts a Postgres boolean from text format ("t" or "f") to Java boolean
     * @param value string containing a valid Postgres boolean in text format
     * @return Java boolean
     * @throws {@link PxfRuntimeException} if the input is not a valid text format Postgres boolean
     */
    public boolean parseBoolLiteral(final String value) {
        if (StringUtils.equals(value, "t") || StringUtils.equals(value, "true")) {
            return true;
        } else if (StringUtils.equals(value, "f") || StringUtils.equals(value, "false")) {
            return false;
        }

        throw new PxfRuntimeException(String.format("malformed boolean literal \"%s\"", value));
    }

    /**
     * Parses a text format element from of a bytea in either escape or hex output
     * @param value string to be parsed
     * @return byte buffer containing the sequence of bytes represented by the input
     */
    public ByteBuffer parseByteaLiteral(String value) {
        if (value.startsWith("\\x")) {
            return parseHexFormat(value);
        } else {
            return parseEscapeFormat(value);
        }
    }

    /**
     * Parses a hex output text format element of bytea
     * @param value string to be parsed
     * @return byte buffer containing the sequence of bytes represented by the input
     * @throws {@link PxfRuntimeException} if input is invalid/malformed
     */
    ByteBuffer parseHexFormat(String value) {
        try {
            return ByteBuffer.wrap(Hex.decodeHex(value.substring(2).toCharArray()));
        } catch (DecoderException e) {
            throw new PxfRuntimeException(String.format("malformed bytea literal \"%s\"", value), e);
        }
    }

    /**
     * Parses an escape output text format element from a bytea array
     *
     * > The "escape" format is the traditional PostgreSQL format for the bytea type.It takes the approach of
     * > representing a binary string as a sequence of ASCII characters, while converting those bytes that cannot
     * > be represented as an ASCII character into special escape sequences.
     *
     * from: https://www.postgresql.org/docs/9.4/datatype-binary.html
     *
     * @param value string to be parsed
     * @return buffer containing the sequence of bytes represented by the input
     */
    ByteBuffer parseEscapeFormat(String value) {
        // a bytea contains at most as many bytes as the number of characters in its text format
        ByteBuffer buffer = ByteBuffer.allocate(value.length());
        int index = 0, length = value.length();

        while (index < length) {
            // see 'Table 8-8. bytea Output Escaped Octets'
            // https://www.postgresql.org/docs/9.4/datatype-binary.html
            if (value.charAt(index) == '\\') {
                // either backslash or a non-printable octect
                if (value.charAt(index + 1) == '\\') {
                    // backslash
                    buffer.put((byte) 0x5C);
                    index += 2;
                } else {
                    // non-printable octect
                    buffer.put(parseUnsignedByte(value.substring(index + 1, index + 4), 8));
                    index += 4;
                }
            } else {
                buffer.put((byte) value.charAt(index));
                index++;
            }
        }

        buffer.flip();
        return buffer;
    }

    /**
     * Parses the string argument as an unsigned byte in the radix specified by the second argument.
     *
     * Note that we cannot use [@code Byte.parseByte} because it parses a _signed_ byte.
     * @param s the String containing the byte representation to be parsed
     * @param radix the radix to be used while parsing s
     * @return the byte value represented by the string argument in the specified radix
     */
    private byte parseUnsignedByte(String s, int radix) {
        int i = Integer.parseInt(s, radix);
        if (i >= 0 && i <= 255) {
            return (byte) i;
        } else {
            throw new NumberFormatException("Value out of range for byte. Value:\"" + s + "\" Radix:" + radix);
        }
    }

    private boolean arrayIsSpace(char c) {
        return (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == 0x0b || c == '\f');
    }

    private boolean isEmptyArray(char[] value) {
        return (value[0] == '{' && value[1] == '}');
    }

    private boolean isQuote(char[] value, int index) {
        int num = 0;
        for (int i = index - 1; i >= 0; i--) {
            if (value[i] == '\\') {
                num++;
            } else {
                break;
            }
        }

        return num % 2 == 0;
    }
}
