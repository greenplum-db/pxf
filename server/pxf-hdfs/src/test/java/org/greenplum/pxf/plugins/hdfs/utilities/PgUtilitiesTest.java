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

import org.assertj.core.util.Arrays;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PgUtilitiesTest {
    private PgUtilities pgUtilities;

    @BeforeEach
    public void setup() {
        pgUtilities = new PgUtilities();
    }

    @Test
    public void testSplitArrayNumericDataTypes() {
        // for numeric data types it is safe to assume that double quotes will never appear
        // https://www.postgresql.org/docs/9.4/arrays.html
        String[] intArraySplits = pgUtilities.splitArray("{1,2,3}");
        assertArrayEquals(Arrays.array("1", "2", "3"), intArraySplits);

        String[] numericArraySplits = pgUtilities.splitArray("{500.215,NULL,500.214,NaN}");
        assertArrayEquals(Arrays.array("500.215", null, "500.214", "NaN"), numericArraySplits);
    }

    @Test
    public void testSplitArrayText() {
        String textArrayLiteral = "{\"first string\",second-string,\"string with a \\\"quote\\\" inside\"}";
        String[] textArraySplits = pgUtilities.splitArray(textArrayLiteral);
        assertArrayEquals(
                Arrays.array("first string", "second-string", "string with a \"quote\" inside"),
                textArraySplits
        );
    }

    @Test
    public void testSplitArrayBytea() {
        String byteaHexArrayLiteral = "{\"\\\\x01\",\"\\\\x2345\",\"\\\\x6789ab\"}";
        String[] byteaHexSplits = pgUtilities.splitArray(byteaHexArrayLiteral);
        assertArrayEquals(
                Arrays.array("\\x01", "\\x2345", "\\x6789ab"),
                byteaHexSplits
        );

        String byteaEscapeArrayLiteral = "{\"\\\\001\",#E,\"g\\\\211\\\\253\"}";
        String[] byteaEscapeSplits = pgUtilities.splitArray(byteaEscapeArrayLiteral);
        assertArrayEquals(
                Arrays.array("\\001", "#E", "g\\211\\253"),
                byteaEscapeSplits
        );
    }

    @Test
    public void testSplitArrayInvalid() {
        String invalidArrayLiteral = "{1,2} is not a valid array";
        Exception e = assertThrows(PxfRuntimeException.class, () -> pgUtilities.splitArray(invalidArrayLiteral));
        assertEquals("array dimension mismatch, rawData: {1,2} is not a valid array", e.getMessage());
    }

    @Test
    void testUnescapeArrayElementEmptyString() {
        assertEquals("", pgUtilities.unescapeArrayElement("\"\""));
    }

    @Test
    void testUnescapeArrayElementNullString() {
        assertNull(pgUtilities.unescapeArrayElement("NULL"));
    }

    @Test
    void testUnescapeArrayElementNullLiteral() {
        assertEquals("null", pgUtilities.unescapeArrayElement("\"null\""));
        assertEquals("Null", pgUtilities.unescapeArrayElement("\"Null\""));
        assertEquals("NULL", pgUtilities.unescapeArrayElement("\"NULL\""));
    }

    @Test
    void testUnescapeArrayElementNoSpecialChars() {
        assertEquals("simple", pgUtilities.unescapeArrayElement("simple"));
    }

    @Test
    void testUnescapeArrayElementWithWhitespace() {
        assertEquals("contains whitespace", pgUtilities.unescapeArrayElement("\"contains whitespace\""));
        assertEquals("contains\twhitespace", pgUtilities.unescapeArrayElement("\"contains\twhitespace\""));
    }

    @Test
    void testUnescapeArrayElementWithCurlyBraces() {
        assertEquals("{1,2} is a valid string", pgUtilities.unescapeArrayElement("\"{1,2} is a valid string\""));
    }

    @Test
    void testUnescapeArrayElementWithBackslashes() {
        assertEquals("a string that contains \\", pgUtilities.unescapeArrayElement("\"a string that contains \\\\\""));
    }

    @Test
    void testEscapeArrayElementEmptyString() {
        assertEquals("\"\"", pgUtilities.escapeArrayElement(""));
    }

    @Test
    void testEscapeArrayElementNullString() {
        assertEquals("NULL", pgUtilities.escapeArrayElement(null));
    }

    @Test
    void testEscapeArrayElementNullLiteral() {
        assertEquals("\"null\"", pgUtilities.escapeArrayElement("null"));
        assertEquals("\"Null\"", pgUtilities.escapeArrayElement("Null"));
        assertEquals("\"NULL\"", pgUtilities.escapeArrayElement("NULL"));
    }

    @Test
    void testEscapeArrayElementNoSpecialChars() {
        assertEquals("simple", pgUtilities.escapeArrayElement("simple"));
    }

    @Test
    void testEscapeArrayElementWithWhitespace() {
        assertEquals("\"contains whitespace\"", pgUtilities.escapeArrayElement("contains whitespace"));
        assertEquals("\"contains\twhitespace\"", pgUtilities.escapeArrayElement("contains\twhitespace"));
    }

    @Test
    void testEscapeArrayElementWithCurlyBraces() {
        assertEquals("\"{1,2} is a valid string\"", pgUtilities.escapeArrayElement("{1,2} is a valid string"));
    }

    @Test
    void testEscapeArrayElementWithBackslashes() {
        assertEquals("\"a string that contains \\\\\"", pgUtilities.escapeArrayElement("a string that contains \\"));
    }

    @Test
    void testEncodeAndEscapeByteaHex() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{0x01});
        assertEquals("\"\\\\x01\"", pgUtilities.encodeAndEscapeByteaHex(buffer));

        buffer = ByteBuffer.wrap(new byte[]{0x01, 0x23});
        assertEquals("\"\\\\x0123\"", pgUtilities.encodeAndEscapeByteaHex(buffer));

        buffer = ByteBuffer.wrap(new byte[]{(byte) 0xab, (byte) 0xef});
        assertEquals("\"\\\\xabef\"", pgUtilities.encodeAndEscapeByteaHex(buffer));
    }

    @Test
    public void testParsePgBoolLiteral() {
        assertTrue(pgUtilities.parseBoolLiteral("t"));
        assertFalse(pgUtilities.parseBoolLiteral("f"));

        assertThrows(PxfRuntimeException.class, () -> pgUtilities.parseBoolLiteral("true"));
        assertThrows(PxfRuntimeException.class, () -> pgUtilities.parseBoolLiteral("false"));
        assertThrows(PxfRuntimeException.class, () -> pgUtilities.parseBoolLiteral("this is just a string"));
    }

    @Test
    public void testParseHexFormat() {
        ByteBuffer byteBuffer = pgUtilities.parseHexFormat("\\x0123456789ABCDEF");
        assertArrayEquals(new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF}, byteBuffer.array());

        assertThrows(PxfRuntimeException.class, () -> pgUtilities.parseHexFormat("\\xghi"));
    }

    @Test
    public void testParseEscapeFormat() {
        ByteBuffer byteBuffer = pgUtilities.parseEscapeFormat("\\000");
        assertEquals(ByteBuffer.wrap(new byte[]{0x00}), byteBuffer);

        byteBuffer = pgUtilities.parseEscapeFormat("\\\\");
        assertEquals(ByteBuffer.wrap(new byte[]{(byte) 0x5C}), byteBuffer);

        byteBuffer = pgUtilities.parseEscapeFormat("\\001#Eg\\211\\253\\315\\357");
        assertEquals(
                ByteBuffer.wrap(new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF}),
                byteBuffer
        );

        byteBuffer = pgUtilities.parseEscapeFormat("\"");
        assertEquals(ByteBuffer.wrap(new byte[]{0x22}), byteBuffer);
    }

    @Test
    public void testParsePgByteaLiteral() {
        ByteBuffer byteBuffer = pgUtilities.parseByteaLiteral("\\x0123456789ABCDEF");
        assertEquals(
                ByteBuffer.wrap(new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF}),
                byteBuffer
        );

        byteBuffer = pgUtilities.parseByteaLiteral("\\001#Eg\\211\\253\\315\\357");
        assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF}),
                byteBuffer
        );

    }
}
