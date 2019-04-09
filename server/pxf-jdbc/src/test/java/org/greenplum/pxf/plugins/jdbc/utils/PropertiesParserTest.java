package org.greenplum.pxf.plugins.jdbc.utils;

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

import java.util.Properties;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class PropertiesParserTest {
    @Test
    public void testParseOneProperty() {
        final String toParse = "a=1";

        Properties expected = new Properties();
        expected.setProperty("a", "1");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParseTwoProperties() {
        final String toParse = "a=1,b=2";

        Properties expected = new Properties();
        expected.setProperty("a", "1");
        expected.setProperty("b", "2");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParseEscapedCommaInKey() {
        final String toParse = "a\\,a=1";

        Properties expected = new Properties();
        expected.setProperty("a,a", "1");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParseEscapedCommaInKey2() {
        final String toParse = "a\\,a=1,b=2";

        Properties expected = new Properties();
        expected.setProperty("a,a", "1");
        expected.setProperty("b", "2");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParse2EscapedCommasInKeys() {
        final String toParse = "a\\,a=1,b=2,\\,cc=3";

        Properties expected = new Properties();
        expected.setProperty("a,a", "1");
        expected.setProperty("b", "2");
        expected.setProperty(",cc", "3");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParse2EscapedCommasInValues() {
        final String toParse = "a=1\\,1,b=22\\,22,c=3";

        Properties expected = new Properties();
        expected.setProperty("a", "1,1");
        expected.setProperty("b", "22,22");
        expected.setProperty("c", "3");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParseEscapedEqInKey() {
        final String toParse = "a\\=a=1";

        Properties expected = new Properties();
        expected.setProperty("a=a", "1");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParseEscapedEqInKey2() {
        final String toParse = "a\\=a=1,b=2";

        Properties expected = new Properties();
        expected.setProperty("a=a", "1");
        expected.setProperty("b", "2");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParse2EscapedEqsInKeys() {
        final String toParse = "a\\=a=1,b=2,\\=cc=3";

        Properties expected = new Properties();
        expected.setProperty("a=a", "1");
        expected.setProperty("b", "2");
        expected.setProperty("=cc", "3");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParse2EscapedEqsInValues() {
        final String toParse = "a=1\\=1,b=22\\=22,c=3";

        Properties expected = new Properties();
        expected.setProperty("a", "1=1");
        expected.setProperty("b", "22=22");
        expected.setProperty("c", "3");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    // This must return the same result as in previous test as
    // '\=' escaping is required only for keys; values can contain unescaped equality signs
    @Test
    public void testParse2UnescapedEqsInValues() {
        final String toParse = "a=1=1,b=22=22,c=3";

        Properties expected = new Properties();
        expected.setProperty("a", "1=1");
        expected.setProperty("b", "22=22");
        expected.setProperty("c", "3");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test
    public void testParseMix() {
        final String toParse = "a=1=1,b=22\\=22,c\\,c=3,dd\\=d\\,=4\\,4";

        Properties expected = new Properties();
        expected.setProperty("a", "1=1");
        expected.setProperty("b", "22=22");
        expected.setProperty("c,c", "3");
        expected.setProperty("dd=d,", "4,4");

        Properties actual = PropertiesParser.parse(toParse);

        assertEquals(expected, actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowExceptionOnEscapeCharMissing() {
        final String toParse = "a=1,b,b=2";

        PropertiesParser.parse(toParse);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowExceptionOnEmptyKey() {
        final String toParse = "a=1,=2,c=3";

        PropertiesParser.parse(toParse);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowExceptionOnEmptyValue() {
        final String toParse = "a=1,b=,c=3";

        PropertiesParser.parse(toParse);
    }
}
