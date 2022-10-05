package org.greenplum.pxf.plugins.json.parser;

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


import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionedJsonParserTest {

    @Test
    public void testFoundIdentifier() {

        PartitionedJsonParser parser = new PartitionedJsonParser("name");
        parser.startNewJsonObject();

        Text result = new Text();
        boolean completed = false;
        String jsonContents = "\"name\"";
        for (int i = 0; i < jsonContents.length(); i++) {
            char ch = jsonContents.charAt(i);
            completed = parser.buildNextObjectContainingMember(ch, result);
        }

        assertFalse(completed);
        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(0, result.getLength());
    }

    @Test
    public void testSimpleMatchingIdentifier() throws URISyntaxException, IOException {

        
        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/simple.json").toURI());

        InputStreamReader jsonStreamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);

        PartitionedJsonParser parser = new PartitionedJsonParser("name");

        int i;
        Text result = new Text();
        boolean completed = false;
        int count = 0;
        while (!completed && (i = jsonStreamReader.read()) != -1) {
            char ch = (char) i;
            if (ch == '{') {
                // assert first character is open bracket for this test
                assertEquals(0, count);
                parser.startNewJsonObject();
            }
            else {
                completed = parser.buildNextObjectContainingMember(ch, result);
            }
            count++;
        }

        assertTrue(completed);
        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(105, result.getLength());
        assertEquals("{\"name\": \"äää\", \"year\": \"2022\", \"cüstömerstätüs\":\"välid\",\"address\": \"söme city\", \"zip\": \"95051\"}", result.toString());

        jsonStreamReader.close();
    }

    @Test
    public void testSimpleNoMatchingIdentifier() throws URISyntaxException, IOException {

        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/simple.json").toURI());

        InputStreamReader jsonStreamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);

        PartitionedJsonParser parser = new PartitionedJsonParser("customer status");

        int i;
        Text result = new Text();
        boolean completed = false;
        int count = 0;
        while (!completed && (i = jsonStreamReader.read()) != -1) {
            char ch = (char) i;
            if (ch == '{') {
                // assert first character is open bracket for this test
                assertEquals(0, count);
                parser.startNewJsonObject();
            }
            else {
                completed = parser.buildNextObjectContainingMember(ch, result);
            }
            count++;
        }

        assertTrue(completed);
        assertFalse(parser.foundObjectWithIdentifier());
        // result should be empty
        assertEquals(0, result.getLength());

        jsonStreamReader.close();
    }

    @Test
    public void testEmptyJson() throws URISyntaxException, IOException {

        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/empty_input.json").toURI());

        InputStreamReader jsonStreamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);

        PartitionedJsonParser parser = new PartitionedJsonParser("name");

        int i;
        Text result = new Text();
        boolean completed = false;
        int count = 0;
        while (!completed && (i = jsonStreamReader.read()) != -1) {
            char ch = (char) i;
            if (ch == '{') {
                // assert first character is open bracket for this test
                assertEquals(0, count);
                parser.startNewJsonObject();
            }
            else {
                completed = parser.buildNextObjectContainingMember(ch, result);
            }
            count++;
        }

        assertTrue(completed);
        assertFalse(parser.foundObjectWithIdentifier());
        assertEquals(0, result.getLength());

        jsonStreamReader.close();
    }


    @Test
    public void testNestedMatchingIdentifier() throws URISyntaxException, IOException {

        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/complex_input.json").toURI());

        InputStreamReader jsonStreamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);

        PartitionedJsonParser parser = new PartitionedJsonParser("year");

        // read the first json object
        int i;
        Text result = new Text();
        boolean completed = false;
        int count = 0;
        while (!completed && (i = jsonStreamReader.read()) != -1) {
            char ch = (char) i;
            if (ch == '{' & count < 10) {
                // assert first character is open bracket for this test
                // `[\n  {` makes it the 5th element (index starts at 0, so value here should be 5)
                // we only care about the first bracket, the parser should handle the rest which is what we want to check
                assertEquals(4, count);
                parser.startNewJsonObject();
            }
            else {
                completed = parser.buildNextObjectContainingMember(ch, result);
            }
            count++;
        }

        // should have read the following 47 bytes
        // [\n
        //   {\n
        //     "name": "äää",\n
        //     "customerdata": [\n
        // before finding the object with the identifier
        assertEquals(176, count);
        assertTrue(completed);
        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(129, result.getLength());
        // should only be the inner object containing the identifier with the same spacing and newlines
        assertEquals("{\n" +
                "        \"cüstömerstätüs\": \"välid\",\n" +
                "        \"year\": \"2022\",\n" +
                "        \"address\": \"söme city\",\n" +
                "        \"zip\": \"95051\"\n" +
                "      }", result.toString());

        // continue reading the stream for the second json object
        completed = false;
        count = 0;
        while (!completed && (i = jsonStreamReader.read()) != -1) {
            char ch = (char) i;
            if (ch == '{' & count < 5) {
                // assert first character is open bracket for this test
                // `[\n  {` makes it the 5th element (index starts at 0, so value here should be 5)
                // we only care about the first bracket, the parser should handle the rest which is what we want to check
                assertEquals(2, count);
                parser.startNewJsonObject();
            }
            else {
                completed = parser.buildNextObjectContainingMember(ch, result);
            }
            count++;
        }

        assertTrue(completed);
        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(139, result.getLength());
        // should only be the entire second object containing the identifier with the same spacing and newlines
        assertEquals("  {\n" +
                "    \"cüstömerstätüs\": \"välid\",\n" +
                "    \"name\": \"你好\",\n" +
                "    \"year\": \"2033\",\n" +
                "    \"address\": \"0\uD804\uDC13a\",\n" +
                "    \"zip\": \"19348\"\n" +
                "  }", result.toString());

        jsonStreamReader.close();
    }
}