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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionedJsonParserTest {

    @Test
    public void testFoundIdentifier() {

        PartitionedJsonParser parser = new PartitionedJsonParser("name");
        // start the json object, handles starting bracket
        parser.startNewJsonObject();

        boolean completed = false;
        String jsonContents = "\"name\"";
        for (int i = 0; i < jsonContents.length(); i++) {
            char ch = jsonContents.charAt(i);
            completed = parser.buildNextObjectContainingMember(ch);
        }

        String result = parser.getCompletedObject();
        assertFalse(completed);
        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(0, result.getBytes().length);
    }

    @Test
    public void testSimpleMatchingIdentifier() {

        PartitionedJsonParser parser = new PartitionedJsonParser("name");
        // start the json object, handles starting bracket
        parser.startNewJsonObject();

        boolean completed = false;
        // if a json object has been started, give this input
        String jsonContents = "\"name\": \"äää\", \"year\": \"2022\", \"cüstömerstätüs\":\"välid\",\"address\": \"söme city\", \"zip\": \"95051\"}";

        for (int i = 0; i < jsonContents.length(); i++) {
            char ch = jsonContents.charAt(i);
            completed = parser.buildNextObjectContainingMember(ch);
        }

        String result = parser.getCompletedObject();
        assertTrue(completed);
        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(105, result.getBytes().length);
        assertEquals("{\"name\": \"äää\", \"year\": \"2022\", \"cüstömerstätüs\":\"välid\",\"address\": \"söme city\", \"zip\": \"95051\"}", result);
    }

    @Test
    public void testSimpleMatchingIdentifierExtraCarriageReturns() {

        PartitionedJsonParser parser = new PartitionedJsonParser("name");
        // start the json object, handles starting bracket
        parser.startNewJsonObject();

        boolean completed = false;
        // if a json object has been started, give this input
        String jsonContents = "\"name\": \"äää\"\r\r\n," +
                "\"year\": \"2022\",\r\r\n" +
                "\"cüstömerstätüs\":\"välid\",\r\r\n" +
                "\"address\": \"söme city\",\r\r\n" +
                "\"zip\": \"95051\"\r\r\n" +
                "}";

        for (int i = 0; i < jsonContents.length(); i++) {
            char ch = jsonContents.charAt(i);
            completed = parser.buildNextObjectContainingMember(ch);
        }

        String result = parser.getCompletedObject();
        assertTrue(completed);
        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(117, result.getBytes().length);
        assertEquals("{\"name\": \"äää\"\r\r\n," +
                "\"year\": \"2022\",\r\r\n" +
                "\"cüstömerstätüs\":\"välid\",\r\r\n" +
                "\"address\": \"söme city\",\r\r\n" +
                "\"zip\": \"95051\"\r\r\n" +
                "}", result);
    }

    @Test
    public void testSimpleMatchingIdentifierMixedCarriageReturns() {

        PartitionedJsonParser parser = new PartitionedJsonParser("name");
        // start the json object, handles starting bracket
        parser.startNewJsonObject();

        boolean completed = false;
        // if a json object has been started, give this input
        String jsonContents = "\"name\": \"äää\"\n," +
                "\"year\": \"2022\",\r\r" +
                "\"cüstömerstätüs\":\"välid\",\r\n" +
                "\"address\": \"söme city\",\r\r\n" +
                "\"zip\": \"95051\"" +
                "}";

        for (int i = 0; i < jsonContents.length(); i++) {
            char ch = jsonContents.charAt(i);
            completed = parser.buildNextObjectContainingMember(ch);
        }

        String result = parser.getCompletedObject();
        assertTrue(completed);
        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(110, result.getBytes().length);
        assertEquals("{\"name\": \"äää\"\n," +
                "\"year\": \"2022\",\r\r" +
                "\"cüstömerstätüs\":\"välid\",\r\n" +
                "\"address\": \"söme city\",\r\r\n" +
                "\"zip\": \"95051\"" +
                "}", result);
    }

    @Test
    public void testSimpleNoMatchingIdentifier()  {

        PartitionedJsonParser parser = new PartitionedJsonParser("customer status");
        // start the json object, handles starting bracket
        parser.startNewJsonObject();

        boolean completed = false;
        // if a json object has been started, give this input
        String jsonContents = "\"name\": \"äää\", \"year\": \"2022\", \"cüstömerstätüs\":\"välid\",\"address\": \"söme city\", \"zip\": \"95051\"}";

        for (int i = 0; i < jsonContents.length(); i++) {
            char ch = jsonContents.charAt(i);
            completed = parser.buildNextObjectContainingMember(ch);
        }

        String result = parser.getCompletedObject();
        assertTrue(completed);
        assertFalse(parser.foundObjectWithIdentifier());
        // result should be empty
        assertEquals(0, result.getBytes().length);
    }

    @Test
    public void testEmptyJson() {

        PartitionedJsonParser parser = new PartitionedJsonParser("name");
        // start the json object, handles starting bracket
        parser.startNewJsonObject();

        boolean completed = false;
        // if a json object has been started, give this input
        String jsonContents = "}";

        for (int i = 0; i < jsonContents.length(); i++) {
            char ch = jsonContents.charAt(i);
            completed = parser.buildNextObjectContainingMember(ch);
        }

        String result = parser.getCompletedObject();
        assertTrue(completed);
        assertFalse(parser.foundObjectWithIdentifier());
        assertEquals(0, result.getBytes().length);
    }

    @Test
    public void testNestedMatchingIdentifier() {

        PartitionedJsonParser parser = new PartitionedJsonParser("year");
        // start the json object, handles starting bracket
        parser.startNewJsonObject();

        int count = 0;
        boolean completed = false;
        // if a json object has been started, give this input
        String jsonContents =
                "  \"name\": \"äää\",\n" +
                "  \"customerdata\":\n" +
                "  [\n" +
                "    {\n" +
                "      \"cüstömerstätüs\": \"välid\",\n" +
                "      \"year\": \"2022\",\n" +
                "      \"address\": \"söme city\",\n" +
                "      \"zip\": \"95051\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        for (int i = 0; i < jsonContents.length(); i++) {
            if (completed) {
                // if the object is completed, then let's break and finish
                break;
            }
            char ch = jsonContents.charAt(i);
            completed = parser.buildNextObjectContainingMember(ch);
            count++;
        }

        String result = parser.getCompletedObject();
        // should have read the following 41 bytes
        // `  "name": "äää",\n`
        // `  "customerdata":\n`
        // `  [\n`
        // before finding the object with the identifier
        assertEquals(156, count);
        // theres should be some remaining chars that we did not read because we found the end of the object
        // `\n` from after the curly bracket
        // `  ]\n`
        // `}`
        assertEquals(6, jsonContents.length() - count);
        assertTrue(completed);
        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(119, result.getBytes().length);
        // should only be the inner object containing the identifier with the same spacing and newlines
        assertEquals("{\n" +
                "      \"cüstömerstätüs\": \"välid\",\n" +
                "      \"year\": \"2022\",\n" +
                "      \"address\": \"söme city\",\n" +
                "      \"zip\": \"95051\"\n" +
                "    }", result);
    }

    @Test
    public void testStringNotMemberString() {

        PartitionedJsonParser parser = new PartitionedJsonParser("year");
        // start the json object, handles starting bracket
        parser.startNewJsonObject();

        boolean completed = false;
        // if a json object has been started, give this input
        String jsonContents = "\"notes\": \"the year we lived\"}";

        for (int i = 0; i < jsonContents.length(); i++) {
            char ch = jsonContents.charAt(i);
            completed = parser.buildNextObjectContainingMember(ch);
        }

        String result = parser.getCompletedObject();
        assertTrue(completed);
        assertFalse(parser.foundObjectWithIdentifier());
        // identifier not found
        assertEquals(0, result.getBytes().length);
    }
}