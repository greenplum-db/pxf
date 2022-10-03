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
            completed = parser.buildNextObjectContainingMember((char) i, result);
        }

        assertTrue(parser.foundObjectWithIdentifier());
        assertEquals(0, result.getLength());
    }

    @Test
    public void testSimple() throws URISyntaxException, IOException {

        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/simple.json").toURI());

        InputStreamReader jsonStreamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);

        PartitionedJsonParser parser = new PartitionedJsonParser("name");

        int i;
        Text result = new Text();
        boolean completed = false;
        while ((i = jsonStreamReader.read()) != -1 && !completed) {
            char ch = (char) i;
            if (ch == '{') {
                parser.startNewJsonObject();
            }
            else {
                completed = parser.buildNextObjectContainingMember((char) i, result);
            }
        }

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
        while ((i = jsonStreamReader.read()) != -1 && !completed) {
            char ch = (char) i;
            if (ch == '{') {
                parser.startNewJsonObject();
            }
            else {
                completed = parser.buildNextObjectContainingMember((char) i, result);
            }
        }

        assertFalse(parser.foundObjectWithIdentifier());
        assertEquals(0, result.getLength());

        jsonStreamReader.close();
    }
}