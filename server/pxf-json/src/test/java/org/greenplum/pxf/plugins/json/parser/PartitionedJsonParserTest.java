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


import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
public class PartitionedJsonParserTest {

    @Test
    public void testOffset() throws IOException, URISyntaxException {
        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json").toURI());

        InputStream jsonInputStream = new FileInputStream(file);

        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);
        String result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNotNull(result);
        // The total number of bytes read here are 107 = 2 bytes for "[" & "\n"
        // and 105 bytes for the first record i.e: {"cüsötmerstätüs":"välid","name": "äää", "year": "2022", "address": "söme city", "zip": "95051"}
        assertEquals(107, parser.getBytesRead());
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"äää\", \"year\": \"2022\", \"address\": \"söme city\", \"zip\": \"95051\"}", result);
        assertEquals(105, result.getBytes(StandardCharsets.UTF_8).length);
        assertEquals(2, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);

        result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNotNull(result);

        // The total number of bytes read here are
        // 216 = 107 bytes from the earlier record + 2 bytes for "," and "\n" and 107 bytes from current record
        assertEquals(216, parser.getBytesRead());
        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"yī\", \"year\": \"2020\", \"address\": \"anöther city\", \"zip\": \"12345\"}", result);
        assertEquals(107,  result.getBytes(StandardCharsets.UTF_8).length);
        assertEquals(109, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);

        jsonInputStream.close();
    }

    @Test
    public void testSimple() throws URISyntaxException, IOException {

        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/simple.json").toURI());

        InputStream jsonInputStream = new FileInputStream(file);

        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);
        String result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNotNull(result);
        assertEquals(105, parser.getBytesRead());
        assertEquals("{\"name\": \"äää\", \"year\": \"2022\", \"cüstömerstätüs\":\"välid\",\"address\": \"söme city\", \"zip\": \"95051\"}", result);

        result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNull(result);
        assertEquals(105, parser.getBytesRead());

        jsonInputStream.close();
    }
}