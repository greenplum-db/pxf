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


import org.apache.hadoop.mapred.LineRecordReader;
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

        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, new LineRecordReader(jsonInputStream, 0,file.length(), 10000));
        String result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNotNull(result);
        // The total number of bytes read here are 106 = 1 bytes for "["
        // and 105 bytes for the first record i.e: {"cüsötmerstätüs":"välid","name": "äää", "year": "2022", "address": "söme city", "zip": "95051"}
        assertEquals(106, parser.getBytesRead());
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"äää\", \"year\": \"2022\", \"address\": \"söme city\", \"zip\": \"95051\"}", result);
        assertEquals(105, result.getBytes(StandardCharsets.UTF_8).length);
        assertEquals(1, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);

        result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNotNull(result);

        // The total number of bytes read here are
        // 214 = 106 bytes from the earlier record + 1 byte for "," and 107 bytes from current record
        assertEquals(214, parser.getBytesRead());
        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"yī\", \"year\": \"2020\", \"address\": \"anöther city\", \"zip\": \"12345\"}", result);
        assertEquals(107,  result.getBytes(StandardCharsets.UTF_8).length);
        assertEquals(107, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);

        jsonInputStream.close();
    }

    @Test
    public void testSimple() throws URISyntaxException, IOException {

        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/simple.json").toURI());

        InputStream jsonInputStream = new FileInputStream(file);

        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, new LineRecordReader(jsonInputStream, 0,file.length(), 10000));
        String result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNotNull(result);
        assertEquals(105, parser.getBytesRead());
        assertEquals("{\"name\": \"äää\", \"year\": \"2022\", \"cüstömerstätüs\":\"välid\",\"address\": \"söme city\", \"zip\": \"95051\"}", result);

        result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNull(result);
        assertEquals(105, parser.getBytesRead());

        jsonInputStream.close();
    }
//
    @Test
    public void testMidObjectSimple() throws URISyntaxException, IOException {

        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/midobject/simple.json").toURI());

        InputStream jsonInputStream = new FileInputStream(file);

        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, new LineRecordReader(jsonInputStream, 0,file.length(), 10000));
        String result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNotNull(result);
        // The total number of bytes read here are 162
        // 2 bytes for "[" & "\n"
        // + 55 bytes for the partial record i.e: ar": "2022", "address": "söme city", "zip": "95051"},
        // + 105 bytes for the first record i.e: {"cüstömerstätüs":"invälid","name": "yī", "year": "2020", "address": "anöther city", "zip": "12345"}
        assertEquals(162, parser.getBytesRead());
        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"yī\", \"year\": \"2020\", \"address\": \"anöther city\", \"zip\": \"12345\"}", result);
        assertEquals(107, result.getBytes(StandardCharsets.UTF_8).length);
        assertEquals(55, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);

        result = parser.nextObjectContainingMember("cüstömerstätüs");
        assertNotNull(result);

        // The total number of bytes read here are
        // 268 = 162 bytes from the earlier record + 2 bytes for "," and "\n" and 104 bytes from current record
        assertEquals(268, parser.getBytesRead());
        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"₡¥\", \"year\": \"2022\", \"address\": \"\uD804\uDC13exas\", \"zip\": \"12345\"}", result);
        assertEquals(104,  result.getBytes(StandardCharsets.UTF_8).length);
        assertEquals(164, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);

        jsonInputStream.close();
    }
//
//    @Test
//    public void testMidObjectWithCurlyBrackets() throws URISyntaxException, IOException {
//
//        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/midobject/withcurlybrackets.json").toURI());
//
//        InputStream jsonInputStream = new FileInputStream(file);
//
//        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, new LineRecordReader(jsonInputStream, 0,file.length(), 10000));
//        String result = parser.nextObjectContainingMember("cüstömerstätüs");
//        assertNotNull(result);
//        // The total number of bytes read here are 166
//        // 2 bytes for "[" & "\n"
//        // + 59 bytes for the partial record i.e: ar": "2022", "address": "söme { } city", "zip": "95051"},
//        // + 105 bytes for the first record i.e: {"cüstömerstätüs":"invälid","name": "yī", "year": "2020", "address": "anöther city", "zip": "12345"}
//        assertEquals(166, parser.getBytesRead());
//        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"yī\", \"year\": \"2020\", \"address\": \"anöther city\", \"zip\": \"12345\"}", result);
//        assertEquals(107, result.getBytes(StandardCharsets.UTF_8).length);
//        assertEquals(59, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);
//
//        result = parser.nextObjectContainingMember("cüstömerstätüs");
//        assertNotNull(result);
//
//        // The total number of bytes read here are
//        // 272 = 168 bytes from the earlier record + 2 bytes for "," and "\n" and 104 bytes from current record
//        assertEquals(272, parser.getBytesRead());
//        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"₡¥\", \"year\": \"2022\", \"address\": \"\uD804\uDC13exas\", \"zip\": \"12345\"}", result);
//        assertEquals(104,  result.getBytes(StandardCharsets.UTF_8).length);
//        assertEquals(168, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);
//
//        jsonInputStream.close();
//    }
//
//    @Test
//    public void testMidObjectWithOpeningBracket() throws URISyntaxException, IOException {
//
//        File file = new File(this.getClass().getClassLoader().getResource("parser-tests/midobject/withopenbracket.json").toURI());
//
//        InputStream jsonInputStream = new FileInputStream(file);
//
//        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, new LineRecordReader(jsonInputStream, 0,file.length(), 10000));
//        String result = parser.nextObjectContainingMember("cüstömerstätüs");
//        assertNotNull(result);
//        // The total number of bytes read here are 134
//        // 2 bytes for "[" & "\n"
//        // + 27 bytes for the partial record i.e:  { city", "zip": "95051"},
//        // + 105 bytes for the first record i.e: {"cüstömerstätüs":"invälid","name": "yī", "year": "2020", "address": "anöther city", "zip": "12345"}
//        assertEquals(134, parser.getBytesRead());
//        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"yī\", \"year\": \"2020\", \"address\": \"anöther city\", \"zip\": \"12345\"}", result);
//        assertEquals(107, result.getBytes(StandardCharsets.UTF_8).length);
//        assertEquals(27, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);
//
//        result = parser.nextObjectContainingMember("cüstömerstätüs");
//        assertNotNull(result);
//
//        // The total number of bytes read here are
//        // 239 = 133 bytes from the earlier record + 2 bytes for "," and "\n" and 104 bytes from current record
//        assertEquals(240, parser.getBytesRead());
//        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"₡¥\", \"year\": \"2022\", \"address\": \"\uD804\uDC13exas\", \"zip\": \"12345\"}", result);
//        assertEquals(104,  result.getBytes(StandardCharsets.UTF_8).length);
//        assertEquals(136, parser.getBytesRead() - result.getBytes(StandardCharsets.UTF_8).length);
//
//        jsonInputStream.close();
//    }
}