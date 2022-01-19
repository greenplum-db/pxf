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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;
import org.greenplum.pxf.plugins.json.JsonRecordReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PartitionedJsonParserOffsetTest {

    private static final String RECORD_MEMBER_IDENTIFIER = "json.input.format.record.identifier";
    private File file;
    private JobConf jobConf;
    private FileSplit fileSplit;
    private LongWritable key;
    private Text data;
    private RequestContext context;
    private Path path;
    private String[] hosts = null;
    private JsonRecordReader jsonRecordReader;

    @Test
    public void testOffset() throws IOException {

        InputStream jsonInputStream = new FileInputStream(file);

        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);
        String result = parser.nextObjectContainingMember("cüsötmerstätüs");
        assertNotNull(result);
        assertEquals(107, parser.getBytesRead());
        assertEquals(11, parser.getBytesRead() - result.length());

        result = parser.nextObjectContainingMember("cüsötmerstätüs");
        assertNotNull(result);
        assertEquals(216, parser.getBytesRead());
        assertEquals(116, parser.getBytesRead() - result.length());
        jsonInputStream.close();
    }

    @Test
    /**
     *  Here the record overlaps between Split-1 and Split-2.
     *  reader will start reading the first record in the
     *  middle of the split. It won't find the start { of that record but will
     *  read till the end. It will successfully return the second record from the Split-2
     */
    public void testInBetweenSplits() throws IOException {

        long start = 32;
        fileSplit = new FileSplit(path, start, 100l, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        jsonRecordReader.next(key, data);
        assertEquals(107, data.toString().getBytes(StandardCharsets.UTF_8).length);

        // since the FileSplit starts at 32, which is the middle of the first record.
        // so the reader reads the first record till it finds the end } and then starts reading the next record in the split
        // it discards the previous read data but keeps track of the bytes read.

        assertEquals(184, jsonRecordReader.getPos() - start);
    }

    @Test
    /**
     * The split size is only 50 bytes. The reader is expected to read the
     * full 1 record here.
     */
    public void testSplitIsSmallerThanRecord() throws IOException {

        long start = 0;
        fileSplit = new FileSplit(path, start, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        jsonRecordReader.next(key, data);
        // reads the full 1 record here
        assertEquals(105, data.toString().getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    /**
     * The Split size is large and the after reading all the records
     * the reader should only return the last read record
     */
    public void testRecordSizeSmallerThanSplit() throws IOException {

        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        jsonRecordReader.next(key, data);
        jsonRecordReader.next(key, data);
        assertNotNull(data);
        Text data1 = data;
        // should not return any new data
        jsonRecordReader.next(key, data);

        assertEquals(data, data1);
    }

    @Test
    public InputStream createFromString(String s) {
        return new ByteArrayInputStream(s.getBytes());
    }

    @BeforeEach
    public void setup() throws IOException, URISyntaxException {
        context = new RequestContext();
        context.setConfiguration(new Configuration());

        jobConf = new JobConf(context.getConfiguration(), PartitionedJsonParserNoSeekTest.class);
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "cüsötmerstätüs");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json").toURI());
        context.setDataSource(file.getPath());
        path = new Path(file.getPath());

        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
    }

    public LongWritable createKey() {
        return new LongWritable();
    }


    public Text createValue() {
        return new Text();
    }
}
