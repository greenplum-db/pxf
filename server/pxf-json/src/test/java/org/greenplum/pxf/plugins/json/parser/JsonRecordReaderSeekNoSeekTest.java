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

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.json.JsonRecordReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class JsonRecordReaderSeekNoSeekTest {

    private static final Log LOG = LogFactory.getLog(JsonRecordReaderSeekNoSeekTest.class);
    private static final String RECORD_MEMBER_IDENTIFIER = "json.input.format.record.identifier";
    private JobConf jobConf;
    private String[] hosts = null;

    @BeforeEach
    public void setup() {
        RequestContext context = new RequestContext();
        context.setConfiguration(new Configuration());

        jobConf = new JobConf(context.getConfiguration());
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "name");
    }

    @Test
    public void testSeek() throws IOException {
        File testsDir = new File("src/test/resources/parser-tests/seek");
        File[] dirs = testsDir.listFiles();


        for (File jsonDir : dirs) {
            File childFile = new File(jsonDir, "input.json");

            File[] jsonObjectFiles = jsonDir.listFiles((file, s) ->
                    s.contains("expected"));
            runTest(childFile, jsonObjectFiles, true);
        }
    }

    @Test
    public void testNoSeek() throws IOException {
        File testsDir = new File("src/test/resources/parser-tests/noseek");
        File[] dirs = testsDir.listFiles();

        for (File jsonFile : dirs) {

            File[] jsonObjectFiles = jsonFile.getParentFile().listFiles((file, s) ->
                    s.contains(jsonFile.getName()) && s.contains("expected"));
            runTest(jsonFile, jsonObjectFiles, false);
        }
    }

    public void runTest(final File jsonFile, File[] jsonObjectFiles, boolean seek) throws IOException {

        int start = 0;
        if (seek) {
            try (InputStream jsonInputStream = new FileInputStream(jsonFile)) {
                start = seekToStart(jsonInputStream);
            }

        }

        Path path = new Path(jsonFile.getPath());
        // for these tests, each file should be its own split so let the length be the size of the file
        FileSplit fileSplit = new FileSplit(path, start, jsonFile.length(), hosts);
        JsonRecordReader jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        LongWritable key = new LongWritable();
        Text data = new Text();

        Arrays.sort(jsonObjectFiles);
        if (jsonObjectFiles.length == 0) {
            jsonRecordReader.next(key, data);
            assertEquals(0, data.getLength(), "File " + jsonFile.getAbsolutePath() + " got result '" + data.toString() + "'");
            LOG.info("File " + jsonFile.getAbsolutePath() + " passed");
        } else {
            for (File jsonObjectFile : jsonObjectFiles) {
                String expected = normalizeWhitespaces(FileUtils.readFileToString(jsonObjectFile, Charset.defaultCharset()));
                jsonRecordReader.next(key, data);
                assertNotEquals(0, data.getLength(), jsonFile.getAbsolutePath() + "/" + jsonObjectFile.getName());
                assertEquals(expected, normalizeWhitespaces(data.toString()), jsonFile.getAbsolutePath() + "/" + jsonObjectFile.getName());
                LOG.info("File " + jsonFile.getAbsolutePath() + "/" + jsonObjectFile.getName() + " passed");
            }
        }
    }

    public int seekToStart(InputStream jsonInputStream) throws IOException {
        int count = 0;
        // pop off characters until we see <SEEK>
        StringBuilder sb = new StringBuilder();
        int i;
        while ((i = jsonInputStream.read()) != -1) {
            count++;
            sb.append((char) i);

            if (sb.toString().endsWith("<SEEK>")) {
                return count;
            }
        }
        fail();
        return 0;
    }

    public String normalizeWhitespaces(String s) {
        return s.replaceAll("[\\n\\t\\r \\t]+", " ").trim();
    }
}
