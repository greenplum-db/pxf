package org.greenplum.pxf.plugins.json;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.greenplum.pxf.plugins.json.parser.PartitionedJsonParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Multi-line json object reader. JsonRecordReader uses a member name (set by the <b>IDENTIFIER</b> PXF parameter) to
 * determine the encapsulating object to extract and read.
 * <p>
 * JsonRecordReader supports compressed input files as well.
 * <p>
 * As a safe guard set the optional <b>MAXLENGTH</b> parameter to limit the max size of a record.
 */
public class JsonRecordReader implements RecordReader<LongWritable, Text> {

    public static final String RECORD_MEMBER_IDENTIFIER = "json.input.format.record.identifier";
    public static final String RECORD_MAX_LENGTH = "multilinejsonrecordreader.maxlength";
    private static final Log LOG = LogFactory.getLog(JsonRecordReader.class);
    private final String jsonMemberName;
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private int maxObjectLength;
    private InputStream is;
    private PartitionedJsonParser parser;
    private LineRecordReader lineRecordReader;
    private StringBuffer currentLineBuffer;
    private JobConf conf;
    private final Path file;
    private int currentBufferIndex;

    private static final char BACKSLASH = '\\';
    private static final char QUOTE = '\"';
    private static final char START_BRACE = '{';
    private static final int EOF = -1;
    private static final int END_OF_SPLIT = -2;

    /**
     * Create new multi-line json object reader.
     *
     * @param conf  Hadoop context
     * @param split HDFS split to start the reading from
     * @throws IOException IOException when reading the file
     */
    public JsonRecordReader(JobConf conf, FileSplit split) throws IOException {

        this.jsonMemberName = conf.get(RECORD_MEMBER_IDENTIFIER);
        this.maxObjectLength = conf.getInt(RECORD_MAX_LENGTH, Integer.MAX_VALUE);

        start = split.getStart();
        end = start + split.getLength();
        file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // openForWrite the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(split.getPath());
        if (codec != null) {
            is = codec.createInputStream(fileIn);
            start = 0;
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                fileIn.seek(start);
            }
            is = fileIn;
        }
        this.conf = conf;
        lineRecordReader =  new LineRecordReader(conf, split);
        parser = new PartitionedJsonParser(jsonMemberName);
        this.pos = start;
    }

    /*
     * {@inheritDoc}
     */
    @Override
    public boolean next(LongWritable key, Text value) throws IOException {

        while (pos < end) {
            int i;
            // object to pass in for streaming
            Text jsonObject = new Text();
            boolean completedObject = false;
            // scan to first start brace object.
            boolean foundBeginObject = scanToFirstBeginObject();
            // found an object, so we will either return a completed object or be mid-object
            if (foundBeginObject) {
                // found a start brace so begin a new json object
                parser.startNewJsonObject();

                // read through the file until the object is completed
                while ((i = readNextChar()) > EOF && !completedObject) {
                    char c = (char) i;
                    completedObject = parser.buildNextObjectContainingMember(c, jsonObject);

                    if (completedObject) {
                        // we have a completed object
                        String json = jsonObject.toString();
                        pos = start + parser.getBytesRead();

                        long jsonStart = pos - json.getBytes(StandardCharsets.UTF_8).length;

                        // if the "begin-object" position is after the end of our split, we should ignore it
                        if (jsonStart >= end) {
                            return false;
                        }

                        if (json.length() > maxObjectLength) {
                            LOG.warn("Skipped JSON object of size " + json.length() + " at pos " + jsonStart);
                        } else {
                            key.set(jsonStart);
                            value.set(json);
                            return true;
                        }
                        return true;
                    } else if (!completedObject && pos >= end) {
                        // we have items in the list but the last one is incomplete so we pull in the next split
                        // and continue reading until end of object
                        getNextSplit();
                        // continue the while loop to complete the object
                        continue;
                    } else {
                        // if we don't have a completed item and we aren't at the end of the split
                        // we should just continue to read
                        continue;
                    }
                }
            }
            // begin object never found
            // don't move onto next split here
            return false;
        }

        return false;
    }

    /*
     * {@inheritDoc}
     */
    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    /*
     * {@inheritDoc}
     */
    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    /*
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() throws IOException {
        if (is != null) {
            is.close();
        }
    }

    /*
     * {@inheritDoc}
     */
    @Override
    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    private int readNextChar() throws IOException {
        Text currentLine = new Text();
        boolean getNext = true;
        // if we are at the end of the buffer, refresh
        if (currentLineBuffer == null || currentBufferIndex >= currentLineBuffer.length()) {
            getNext = lineRecordReader.next(lineRecordReader.createKey(), currentLine);
            currentLineBuffer = new StringBuffer(currentLine.toString());
            currentBufferIndex = 0;
        }
        if (!getNext || currentLine == null) {
            return END_OF_SPLIT;
        }
        // its possible for the json object to have an empty line
        if (currentLineBuffer.length() > 0) {
            char c = currentLineBuffer.charAt(currentBufferIndex);
            currentBufferIndex++;
            parser.trackUncountedCharsReadFromStream(c);

            return c;
        } else {
            return readNextChar();
        }
    }

    private boolean scanToFirstBeginObject() throws IOException {
        // seek until we hit the first begin-object
        boolean inString = false;
        int i;
        while ((i = readNextChar()) > EOF) {
            char c = (char) i;
            // if the current value is a backslash, then ignore the next value as it's an escaped char
            if (c == BACKSLASH) {
                readNextChar();
                break;
            } else if (c == QUOTE) {
                inString = !inString;
            } else if (c == START_BRACE && !inString) {
                return true;
            }
        }
        return false;
    }

    private void getNextSplit() throws IOException {
        // we need to move into the next split, so create one that starts at the current pos
        // and goes until the end of the file
        FileSplit nextSplit = new FileSplit(file, pos, Long.MAX_VALUE, (String[]) null);
        lineRecordReader = new LineRecordReader(conf, nextSplit);
    }
}
