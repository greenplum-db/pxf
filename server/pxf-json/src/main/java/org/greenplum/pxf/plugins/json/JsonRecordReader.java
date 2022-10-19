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
    private String[] hosts;
    private int maxObjectLength;
    private PartitionedJsonParser parser;
    private LineRecordReader lineRecordReader;
    // position of the underlying lineRecordReader
    private long linePos;
    // line that was read in by the line record reader
    private Text currentLine;
    private JobConf conf;
    private final Path file;
    // this is the current line in buffer form
    private StringBuffer currentLineBuffer;
    // index where the JsonRecordReader has read to in the currentLineBuffer
    private int currentLineIndex = Integer.MAX_VALUE;
    private boolean inNextSplit = false;

    private static final char BACKSLASH = '\\';
    private static final char QUOTE = '\"';
    private static final char START_BRACE = '{';
    private static final int EOF = -1;
    private static final int END_OF_SPLIT = -2;
    private final byte[] NEW_LINE = "\n".getBytes(StandardCharsets.UTF_8);
    private final byte[] CARRIAGERETURN_NEWLINE = "\r\n".getBytes(StandardCharsets.UTF_8);

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
        hosts = split.getLocations();
        compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // openForWrite the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(split.getPath());
        if (codec != null) {
            start = 0;
            end = Long.MAX_VALUE;
            FileSplit codecSplit = new FileSplit(file, start, Long.MAX_VALUE, hosts);
            lineRecordReader =  new LineRecordReader(conf, codecSplit);
        } else {
            lineRecordReader =  new LineRecordReader(conf, split);
        }
        this.conf = conf;
        parser = new PartitionedJsonParser(jsonMemberName);
        currentLine = new Text();
        this.pos = start;
        this.linePos = start;
    }

    /*
     * {@inheritDoc}
     */
    @Override
    public boolean next(LongWritable key, Text value) throws IOException {

        while (!inNextSplit) { // split level. if out of split, then return false
            // scan to first start brace object.
            boolean foundBeginObject = scanToNextJsonBeginObject();
            if (!foundBeginObject) {
                // if we've scanned the entire file and didn't find anything,
                // then the position of the JsonRecordReader should match the lineRecordReader
                pos = linePos;
                return false;
            }

            // found a start brace so begin a new json object
            parser.startNewJsonObject();

            // read through the file until the object is completed
            int i;
            boolean isObjectComplete = false;
            // object to pass in for streaming
            Text jsonObject = new Text();
            while (!isObjectComplete && (i = readNextChar()) != EOF) {
                // if the currentLineBuffer is null, nothing has been read yet, so we need to read the next line
                // if the currentLineIndex is greater than the length,  we are at the end of the buffer, read the next line
                // however, if we are at the end of the split, then we need to get the next split before we can read the line
                if (i == END_OF_SPLIT && (currentLineBuffer == null || currentLineIndex >= currentLineBuffer.length())) {
                    LOG.debug("JSON object incomplete, moving onto next split to finish");
                    getNextSplit();
                    // continue the while loop to complete the object
                    continue;
                }

                char c = (char) i;
                isObjectComplete = parser.buildNextObjectContainingMember(c, jsonObject);
            }

            // we've completed an object but there might still be things in the buffer. Calculate the proper
            // position of the JsonRecordReader
            long unreadCharsInBuffer = currentLineBuffer.length() - currentLineIndex;
            pos = linePos - unreadCharsInBuffer;

            if (isObjectComplete && parser.foundObjectWithIdentifier()) {
                String json = jsonObject.toString();
                if (json.length() > maxObjectLength) {
                    LOG.warn("Skipped JSON object of size " + json.length() + " at pos " + pos);
                } else {
                    // the key is set to beginning of the json object
                    key.set(pos - json.length());
                    value.set(json);
                    return true;
                }
            }
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
        if (lineRecordReader != null) {
            lineRecordReader.close();
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

    /**
     * Reads the next character in the buffer. It will pull the next line as necessary
     * @return the int value of a character
     * @throws IOException
     */
    private int readNextChar() throws IOException {
        // if the currentLineBuffer is null, nothing has been read yet, so we need to read the next line
        // if the currentLineIndex is greater than the length,  we are at the end of the buffer, read the next line
        if (currentLineBuffer == null || currentLineIndex >= currentLineBuffer.length()) {
            if (!getNextLine()) {
                return END_OF_SPLIT;
            }
        }

        int c = currentLineBuffer.charAt(currentLineIndex);
        currentLineIndex++;

        return c;

    }

    /**
     * Read through the characters until we hit starting bracket that indicates the start of a JSON object
     * @return true when an open bracket '{' is found, false otherwise
     * @throws IOException
     */
    private boolean scanToNextJsonBeginObject() throws IOException {
        // assumes each line is a valid json line
        // seek until we hit the first begin-object
        boolean inString = false;
        int i;
        // since we have not yet found a starting object, exit if either EOF (-1) or END_OF_SPLIT (-2)
        while ((i = readNextChar()) > EOF) {
            char c = (char) i;
            // if the current value is a backslash, then ignore the next value as it's an escaped char
            if (c == BACKSLASH) {
                readNextChar();
            } else if (c == QUOTE) {
                inString = !inString;
            } else if (c == START_BRACE && !inString) {
                return true;
            }
        }
        return false;
    }

    /**
     * This function allows JsonRecordReader to go into the next split to finish a JSON object.
     *   Closes the current LineRecordReader and opens a new one that starts at the end of the current split
     *  The end of the new split is set to Long.MAX
     * @throws IOException
     */
    private void getNextSplit() throws IOException {
        // close the old lineRecordReader
        lineRecordReader.close();
        // we need to move into the next split, so create one that starts at the end of the current split
        // and goes until the end of the file
        FileSplit nextSplit = new FileSplit(file, end, Long.MAX_VALUE - end, hosts);
        lineRecordReader = new LineRecordReader(conf, nextSplit);
        inNextSplit = true;
    }

    /**
     * Reads the next line of the file in to begin parsing the characters
     * @return
     * @throws IOException
     */
    private boolean getNextLine() throws IOException {
        currentLine.clear();
        long currentPos = lineRecordReader.getPos();
        boolean didReturnLine = lineRecordReader.next(lineRecordReader.createKey(), currentLine);
        linePos = lineRecordReader.getPos();
        if (didReturnLine) {
            // lineRecordReader removes the new lines and carriage returns when it does the read
            // we want to track that delta so we know the proper size of the line that was returned
            long delta = linePos - currentPos - currentLine.getLength();
            // append the removed chars back for proper accounting
            if (delta == 2) {
                currentLine.append(CARRIAGERETURN_NEWLINE, 0, CARRIAGERETURN_NEWLINE.length);
            } else if (delta == 1) {
                currentLine.append(NEW_LINE, 0, NEW_LINE.length);
            } else {
                LOG.debug("LineRecordReader removed more characters than expected");
            }
            currentLineBuffer = new StringBuffer(currentLine.toString());
            currentLineIndex = 0;
        }
        return didReturnLine;
    }
}
