package org.greenplum.pxf.plugins.hdfs;

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


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * A PXF Accessor for reading delimited plain text records.
 */
public class LineBreakAccessor extends HdfsSplittableDataAccessor {

    private static final int DEFAULT_BUFFER_SIZE = 8192;
    public static final String PXF_CHUNK_RECORD_READER_ENABLED = "pxf.reader.chunk-record-reader.enabled";
    public static final boolean PXF_CHUNK_RECORD_READER_DEFAULT = false;

    private int skipHeaderCount;
    private DataOutputStream dos;
    private FSDataOutputStream fsdos;
    private FileSystem fs;
    private Path file;

    /**
     * Constructs a LineBreakAccessor.
     */
    public LineBreakAccessor() {
        super(new TextInputFormat());
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        ((TextInputFormat) inputFormat).configure(jobConf);
        skipHeaderCount = context.getFragmentIndex() == 0
                ? context.getOption("SKIP_HEADER_COUNT", 0, true)
                : 0;
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split)
            throws IOException {

        // Disable the ChunkRecordReader by default, but it can be enabled by
        // setting the `pxf.reader.chunk-record-reader.enabled` property to true
        if (configuration.getBoolean(PXF_CHUNK_RECORD_READER_ENABLED, PXF_CHUNK_RECORD_READER_DEFAULT)) {
            try {
                return new ChunkRecordReader(jobConf, (FileSplit) split);
            } catch (IncompatibleInputStreamException e) {
                // ignore and fallback to using LineRecordReader
                LOG.debug("Failed to use ChunkRecordReader, falling back to LineRecordReader : " + e.getMessage());
            }
        }
        return new LineRecordReader(jobConf, (FileSplit) split,
                context.getGreenplumCSV().getNewline().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public OneRow readNextObject() throws IOException {
        while (skipHeaderCount > 0) {
            if (super.readNextObject() == null)
                return null;
            skipHeaderCount--;
        }
        return super.readNextObject();
    }

    /**
     * Opens file for write.
     */
    @Override
    public boolean openForWrite() throws IOException {
        String compressCodec = context.getOption("COMPRESSION_CODEC");
        // get compression codec
        CompressionCodec codec = compressCodec != null ?
                getCodec(compressCodec) : null;
        String fileName = hcfsType.getUriForWrite(context, codec);

        file = new Path(fileName);
        fs = FileSystem.get(URI.create(fileName), configuration);
        HdfsUtilities.validateFile(file, fs);

        // create output stream - do not allow overwriting existing file
        createOutputStream(file, codec);
        return true;
    }

    /**
     * Writes row into stream.
     */
    @Override
    public boolean writeNextObject(OneRow onerow) throws IOException {
        if (onerow.getData() instanceof InputStream) {
            // io.file.buffer.size is the name of the configuration parameter
            // used to determine the buffer size of the DataOutputStream. We
            // match same buffer size to read data from the input stream. The
            // buffer size can be configured externally.
            int bufferSize = configuration.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);

            InputStream inputStream = (InputStream) onerow.getData();
            final byte[] buffer = new byte[bufferSize];

            // The logic below is copied from IOUtils.copyLarge to add logging
            long count = 0;
            int n;
            long iterationCount = 0;
            long iterationByteCount = 0;

            while (-1 != (n = inputStream.read(buffer))) {
                dos.write(buffer, 0, n);
                count += n;
                iterationByteCount += n;
                if (LOG.isDebugEnabled() && (iterationCount % 100 == 0)) {
                    // Log this message after every 100th Iteration
                    LOG.debug("wrote {} bytes, total so far {} (buffer size {})", iterationByteCount, count, bufferSize);
                    iterationByteCount = 0;
                }

                iterationCount++;
            }

            LOG.debug("Wrote {} bytes to outputStream using a buffer of size {}", count, bufferSize);
            return count > 0;
        } else {
            dos.write((byte[]) onerow.getData());
        }
        return true;
    }

    /**
     * Closes the output stream after done writing.
     */
    @Override
    public void closeForWrite() throws IOException {
        if ((dos != null) && (fsdos != null)) {
            LOG.debug("Closing writing stream for path {}", file);
            dos.flush();
            /*
             * From release 0.21.0 sync() is deprecated in favor of hflush(),
             * which only guarantees that new readers will see all data written
             * to that point, and hsync(), which makes a stronger guarantee that
             * the operating system has flushed the data to disk (like POSIX
             * fsync), although data may still be in the disk cache.
             */
            fsdos.hsync();
            dos.close();
        }
    }

    /*
     * Creates output stream from given file. If compression codec is provided,
     * wrap it around stream.
     */
    private void createOutputStream(Path file, CompressionCodec codec)
            throws IOException {
        fsdos = fs.create(file, false);
        if (codec != null) {
            dos = new DataOutputStream(codec.createOutputStream(fsdos));
        } else {
            dos = fsdos;
        }

    }
}
