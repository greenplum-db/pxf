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
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * A PXF Accessor for reading delimited plain text records.
 */
public class LineBreakAccessor extends HdfsSplittableDataAccessor {
    private DataOutputStream dos;
    private FSDataOutputStream fsdos;
    private FileSystem fs;
    private Path file;
    private CodecFactory codecFactory;

    /**
     * Constructs a LineBreakAccessor.
     */
    public LineBreakAccessor() {
        super(new TextInputFormat());
        this.codecFactory = CodecFactory.getInstance();
    }

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        ((TextInputFormat) inputFormat).configure(jobConf);
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split)
            throws IOException {

        return (hcfsType == HcfsType.HDFS) ?
                new ChunkRecordReader(jobConf, (FileSplit) split) :
                new LineRecordReader(jobConf, (FileSplit) split);
    }

    /**
     * Opens file for write.
     */
    @Override
    public boolean openForWrite() throws IOException {
        String fileName = hcfsType.getUriForWrite(jobConf, context);
        String compressCodec = context.getOption("COMPRESSION_CODEC");
        // get compression codec
        CompressionCodec codec = compressCodec != null ?
                codecFactory.getCodec(compressCodec, configuration) : null;

        file = new Path(fileName);
        fs = FileSystem.get(URI.create(fileName), configuration);
        HdfsUtilities.validateFile(file, fs);

        // create output stream - do not allow overwriting existing file
        createOutputStream(file, codec);
        return true;
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

    /**
     * Writes row into stream.
     */
    @Override
    public boolean writeNextObject(OneRow onerow) throws IOException {
        dos.write((byte[]) onerow.getData());
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
}
