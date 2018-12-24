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
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Parquet file accessor.
 * Unit of operation is record.
 */
public class ParquetFileAccessor extends BasePlugin implements Accessor {

    private ParquetFileReader fileReader;
    private MessageColumnIO columnIO;
    private MessageType schema;
    private DataOutputStream dos;
    private FSDataOutputStream fsdos;
    private ParquetFileWriter writer;
    private JobConf jobConf;
    private HcfsType hcfsType;
    private Path file;
    private ParquetWriter<Group> dataFileWriter;
    private PageReadStore currentRowGroup;
    private RecordReader<Group> recordReader;
    private long rowsInRowGroup;

    private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
    private static final int DEFAULT_ROWGROUP_SIZE = 8 * 1024 * 1024;
    private static final int DEFAULT_DICTIONARY_PAGE_SIZE = 512 * 1024;
    private static final WriterVersion DEFAULT_PARQUET_VERSION = WriterVersion.PARQUET_1_0;
    private static final CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME = CompressionCodecName.UNCOMPRESSED;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        // variable required for the splits iteration logic
        jobConf = new JobConf(configuration,  ParquetFileAccessor.class);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, requestContext);
    }

    @Override
    public boolean openForRead() throws Exception {
        Path file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);
        // Create reader for a given split, read a range in file
        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, file, ParquetMetadataConverter.NO_FILTER);
        schema = readFooter.getFileMetaData().getSchema();
        columnIO = new ColumnIOFactory().getColumnIO(schema);
        fileReader = new ParquetFileReader(configuration, file, ParquetMetadataConverter.range(
                fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength()));
        currentRowGroup = fileReader.readNextRowGroup();
        recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(schema));
        rowsInRowGroup = currentRowGroup.getRowCount();
        return currentRowGroup != null;
    }

    /**
     * @return one record or null when split is already exhausted
     */
    @Override
    public OneRow readNextObject() throws IOException {
        Group g = recordReader.read();
        if (g == null) {
            currentRowGroup = fileReader.readNextRowGroup();
            if (currentRowGroup == null)
                return null;
            rowsInRowGroup = currentRowGroup.getRowCount();
            recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(schema));
            g = recordReader.read();
        }
        return new OneRow(null, g);
    }

    @Override
    public void closeForRead() throws Exception {
        if (fileReader != null) {
            fileReader.close();
        }
    }

    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     * @throws Exception if opening the resource failed
     */
    @Override
    public boolean openForWrite() throws Exception {

        String fileName = hcfsType.getDataUri(configuration, context);
        String compressCodec = context.getOption("COMPRESSION_CODEC");
        CompressionCodec codec = null;

        // get compression codec
        if (compressCodec != null) {
            codec = HdfsUtilities.getCodec(configuration, compressCodec);
            String extension = codec.getDefaultExtension();
            fileName += extension;
        }

        FileSystem fs = FileSystem.get(URI.create(fileName), configuration);
        file = new Path(fileName);
        if (fs.exists(file)) {
            throw new IOException("File " + file.toString() + " already exists, can't write data");
        }
        Path parent = file.getParent();
        if (!fs.exists(parent)) {
            fs.mkdirs(parent);
            LOG.debug("Created new dir {}", parent);
        }

        // create output stream - do not allow overwriting existing file
        fsdos = fs.create(file, false);
        dos = (codec != null) ? new DataOutputStream(codec.createOutputStream(fsdos)) : fsdos;

        ParquetWriter<Group> writer = new ParquetWriter<>(file, new GroupWriteSupport(),
                DEFAULT_COMPRESSION_CODEC_NAME, DEFAULT_ROWGROUP_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_DICTIONARY_PAGE_SIZE,
                false, false, DEFAULT_PARQUET_VERSION, configuration);
        return true;
    }

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     * @throws Exception writing to the resource failed
     */
    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes the resource for write.
     *
     * @throws Exception if closing the resource failed
     */
    @Override
    public void closeForWrite() throws Exception {
        if (dos != null && fsdos != null) {
            LOG.debug("Closing writing stream for path {}", file);
            dos.flush();
            fsdos.hsync();
            dos.close();
        }
    }
}
