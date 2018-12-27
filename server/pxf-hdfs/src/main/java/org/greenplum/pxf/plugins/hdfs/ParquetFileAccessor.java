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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Parquet file accessor.
 * Unit of operation is record.
 */
public class ParquetFileAccessor extends BasePlugin implements Accessor {

    private ParquetFileReader fileReader;
    private MessageColumnIO columnIO;
    private HcfsType hcfsType;
    private ParquetWriter<Group> parquetWriter;
    private RecordReader<Group> recordReader;
    private long rowsInRowGroup;

    private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
    private static final int DEFAULT_ROWGROUP_SIZE = 8 * 1024 * 1024;
    private static final int DEFAULT_DICTIONARY_PAGE_SIZE = 512 * 1024;
    private static final WriterVersion DEFAULT_PARQUET_VERSION = WriterVersion.PARQUET_1_0;
    private static final CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME = CompressionCodecName.UNCOMPRESSED;

    MessageType schema;
    SimpleGroupFactory groupFactory;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, requestContext);
        schema = context.getFragmentUserData() == null ?
                autoGenerateParquetSchema(requestContext.getTupleDescription()) :
                MessageTypeParser.parseMessageType(new String(context.getFragmentUserData()));
        groupFactory = new SimpleGroupFactory(schema);
    }

    @Override
    public boolean openForRead() throws IOException {
        Path file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);
        // Create reader for a given split, read a range in file
        fileReader = new ParquetFileReader(configuration, file, ParquetMetadataConverter.range(
                fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength()));
        columnIO = new ColumnIOFactory().getColumnIO(schema);
        return readNextRowGroup();
    }

    private boolean readNextRowGroup() throws IOException {
        PageReadStore currentRowGroup = fileReader.readNextRowGroup();
        if (currentRowGroup == null)
            return false;
        recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(schema));
        rowsInRowGroup = currentRowGroup.getRowCount();
        return true;
    }

    /**
     * @return one record or null when split is already exhausted
     */
    @Override
    public OneRow readNextObject() throws IOException {
        if (rowsInRowGroup-- == 0 && !readNextRowGroup())
            return null;
        return new OneRow(null, recordReader.read());
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
        CompressionCodecName codecName = DEFAULT_COMPRESSION_CODEC_NAME;
        CompressionCodec codec;

        // get compression codec
        if (compressCodec != null) {
            codec = HdfsUtilities.getCodec(configuration, compressCodec);
            String extension = codec.getDefaultExtension();
            fileName += extension;
            switch (compressCodec) {
                case "lzo":
                    codecName = CompressionCodecName.LZO;
                    break;
                case "snappy":
                    codecName = CompressionCodecName.SNAPPY;
                    break;
                case "gz":
                    codecName = CompressionCodecName.GZIP;
                    break;
                default:
                    throw new IOException("compression method not support, codec:" + compressCodec);
            }
        }

        FileSystem fs = FileSystem.get(URI.create(fileName), configuration);
        Path file = new Path(fileName);
        if (fs.exists(file)) {
            throw new IOException("File " + file.toString() + " already exists, can't write data");
        }
        Path parent = file.getParent();
        if (!fs.exists(parent)) {
            fs.mkdirs(parent);
            LOG.debug("Created new dir {}", parent);
        }

        GroupWriteSupport.setSchema(schema, configuration);
        //noinspection deprecation
        parquetWriter = new ParquetWriter<>(file, new GroupWriteSupport(), codecName,
                DEFAULT_ROWGROUP_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_DICTIONARY_PAGE_SIZE,
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
        parquetWriter.write((Group) onerow.getData());
        return true;
    }

    /**
     * Closes the resource for write.
     *
     * @throws Exception if closing the resource failed
     */
    @Override
    public void closeForWrite() throws Exception {
        if (parquetWriter != null) {
            parquetWriter.close();
        }
    }



    /**
     * generate schema automatically
     */
    public static MessageType autoGenerateParquetSchema(List<ColumnDescriptor> columns) {

        List<Type> fields = new ArrayList<>();
        for (ColumnDescriptor column: columns) {
            String columnName = column.columnName();
            int columnTypeCode = column.columnTypeCode();

            PrimitiveType.PrimitiveTypeName typeName;
            OriginalType origType = null;
            DecimalMetadata dmt = null;
            int length = 0;
            switch (DataType.get(columnTypeCode)) {
                case BOOLEAN:
                    typeName = PrimitiveType.PrimitiveTypeName.BOOLEAN;
                    break;
                case BYTEA:
                    typeName = PrimitiveType.PrimitiveTypeName.BINARY;
                    break;
                case BIGINT:
                    typeName = PrimitiveType.PrimitiveTypeName.INT64;
                    break;
                case SMALLINT:
                    origType = OriginalType.INT_16;
                    typeName = PrimitiveType.PrimitiveTypeName.INT32;
                    break;
                case INTEGER:
                    typeName = PrimitiveType.PrimitiveTypeName.INT32;
                    break;
                case REAL:
                    typeName = PrimitiveType.PrimitiveTypeName.FLOAT;
                    break;
                case FLOAT8:
                    typeName = PrimitiveType.PrimitiveTypeName.DOUBLE;
                    break;
                case NUMERIC:
                    origType = OriginalType.DECIMAL;
                    typeName = PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
                    length = 16;
                    dmt = new DecimalMetadata(38, 18);
                    break;
                case TIMESTAMP:
                    typeName = PrimitiveType.PrimitiveTypeName.INT96;
                    break;
                case DATE:
                case TIME:
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                    origType = OriginalType.UTF8;
                    typeName = PrimitiveType.PrimitiveTypeName.BINARY;
                    break;
                default:
                    throw new UnsupportedTypeException("Type " + columnTypeCode + "is not supported");
            }
            fields.add(new PrimitiveType(Type.Repetition.OPTIONAL,
                    typeName, length, columnName, origType, dmt, null));
        }

        return new MessageType("hive_schema", fields);
    }

}
