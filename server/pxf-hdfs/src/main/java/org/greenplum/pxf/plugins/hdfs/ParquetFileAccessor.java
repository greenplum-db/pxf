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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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

    private static final int DECIMAL_SCALE = 18;
    private static final int DECIMAL_PRECISION = 38;
    private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
    private static final int DEFAULT_ROWGROUP_SIZE = 8 * 1024 * 1024;
    private static final int DEFAULT_DICTIONARY_PAGE_SIZE = 512 * 1024;
    private static final WriterVersion DEFAULT_PARQUET_VERSION = WriterVersion.PARQUET_1_0;

    private MessageType schema;
    private ParquetFileReader fileReader;
    private MessageColumnIO columnIO;
    private HcfsType hcfsType;
    private ParquetWriter<Group> parquetWriter;
    private RecordReader<Group> recordReader;
    private long rowsInRowGroup;
    private long rowGroupsReadCount;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, requestContext);
        schema = context.getFragmentUserData() == null ?
                generateParquetSchema(requestContext.getTupleDescription()) : // write-flow
                MessageTypeParser.parseMessageType(new String(context.getFragmentUserData())); // read-flow
        LOG.debug("Schema fields = {}", schema.getFields());

        // We get the parquet schema and set it to the metadata in the request context
        // to avoid computing the schema again in the Resolver
        context.setMetadata(schema);
    }

    /**
     * Opens the resource for read.
     *
     * @throws IOException if opening the resource failed
     */
    @Override
    public boolean openForRead() throws IOException {

        Path file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);
        // Create reader for a given split, read a range in file
        fileReader = new ParquetFileReader(configuration, file, ParquetMetadataConverter.range(
                fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength()));
        columnIO = new ColumnIOFactory().getColumnIO(schema);
        return true;
    }

    /**
     * Reads the next record.
     *
     * @return one record or null when split is already exhausted
     * @throws IOException if unable to read
     */
    @Override
    public OneRow readNextObject() throws IOException {

        rowsInRowGroup = (rowsInRowGroup == 0) ? readNextRowGroup() : rowsInRowGroup;

        if (rowsInRowGroup == 0) {
            return null;
        }

        rowsInRowGroup--;

        return new OneRow(null, recordReader.read());
    }

    private long readNextRowGroup() throws IOException {

        PageReadStore currentRowGroup = fileReader.readNextRowGroup();
        if (currentRowGroup == null) {
            LOG.debug("All row groups have been exhausted");
            return 0;
        }
        rowGroupsReadCount++;
        recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(schema));
        long count = currentRowGroup.getRowCount();

        LOG.debug("Found {} rows in row group #{}", count, rowGroupsReadCount);

        return count;
    }

    private CompressionCodecName getCodec(String name) {
        CompressionCodecName codecName = CompressionCodecName.SNAPPY;
        if (name != null) {
            try {
                codecName = CompressionCodecName.fromConf(name);
            } catch(IllegalArgumentException ie) {
                try {
                    codecName = CompressionCodecName.fromCompressionCodec(Class.forName(name));
                } catch(ClassNotFoundException ce) {
                    throw new IllegalArgumentException(String.format("Invalid codec: %s ", name));
                }
            }
        }
        return codecName;
    }

    /**
     * Closes the resource for read.
     *
     * @throws IOException if closing the resource failed
     */
    @Override
    public void closeForRead() throws IOException {
        LOG.debug("Read {} rowGroups", rowGroupsReadCount);
        if (fileReader != null) {
            fileReader.close();
        }
    }

    /**
     * Opens the resource for write.
     * Uses compression codec based on user input which
     * defaults to Snappy
     *
     * @return true if the resource is successfully opened
     * @throws IOException if opening the resource failed
     */
    @Override
    public boolean openForWrite() throws IOException {

        String fileName = hcfsType.getDataUri(configuration, context);
        String compressCodec = context.getOption("COMPRESSION_CODEC");
        CompressionCodecName codecName = getCodec(compressCodec);
        fileName += codecName.getExtension() + ".parquet";
        LOG.debug("Creating file {}", fileName);

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
     * Generate parquet schema using column descriptors
     */
    private MessageType generateParquetSchema(List<ColumnDescriptor> columns) {

        LOG.debug("Generating parquet schema for write using {}", columns);
        List<Type> fields = new ArrayList<>();
        for (ColumnDescriptor column : columns) {
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
                    length = 16; //per parquet specs
                    dmt = new DecimalMetadata(DECIMAL_PRECISION, DECIMAL_SCALE);
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
