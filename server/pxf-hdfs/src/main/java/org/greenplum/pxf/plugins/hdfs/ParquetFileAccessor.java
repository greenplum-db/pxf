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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.parquet.filter.ColumnPredicates.equalTo;
import static org.apache.parquet.filter.ColumnRecordFilter.column;

/**
 * Parquet file accessor.
 * Unit of operation is record.
 */
public class ParquetFileAccessor extends BasePlugin implements Accessor {

    private static final int DECIMAL_SCALE = 18;
    private static final int DECIMAL_PRECISION = 38;
    private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
    private static final int DEFAULT_FILE_SIZE = 128 * 1024 * 1024;
    private static final int DEFAULT_ROWGROUP_SIZE = 8 * 1024 * 1024;
    private static final int DEFAULT_DICTIONARY_PAGE_SIZE = 512 * 1024;
    private static final WriterVersion DEFAULT_PARQUET_VERSION = WriterVersion.PARQUET_1_0;
    private static final CompressionCodecName DEFAULT_COMPRESSION = CompressionCodecName.SNAPPY;

    private ParquetFileReader fileReader;
    private MessageColumnIO columnIO;
    private CompressionCodecName codecName;
    private ParquetWriter<Group> parquetWriter;
    private FilterCompat.Filter recordFilter;
    private RecordReader<Group> recordReader;
    private GroupRecordConverter groupRecordConverter;
    private GroupWriteSupport groupWriteSupport;
    private FileSystem fs;
    private Path file;
    private String filePrefix;
    private int fileIndex, pageSize, rowgroupSize, dictionarySize;
    private long rowsRead, rowsWritten, totalRowsRead, totalRowsWritten;
    private long rowsInRowGroup, rowGroupsReadCount;
    private WriterVersion parquetVersion;
    private CodecFactory codecFactory = CodecFactory.getInstance();

    /**
     * Opens the resource for read.
     *
     * @throws IOException if opening the resource failed
     */
    @Override
    public boolean openForRead() throws IOException {
        MessageType schema, readSchema;

        file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);
        // Create reader for a given split, read a range in file
        MetadataFilter filter = ParquetMetadataConverter.range(
                fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength());

        recordFilter = getRecordFilter();

        ParquetReadOptions parquetReadOptions = HadoopReadOptions
                .builder(configuration)
                .withMetadataFilter(filter)
                .withRecordFilter(recordFilter)
                .build();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(this.file, configuration);
        fileReader = ParquetFileReader.open(inputFile, parquetReadOptions);

        try {
            ParquetMetadata metadata = fileReader.getFooter();
            schema = metadata.getFileMetaData().getSchema();
            readSchema = buildReadSchema(schema);

            columnIO = new ColumnIOFactory().getColumnIO(readSchema, schema);
            groupRecordConverter = new GroupRecordConverter(readSchema);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reading file {} with {} records in {} rowgroups",
                        file.getName(), fileReader.getRecordCount(),
                        fileReader.getRowGroups().size());
            }
        } catch (Exception e) {
            fileReader.close();
            throw new IOException(e);
        }
        context.setMetadata(readSchema);
        return true;
    }

    private FilterCompat.Filter getRecordFilter() {
        String filterString = context.getFilterString();
        if (StringUtils.isBlank(filterString)) {
            return null;
        }

        return new ParquetRecordFilterBuilder(context.getTupleDescription())
                .buildRecordFilter(filterString);
//        return FilterCompat.get(column("l_partkey", equalTo(185761L)));
    }

    /**
     * Reads the next record.
     *
     * @return one record or null when split is already exhausted
     * @throws IOException if unable to read
     */
    @Override
    public OneRow readNextObject() throws IOException {

        if (rowsRead == rowsInRowGroup && !readNextRowGroup())
            return null;
        Group group = recordReader.read();

        while (group == null && readNextRowGroup()) {
            group = recordReader.read();
        }

        if (group == null)
            return null;

        rowsRead++;

        return new OneRow(null, group);
    }

    private boolean readNextRowGroup() throws IOException {

        PageReadStore currentRowGroup = fileReader.readNextRowGroup();
        if (currentRowGroup == null) {
            LOG.debug("All rowgroups have been exhausted for {}", file.getName());
            return false;
        }

        rowGroupsReadCount++;
        totalRowsRead += rowsRead;
        // Reset rows read
        rowsRead = 0;
        recordReader = columnIO.getRecordReader(currentRowGroup, groupRecordConverter, recordFilter);
        rowsInRowGroup = currentRowGroup.getRowCount();

        LOG.debug("Reading {} rows (rowgroup {})", rowsInRowGroup, rowGroupsReadCount);
        return true;
    }

    private int getOption(String optionName, int defaultValue) {
        String optionStr = context.getOption(optionName);
        return optionStr != null ? Integer.parseInt(optionStr) : defaultValue;
    }

    /**
     * Closes the resource for read.
     *
     * @throws IOException if closing the resource failed
     */
    @Override
    public void closeForRead() throws IOException {

        totalRowsRead += rowsRead;
        LOG.debug("Read TOTAL of {} rows in {} rowgroups", totalRowsRead, rowGroupsReadCount);
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

        HcfsType hcfsType = HcfsType.getHcfsType(configuration, context);
        // skip codec extension in filePrefix, because we add it in this accessor
        filePrefix = hcfsType.getUriForWrite(configuration, context, true);
        String compressCodec = context.getOption("COMPRESSION_CODEC");
        codecName = codecFactory.getCodec(compressCodec, DEFAULT_COMPRESSION);

        // Options for parquet write
        pageSize = getOption("PAGE_SIZE", DEFAULT_PAGE_SIZE);
        rowgroupSize = getOption("ROWGROUP_SIZE", DEFAULT_ROWGROUP_SIZE);
        dictionarySize = getOption("DICTIONARY_PAGE_SIZE", DEFAULT_DICTIONARY_PAGE_SIZE);
        String parquetVerStr = context.getOption("PARQUET_VERSION");
        parquetVersion = parquetVerStr != null ? WriterVersion.fromString(parquetVerStr.toLowerCase()) : DEFAULT_PARQUET_VERSION;
        LOG.debug("Parquet options: PAGE_SIZE = {}, ROWGROUP_SIZE = {}, DICTIONARY_PAGE_SIZE = {}, PARQUET_VERSION = {}",
                pageSize, rowgroupSize, dictionarySize, parquetVersion);

        // Read schema file, if given
        String schemaFile = context.getOption("SCHEMA");
        MessageType schema = (schemaFile != null) ? readSchemaFile(schemaFile) :
                generateParquetSchema(context.getTupleDescription());
        LOG.debug("Schema fields = {}", schema.getFields());
        GroupWriteSupport.setSchema(schema, configuration);
        groupWriteSupport = new GroupWriteSupport();

        // We get the parquet schema and set it to the metadata in the request context
        // to avoid computing the schema again in the Resolver
        context.setMetadata(schema);
        createParquetWriter();
        return true;
    }

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     * @throws IOException writing to the resource failed
     */
    @Override
    public boolean writeNextObject(OneRow onerow) throws IOException {

        parquetWriter.write((Group) onerow.getData());
        rowsWritten++;
        // Check for the output file size every 1000 rows
        if (rowsWritten % 1000 == 0 && parquetWriter.getDataSize() > DEFAULT_FILE_SIZE) {
            parquetWriter.close();
            totalRowsWritten += rowsWritten;
            // Reset rows written
            rowsWritten = 0;
            fileIndex++;
            createParquetWriter();
        }
        return true;
    }

    /**
     * Closes the resource for write.
     *
     * @throws IOException if closing the resource failed
     */
    @Override
    public void closeForWrite() throws IOException {

        if (parquetWriter != null) {
            parquetWriter.close();
            totalRowsWritten += rowsWritten;
        }
        LOG.debug("Segment {}: writer closed, wrote a TOTAL of {} rows to {} on server {}",
                context.getSegmentId(),
                totalRowsWritten,
                context.getDataSource(),
                context.getServerName());
    }

    /**
     * Generates a read schema when there is column projection
     *
     * @param originalSchema the original read schema
     */
    private MessageType buildReadSchema(MessageType originalSchema) {
        Map<String, Type> originalFields = new HashMap<>(originalSchema.getFieldCount() * 2);

        // We need to add the original name and lower cased name to
        // the map to support mixed case where in GPDB the column name
        // was created with quotes i.e "mIxEd CaSe". When quotes are not
        // used to create a table in GPDB, the name of the column will
        // always come in lower-case
        originalSchema.getFields().forEach(t -> {
            String columnName = t.getName();
            originalFields.put(columnName, t);
            originalFields.put(columnName.toLowerCase(), t);
        });

        List<Type> projectedFields = context.getTupleDescription().stream()
                .filter(ColumnDescriptor::isProjected)
                .map(c -> {
                    Type t = originalFields.get(c.columnName());
                    if (t == null) {
                        throw new IllegalArgumentException(
                                String.format("Column %s is missing from parquet schema", c.columnName()));
                    }
                    return t;
                })
                .collect(Collectors.toList());
        return new MessageType(originalSchema.getName(), projectedFields);
    }

    private void createParquetWriter() throws IOException {

        String fileName = filePrefix + "." + fileIndex;
        fileName += codecName.getExtension() + ".parquet";
        LOG.debug("Creating file {}", fileName);
        file = new Path(fileName);
        fs = FileSystem.get(URI.create(fileName), configuration);
        HdfsUtilities.validateFile(file, fs);

        //noinspection deprecation
        parquetWriter = new ParquetWriter<>(file, groupWriteSupport, codecName,
                rowgroupSize, pageSize, dictionarySize,
                true, false, parquetVersion, configuration);
    }

    /**
     * Generate parquet schema using schema file
     */
    private MessageType readSchemaFile(String schemaFile)
            throws IOException {

        LOG.debug("Using parquet schema from given schema file {}", schemaFile);
        try (InputStream inputStream = fs.open(new Path(schemaFile))) {
            return MessageTypeParser.parseMessageType(IOUtils.toString(inputStream));
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

            PrimitiveTypeName typeName;
            OriginalType origType = null;
            DecimalMetadata dmt = null;
            int length = 0;
            switch (DataType.get(columnTypeCode)) {
                case BOOLEAN:
                    typeName = PrimitiveTypeName.BOOLEAN;
                    break;
                case BYTEA:
                    typeName = PrimitiveTypeName.BINARY;
                    break;
                case BIGINT:
                    typeName = PrimitiveTypeName.INT64;
                    break;
                case SMALLINT:
                    origType = OriginalType.INT_16;
                    typeName = PrimitiveTypeName.INT32;
                    break;
                case INTEGER:
                    typeName = PrimitiveTypeName.INT32;
                    break;
                case REAL:
                    typeName = PrimitiveTypeName.FLOAT;
                    break;
                case FLOAT8:
                    typeName = PrimitiveTypeName.DOUBLE;
                    break;
                case NUMERIC:
                    origType = OriginalType.DECIMAL;
                    typeName = PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
                    length = 16; //per parquet specs
                    dmt = new DecimalMetadata(DECIMAL_PRECISION, DECIMAL_SCALE);
                    break;
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                    typeName = PrimitiveTypeName.INT96;
                    break;
                case DATE:
                case TIME:
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                    origType = OriginalType.UTF8;
                    typeName = PrimitiveTypeName.BINARY;
                    break;
                default:
                    throw new UnsupportedTypeException("Type " + columnTypeCode + "is not supported");
            }
            fields.add(new PrimitiveType(
                    Type.Repetition.OPTIONAL,
                    typeName,
                    length,
                    columnName,
                    origType,
                    dmt,
                    null));
        }

        return new MessageType("hive_schema", fields);
    }
}
