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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.ListType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.GroupType;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.InOperatorTransformer;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.filter.BPCharOperatorTransformer;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetOperatorPruner;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetRecordFilterBuilder;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_PAGE_SIZE;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_WRITER_VERSION;
import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetOutputFormat.DICTIONARY_PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetOutputFormat.ENABLE_DICTIONARY;
import static org.apache.parquet.hadoop.ParquetOutputFormat.PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;

/**
 * Parquet file accessor.
 * Unit of operation is record.
 */
public class ParquetFileAccessor extends BasePlugin implements Accessor {

    private static final int DEFAULT_ROWGROUP_SIZE = 8 * 1024 * 1024;
    private static final CompressionCodecName DEFAULT_COMPRESSION = CompressionCodecName.SNAPPY;

    // From org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
    public static final int[] PRECISION_TO_BYTE_COUNT = new int[38];

    static {
        for (int prec = 1; prec <= 38; prec++) {
            // Estimated number of bytes needed.
            PRECISION_TO_BYTE_COUNT[prec - 1] = (int)
                    Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
        }
    }

    public static final EnumSet<Operator> SUPPORTED_OPERATORS = EnumSet.of(
            Operator.NOOP,
            Operator.LESS_THAN,
            Operator.GREATER_THAN,
            Operator.LESS_THAN_OR_EQUAL,
            Operator.GREATER_THAN_OR_EQUAL,
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.IS_NULL,
            Operator.IS_NOT_NULL,
            // Operator.IN,
            Operator.OR,
            Operator.AND,
            Operator.NOT
    );

    private static final TreeTraverser TRAVERSER = new TreeTraverser();
    private static final TreeVisitor IN_OPERATOR_TRANSFORMER = new InOperatorTransformer();

    private ParquetReader<Group> fileReader;
    private CompressionCodecName codecName;
    private RecordWriter<Void, Group> recordWriter;
    private GroupWriteSupport groupWriteSupport;
    private FileSystem fs;
    private Path file;
    private String filePrefix;
    private boolean enableDictionary;
    private int pageSize, rowGroupSize, dictionarySize;
    private long rowsRead, totalRowsRead, totalRowsWritten;
    private WriterVersion parquetVersion;
    private long totalReadTimeInNanos;

    /**
     * Opens the resource for read.
     *
     * @throws IOException if opening the resource failed
     */
    @Override
    public boolean openForRead() throws IOException {
        file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context.getDataSource(), context.getFragmentMetadata());

        // Read the original schema from the parquet file
        MessageType originalSchema = getSchema(file, fileSplit);
        // Get a map of the column name to Types for the given schema
        Map<String, Type> originalFieldsMap = getOriginalFieldsMap(originalSchema);
        // Get the read schema. This is either the full set or a subset (in
        // case of column projection) of the greenplum schema.
        MessageType readSchema = buildReadSchema(originalFieldsMap, originalSchema);
        // Get the record filter in case of predicate push-down
        FilterCompat.Filter recordFilter = getRecordFilter(context.getFilterString(), originalFieldsMap);

        // add column projection
        configuration.set(PARQUET_READ_SCHEMA, readSchema.toString());

        fileReader = ParquetReader.builder(new GroupReadSupport(), file)
                .withConf(configuration)
                // Create reader for a given split, read a range in file
                .withFileRange(fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength())
                .withFilter(recordFilter)
                .build();
        context.setMetadata(readSchema);
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
        final long then = System.nanoTime();
        Group group = fileReader.read();
        final long nanos = System.nanoTime() - then;
        totalReadTimeInNanos += nanos;

        if (group != null) {
            rowsRead++;
            return new OneRow(null, group);
        }
        return null;
    }

    /**
     * Closes the resource for read.
     *
     * @throws IOException if closing the resource failed
     */
    @Override
    public void closeForRead() throws IOException {

        totalRowsRead += rowsRead;

        logReadStats(totalRowsRead, totalReadTimeInNanos);
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
    public boolean openForWrite() throws IOException, InterruptedException {

        HcfsType hcfsType = HcfsType.getHcfsType(context);
        // skip codec extension in filePrefix, because we add it in this accessor
        filePrefix = hcfsType.getUriForWrite(context);
        String compressCodec = context.getOption("COMPRESSION_CODEC");
        codecName = getCodecName(compressCodec, DEFAULT_COMPRESSION);

        // Options for parquet write
        pageSize = context.getOption("PAGE_SIZE", DEFAULT_PAGE_SIZE);
        rowGroupSize = context.getOption("ROWGROUP_SIZE", DEFAULT_ROWGROUP_SIZE);
        enableDictionary = context.getOption("ENABLE_DICTIONARY", DEFAULT_IS_DICTIONARY_ENABLED);
        dictionarySize = context.getOption("DICTIONARY_PAGE_SIZE", DEFAULT_DICTIONARY_PAGE_SIZE);
        String parquetVerStr = context.getOption("PARQUET_VERSION");
        parquetVersion = parquetVerStr != null ? WriterVersion.fromString(parquetVerStr.toLowerCase()) : DEFAULT_WRITER_VERSION;
        LOG.debug("{}-{}: Parquet options: PAGE_SIZE = {}, ROWGROUP_SIZE = {}, DICTIONARY_PAGE_SIZE = {}, PARQUET_VERSION = {}, ENABLE_DICTIONARY = {}",
                context.getTransactionId(), context.getSegmentId(), pageSize, rowGroupSize, dictionarySize, parquetVersion, enableDictionary);

        // Read schema file, if given
        String schemaFile = context.getOption("SCHEMA");
        MessageType schema = (schemaFile != null) ? readSchemaFile(schemaFile) :
                generateParquetSchema(context.getTupleDescription());
        LOG.debug("{}-{}: Schema fields = {}", context.getTransactionId(),
                context.getSegmentId(), schema.getFields());
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
    public boolean writeNextObject(OneRow onerow) throws IOException, InterruptedException {
        recordWriter.write(null, (Group) onerow.getData());
        totalRowsWritten++;
        return true;
    }

    /**
     * Closes the resource for write.
     *
     * @throws IOException if closing the resource failed
     */
    @Override
    public void closeForWrite() throws IOException, InterruptedException {

        if (recordWriter != null) {
            recordWriter.close(null);
        }
        LOG.debug("{}-{}: writer closed, wrote a TOTAL of {} rows to {} on server {}",
                context.getTransactionId(),
                context.getSegmentId(),
                totalRowsWritten,
                context.getDataSource(),
                context.getServerName());
    }

    /**
     * Returns the parquet record filter for the given filter string
     *
     * @param filterString      the filter string
     * @param originalFieldsMap a map of field names to types
     * @return the parquet record filter for the given filter string
     */
    private FilterCompat.Filter getRecordFilter(String filterString, Map<String, Type> originalFieldsMap) {
        if (StringUtils.isBlank(filterString)) {
            return FilterCompat.NOOP;
        }

        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        ParquetRecordFilterBuilder filterBuilder = new ParquetRecordFilterBuilder(
                tupleDescription, originalFieldsMap);
        TreeVisitor pruner = new ParquetOperatorPruner(
                tupleDescription, originalFieldsMap, SUPPORTED_OPERATORS);
        TreeVisitor bpCharTransformer = new BPCharOperatorTransformer(tupleDescription);

        try {
            // Parse the filter string into a expression tree Node
            Node root = new FilterParser().parse(filterString);
            // Transform IN operators into a chain of ORs, then
            // prune the parsed tree with valid supported operators and then
            // traverse the pruned tree with the ParquetRecordFilterBuilder to
            // produce a record filter for parquet
            TRAVERSER.traverse(root, IN_OPERATOR_TRANSFORMER, pruner, bpCharTransformer, filterBuilder);
            return filterBuilder.getRecordFilter();
        } catch (Exception e) {
            LOG.error(String.format("%s-%d: %s--%s Unable to generate Parquet Record Filter for filter",
                    context.getTransactionId(),
                    context.getSegmentId(),
                    context.getDataSource(),
                    context.getFilterString()), e);
            return FilterCompat.NOOP;
        }
    }

    /**
     * Reads the original schema from the parquet file.
     *
     * @param parquetFile the path to the parquet file
     * @param fileSplit   the file split we are accessing
     * @return the original schema from the parquet file
     * @throws IOException when there's an IOException while reading the schema
     */
    private MessageType getSchema(Path parquetFile, FileSplit fileSplit) throws IOException {

        final long then = System.nanoTime();
        ParquetMetadataConverter.MetadataFilter filter = ParquetMetadataConverter.range(
                fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength());
        ParquetReadOptions parquetReadOptions = HadoopReadOptions
                .builder(configuration)
                .withMetadataFilter(filter)
                .build();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(parquetFile, configuration);
        try (ParquetFileReader parquetFileReader =
                     ParquetFileReader.open(inputFile, parquetReadOptions)) {
            FileMetaData metadata = parquetFileReader.getFileMetaData();
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}-{}: Reading file {} with {} records in {} RowGroups",
                        context.getTransactionId(), context.getSegmentId(),
                        parquetFile.getName(), parquetFileReader.getRecordCount(),
                        parquetFileReader.getRowGroups().size());
            }
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
            LOG.debug("{}-{}: Read schema in {} ms", context.getTransactionId(),
                    context.getSegmentId(), millis);
            return metadata.getSchema();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Builds a map of names to Types from the original schema, the map allows
     * easy access from a given column name to the schema {@link Type}.
     *
     * @param originalSchema the original schema of the parquet file
     * @return a map of field names to types
     */
    private Map<String, Type> getOriginalFieldsMap(MessageType originalSchema) {
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

        return originalFields;
    }

    /**
     * Generates a read schema when there is column projection
     *
     * @param originalFields a map of field names to types
     * @param originalSchema the original read schema
     */
    private MessageType buildReadSchema(Map<String, Type> originalFields, MessageType originalSchema) {
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

    private void createParquetWriter() throws IOException, InterruptedException {

        String fileName = filePrefix + codecName.getExtension() + ".parquet";
        LOG.debug("{}-{}: Creating file {}", context.getTransactionId(),
                context.getSegmentId(), fileName);
        file = new Path(fileName);
        fs = FileSystem.get(URI.create(fileName), configuration);
        HdfsUtilities.validateFile(file, fs);

        configuration.setInt(PAGE_SIZE, pageSize);
        configuration.setInt(DICTIONARY_PAGE_SIZE, dictionarySize);
        configuration.setBoolean(ENABLE_DICTIONARY, enableDictionary);
        configuration.set(WRITER_VERSION, parquetVersion.toString());
        configuration.setLong(BLOCK_SIZE, rowGroupSize);

        recordWriter = new ParquetOutputFormat<>(groupWriteSupport)
                .getRecordWriter(configuration, file, codecName, ParquetFileWriter.Mode.CREATE);
    }

    /**
     * Generate parquet schema using schema file
     */
    private MessageType readSchemaFile(String schemaFile)
            throws IOException {
        LOG.debug("{}-{}: Using parquet schema from given schema file {}", context.getTransactionId(),
                context.getSegmentId(), schemaFile);
        try (InputStream inputStream = fs.open(new Path(schemaFile))) {
            return MessageTypeParser.parseMessageType(IOUtils.toString(inputStream, StandardCharsets.UTF_8));
        }
    }

    /**
     * Generate parquet schema using column descriptors
     */
    private MessageType generateParquetSchema(List<ColumnDescriptor> columns) {
        LOG.debug("{}-{}: Generating parquet schema for write using {}", context.getTransactionId(),
                context.getSegmentId(), columns);
        List<Type> fields = new ArrayList<>();
        for (ColumnDescriptor column : columns) {
            String columnName = column.columnName();
            int columnTypeCode = column.columnTypeCode();

            Types.PrimitiveBuilder<PrimitiveType> primitiveBuilder = null;
            Types.GroupBuilder<GroupType> groupBuilder = null;

            switch (DataType.get(columnTypeCode)) {
                case BOOLEAN:
                    primitiveBuilder = Types.optional(PrimitiveTypeName.BOOLEAN);
                    break;
                case BYTEA:
                    primitiveBuilder = Types.optional(PrimitiveTypeName.BINARY);
                    break;
                case BIGINT:
                    primitiveBuilder = Types.optional(PrimitiveTypeName.INT64);
                    break;
                case SMALLINT:
                    primitiveBuilder = Types.optional(PrimitiveTypeName.INT32)
                            .as(intType(16, true));
                    break;
                case INTEGER:
                    primitiveBuilder = Types.optional(PrimitiveTypeName.INT32);
                    break;
                case REAL:
                    primitiveBuilder = Types.optional(PrimitiveTypeName.FLOAT);
                    break;
                case FLOAT8:
                    primitiveBuilder = Types.optional(PrimitiveTypeName.DOUBLE);
                    break;
                case NUMERIC:
                    Integer[] columnTypeModifiers = column.columnTypeModifiers();
                    int precision = HiveDecimal.SYSTEM_DEFAULT_PRECISION;
                    int scale = HiveDecimal.SYSTEM_DEFAULT_SCALE;

                    if (columnTypeModifiers != null && columnTypeModifiers.length > 1) {
                        precision = columnTypeModifiers[0];
                        scale = columnTypeModifiers[1];
                    }
                    primitiveBuilder = Types
                            .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                            .length(PRECISION_TO_BYTE_COUNT[precision - 1])
                            .as(DecimalLogicalTypeAnnotation.decimalType(scale, precision));
                    break;
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                    primitiveBuilder = Types.optional(PrimitiveTypeName.INT96);
                    break;
                case DATE:
                    // DATE is used to for a logical date type, without a time
                    // of day. It must annotate an int32 that stores the number
                    // of days from the Unix epoch, 1 January 1970. The sort
                    // order used for DATE is signed.
                    primitiveBuilder = Types.optional(PrimitiveTypeName.INT32)
                            .as(dateType());
                    break;
                case TIME:
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                    primitiveBuilder = Types.optional(PrimitiveTypeName.BINARY)
                            .as(stringType());
                    break;
                case BOOLARRAY:
                    groupBuilder = Types.optionalGroup()
                            .repeatedGroup()
                            .optional(PrimitiveTypeName.BOOLEAN).named("element")
                            .named("list");
                    break;
                default:
                    throw new UnsupportedTypeException(
                            String.format("Type %d is not supported", columnTypeCode));
            }

            if(primitiveBuilder != null){
                fields.add(primitiveBuilder.named(columnName));
            }

            if(groupBuilder != null){
                fields.add((Type) groupBuilder.named(columnName));
            }
        }
        return new MessageType("hive_schema", fields);
    }

    /**
     * Returns the {@link CompressionCodecName} for the given name, or default if name is null
     *
     * @param name         the name or class name of the compression codec
     * @param defaultCodec the default codec
     * @return the {@link CompressionCodecName} for the given name, or default if name is null
     */
    private CompressionCodecName getCodecName(String name, CompressionCodecName defaultCodec) {
        if (name == null) return defaultCodec;

        try {
            return CompressionCodecName.fromConf(name);
        } catch (IllegalArgumentException ie) {
            try {
                return CompressionCodecName.fromCompressionCodec(Class.forName(name));
            } catch (ClassNotFoundException ce) {
                throw new IllegalArgumentException(String.format("Invalid codec: %s ", name));
            }
        }
    }
}
