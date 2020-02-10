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
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetRecordFilterBuilder;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetSchemaUtility;
import org.greenplum.pxf.plugins.hdfs.parquet.SupportedParquetPrimitiveTypePruner;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;

/**
 * Parquet file accessor.
 * Unit of operation is record.
 */
public class ParquetFileAccessor extends BasePlugin implements Accessor {

    private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
    private static final int DEFAULT_FILE_SIZE = 128 * 1024 * 1024;
    private static final int DEFAULT_ROWGROUP_SIZE = 8 * 1024 * 1024;
    private static final int DEFAULT_DICTIONARY_PAGE_SIZE = 512 * 1024;
    private static final WriterVersion DEFAULT_PARQUET_VERSION = WriterVersion.PARQUET_1_0;
    private static final CompressionCodecName DEFAULT_COMPRESSION = CompressionCodecName.SNAPPY;

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

    private ParquetReader<Group> fileReader;
    private CompressionCodecName codecName;
    private ParquetWriter<Group> parquetWriter;
    private GroupWriteSupport groupWriteSupport;
    private FileSystem fs;
    private Path file;
    private String filePrefix;
    private int fileIndex, pageSize, rowGroupSize, dictionarySize;
    private long rowsRead, rowsWritten, totalRowsRead, totalRowsWritten;
    private WriterVersion parquetVersion;
    private CodecFactory codecFactory = CodecFactory.getInstance();
    private long totalReadTimeInNanos;
    private ParquetSchemaUtility parquetSchemaUtility;

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
        this.parquetSchemaUtility = new ParquetSchemaUtility(context);
    }

    /**
     * Opens the resource for read.
     *
     * @throws IOException if opening the resource failed
     */
    @Override
    public boolean openForRead() throws IOException {
        file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);

        LOG.debug("{}-{}: Get read schema from parquet file footer", context.getTransactionId(), context.getSegmentId());
        MessageType readSchema = parquetSchemaUtility.getReadSchema(fileSplit, configuration, context.getTupleDescription());

        // Get a map of the column name to Types for the given schema
        Map<String, Type> originalFieldsMap = parquetSchemaUtility.getOriginalFieldsMap(readSchema);
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

        if (LOG.isDebugEnabled()) {
            final long millis = TimeUnit.NANOSECONDS.toMillis(totalReadTimeInNanos);
            long average = totalReadTimeInNanos / totalRowsRead;
            LOG.debug("{}-{}: Read TOTAL of {} rows from file {} on server {} in {} ms. Average speed: {} nanoseconds",
                    context.getTransactionId(),
                    context.getSegmentId(),
                    totalRowsRead,
                    context.getDataSource(),
                    context.getServerName(),
                    millis,
                    average);
        }
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
        pageSize = context.getOption("PAGE_SIZE", DEFAULT_PAGE_SIZE);
        rowGroupSize = context.getOption("ROWGROUP_SIZE", DEFAULT_ROWGROUP_SIZE);
        dictionarySize = context.getOption("DICTIONARY_PAGE_SIZE", DEFAULT_DICTIONARY_PAGE_SIZE);
        String parquetVerStr = context.getOption("PARQUET_VERSION");
        parquetVersion = parquetVerStr != null ? WriterVersion.fromString(parquetVerStr.toLowerCase()) : DEFAULT_PARQUET_VERSION;
        LOG.debug("{}-{}: Parquet options: PAGE_SIZE = {}, ROWGROUP_SIZE = {}, DICTIONARY_PAGE_SIZE = {}, PARQUET_VERSION = {}",
                context.getTransactionId(), context.getSegmentId(), pageSize, rowGroupSize, dictionarySize, parquetVersion);

        // Read schema file, if given
        String schemaFile = context.getOption("SCHEMA");
        MessageType schema = (schemaFile != null) ? readSchemaFile(schemaFile) :
                parquetSchemaUtility.generateParquetSchema(context.getTupleDescription());
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

        ParquetRecordFilterBuilder filterBuilder = new ParquetRecordFilterBuilder(
                context.getTupleDescription(), originalFieldsMap);
        TreeVisitor pruner = new SupportedParquetPrimitiveTypePruner(
                context.getTupleDescription(), originalFieldsMap, SUPPORTED_OPERATORS);

        try {
            // Parse the filter string into a expression tree Node
            Node root = new FilterParser().parse(filterString);
            // Prune the parsed tree with valid supported operators and then
            // traverse the pruned tree with the ParquetRecordFilterBuilder to
            // produce a record filter for parquet
            TRAVERSER.traverse(root, pruner, filterBuilder);
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

    private void createParquetWriter() throws IOException {

        String fileName = filePrefix + "." + fileIndex;
        fileName += codecName.getExtension() + ".parquet";
        LOG.debug("{}-{}: Creating file {}", context.getTransactionId(),
                context.getSegmentId(), fileName);
        file = new Path(fileName);
        fs = FileSystem.get(URI.create(fileName), configuration);
        HdfsUtilities.validateFile(file, fs);

        //noinspection deprecation
        parquetWriter = new ParquetWriter<>(file, groupWriteSupport, codecName,
                rowGroupSize, pageSize, dictionarySize,
                true, false, parquetVersion, configuration);
    }

    /**
     * Generate parquet schema using schema file
     */
    private MessageType readSchemaFile(String schemaFile)
            throws IOException {
        LOG.debug("{}-{}: Using parquet schema from given schema file {}", context.getTransactionId(),
                context.getSegmentId(), schemaFile);
        try (InputStream inputStream = fs.open(new Path(schemaFile))) {
            return MessageTypeParser.parseMessageType(IOUtils.toString(inputStream));
        }
    }
}
