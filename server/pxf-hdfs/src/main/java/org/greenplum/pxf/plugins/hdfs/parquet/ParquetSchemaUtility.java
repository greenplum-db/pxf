package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Parquet-schema related functionality
 */
public class ParquetSchemaUtility {

    private final RequestContext context;
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    // From org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
    public static final int[] PRECISION_TO_BYTE_COUNT = new int[38];

    static {
        for (int prec = 1; prec <= 38; prec++) {
            // Estimated number of bytes needed.
            PRECISION_TO_BYTE_COUNT[prec - 1] = (int)
                    Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
        }
    }

    public ParquetSchemaUtility(RequestContext context) {
        this.context = context;
    }

    /**
     * Retrieves the original schema from the parquet file split, and returns
     * the read schema for the projected columns in the query.
     *
     * @param fileSplit         the file split we are accessing
     * @param configuration     the hadoop configuration
     * @param columnDescriptors the list of column descriptors
     * @return the read schema for the given query
     * @throws IOException when there's an IOException while reading the schema
     */
    public MessageType getReadSchema(FileSplit fileSplit, Configuration configuration, List<ColumnDescriptor> columnDescriptors) throws IOException {
        // Read the original schema from the parquet file
        MessageType originalSchema = getOriginalSchema(fileSplit, configuration);
        // Get a map of the column name to Types for the given schema
        Map<String, Type> originalFieldsMap = getOriginalFieldsMap(originalSchema);
        // Get the read schema in case of column projection
        return buildReadSchema(originalFieldsMap, originalSchema, columnDescriptors);
    }

    /**
     * Reads the original schema from the parquet file.
     *
     * @param fileSplit     the file split we are accessing
     * @param configuration the hadoop configuration
     * @return the original schema from the parquet file
     * @throws IOException when there's an IOException while reading the schema
     */
    public MessageType getOriginalSchema(FileSplit fileSplit, Configuration configuration) throws IOException {

        final long then = System.nanoTime();
        ParquetMetadataConverter.MetadataFilter filter = ParquetMetadataConverter.range(
                fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength());
        ParquetReadOptions parquetReadOptions = HadoopReadOptions
                .builder(configuration)
                .withMetadataFilter(filter)
                .build();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(fileSplit.getPath(), configuration);
        try (ParquetFileReader parquetFileReader =
                     ParquetFileReader.open(inputFile, parquetReadOptions)) {
            FileMetaData metadata = parquetFileReader.getFileMetaData();
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}-{}: Reading file {} with {} records in {} RowGroups",
                        context.getTransactionId(), context.getSegmentId(),
                        fileSplit.getPath().getName(), parquetFileReader.getRecordCount(),
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
     * @param schema the original schema of the parquet file
     * @return a map of field names to types
     */
    public Map<String, Type> getOriginalFieldsMap(MessageType schema) {
        Map<String, Type> originalFields = new HashMap<>(schema.getFieldCount() * 2);

        // We need to add the original name and lower cased name to
        // the map to support mixed case where in GPDB the column name
        // was created with quotes i.e "mIxEd CaSe". When quotes are not
        // used to create a table in GPDB, the name of the column will
        // always come in lower-case
        schema.getFields().forEach(t -> {
            String columnName = t.getName();
            originalFields.put(columnName, t);
            originalFields.put(columnName.toLowerCase(), t);
        });

        return originalFields;
    }

    /**
     * Generates a read schema when there is column projection
     *
     * @param originalFields    a map of field names to types
     * @param originalSchema    the original read schema
     * @param columnDescriptors the list of column descriptors
     */
    public MessageType buildReadSchema(Map<String, Type> originalFields, MessageType originalSchema, List<ColumnDescriptor> columnDescriptors) {
        List<Type> projectedFields = columnDescriptors.stream()
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

    /**
     * Generate parquet schema using column descriptors
     */
    public MessageType generateParquetSchema(List<ColumnDescriptor> columns) {
        LOG.debug("{}-{}: Generating parquet schema for write using {}", context.getTransactionId(),
                context.getSegmentId(), columns);
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
                    Integer[] columnTypeModifiers = column.columnTypeModifiers();
                    int precision = HiveDecimal.SYSTEM_DEFAULT_PRECISION;
                    int scale = HiveDecimal.SYSTEM_DEFAULT_SCALE;

                    if (columnTypeModifiers != null && columnTypeModifiers.length > 1) {
                        precision = columnTypeModifiers[0];
                        scale = columnTypeModifiers[1];
                    }
                    length = PRECISION_TO_BYTE_COUNT[precision - 1];
                    dmt = new DecimalMetadata(precision, scale);
                    break;
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
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
                    throw new UnsupportedTypeException(
                            String.format("Type %d is not supported", columnTypeCode));
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
