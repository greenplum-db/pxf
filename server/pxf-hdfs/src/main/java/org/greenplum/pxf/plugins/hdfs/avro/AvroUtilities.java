package org.greenplum.pxf.plugins.hdfs.avro;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public final class AvroUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(AvroUtilities.class);
    private static final String COMMON_NAMESPACE = "public.avro";

    private AvroSchemaFileReaderFactory schemaFileReaderFactory;
    private final FileSearcher fileSearcher;
    private PgUtilities pgUtilities;

    public interface FileSearcher {
        File searchForFile(String filename);
    }

    /**
     * default constructor
     */
    public AvroUtilities() {
        this(new DefaultFileSearcher());
    }

    // constructor for use in test
    @VisibleForTesting
    AvroUtilities(FileSearcher fileSearcher) {
        this.fileSearcher = fileSearcher;
    }

    @Autowired
    public void setSchemaFileReaderFactory(AvroSchemaFileReaderFactory schemaFileReaderFactory) {
        this.schemaFileReaderFactory = schemaFileReaderFactory;
    }

    @Autowired
    public void setPgUtilities(PgUtilities pgUtilities) {
        this.pgUtilities = pgUtilities;
    }

    /**
     * All-purpose method for obtaining an Avro schema based on the request context and
     * HCFS config.
     *
     * @param context  the context for the request
     * @param hcfsType the type of hadoop-compatible filesystem we are accessing
     * @return the avro schema
     */
    public Schema obtainSchema(RequestContext context, HcfsType hcfsType) {
        Schema schema = (Schema) context.getMetadata();

        if (schema != null) {
            return schema;
        }

        String userProvidedSchema = context.getOption("SCHEMA");
        String schemaFile = userProvidedSchema != null ? userProvidedSchema : context.getDataSource();

        try {
            schema = readOrGenerateAvroSchema(context, hcfsType, userProvidedSchema);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to obtain Avro schema from '%s'", schemaFile), e);
        }
        context.setMetadata(schema);
        return schema;
    }

    /**
     * Parse a Postgres external format into a given Avro schema
     *
     * Re-used from GPHDFS
     * https://github.com/greenplum-db/gpdb/blob/3b0bfdc169fab7f686276be7eccb024a5e29543c/gpAux/extensions/gphdfs/src/java/1.2/com/emc/greenplum/gpdb/hadoop/formathandler/util/FormatHandlerUtil.java
     * @param schema target Avro schema
     * @param value Postgres external format (the output of function named by typoutput in pg_type) or `null` if null value
     * @param isTopLevel
     * @return
     */
    public Object decodeString(Schema schema, String value, boolean isTopLevel, boolean hasUserProvidedSchema) {
        LOG.trace("schema={}, value={}, isTopLevel={}", schema, value, isTopLevel);

        Schema.Type fieldType = schema.getType();
        if (fieldType == Schema.Type.ARRAY) {
            if (value == null) {
                return null;
            }

            List<Object> list = new ArrayList<>();
            String[] splits = pgUtilities.splitArray(value);
            Schema elementType = schema.getElementType();
            for (String split : splits) {
                try {
                    list.add(decodeString(elementType, split, false, hasUserProvidedSchema));
                } catch (NumberFormatException | PxfRuntimeException e) {
                    String hint = "";
                    if (StringUtils.startsWith(split, "{")) {
                        hint = hasUserProvidedSchema ?
                                "Value is a multi-dimensional array, please check that the provided AVRO schema has the correct dimensions." :
                                "Value is a multi-dimensional array, user is required to provide an AVRO schema with matching dimensions.";

                    } else {
                        hint = hasUserProvidedSchema ?
                                "Check that the AVRO and GPDB schemas are correct." :
                                "Unexpected state since PXF generated the AVRO schema.";
                    }
                   throw new PxfRuntimeException(String.format("Error parsing array element: %s was not of expected type %s", split, elementType), hint, e);
                }
            }
            return list;
        } else {
            if (fieldType == Schema.Type.UNION) {
                schema = firstNotNullSchema(schema.getTypes());

                fieldType = schema.getType();
                if (fieldType == Schema.Type.ARRAY) {
                    return decodeString(schema, value, isTopLevel, hasUserProvidedSchema);
                }
            }
            if (value == null && !isTopLevel) {
                return null;
            }

            switch (fieldType) {
                case INT:
                    return Integer.parseInt(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case STRING:
                case RECORD:
                case ENUM:
                case MAP:
                    return value;
                case FLOAT:
                    return Float.parseFloat(value);
                case LONG:
                    return Long.parseLong(value);
                case BYTES:
                    return pgUtilities.parseByteaLiteral(value);
                case BOOLEAN:
                    return pgUtilities.parseBoolLiteral(value);
                default:
                    throw new PxfRuntimeException(String.format("type: %s is not supported", fieldType));
            }
        }
    }

    private Schema readOrGenerateAvroSchema(RequestContext context, HcfsType hcfsType, String userProvidedSchemaFile) throws IOException {
        // user-provided schema trumps everything
        if (userProvidedSchemaFile != null) {
            AvroSchemaFileReader schemaFileReader = schemaFileReaderFactory.getAvroSchemaFileReader(userProvidedSchemaFile);
            return schemaFileReader.readSchema(context.getConfiguration(), userProvidedSchemaFile, hcfsType, fileSearcher);
        }

        // if we are writing we must generate the schema since there is none to read
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            return generateSchema(context.getTupleDescription());
        }

        // reading from external: get the schema from data source
        return readSchemaFromAvroDataSource(context.getConfiguration(), context.getDataSource());
    }

    private Schema readSchemaFromAvroDataSource(Configuration configuration, String dataSource) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        FsInput inStream = new FsInput(new Path(dataSource), configuration);

        try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(inStream, datumReader)) {
            return fileReader.getSchema();
        }
    }

    private Schema generateSchema(List<ColumnDescriptor> tupleDescription) throws IOException {
        Schema schema = Schema.createRecord("tableName", "", COMMON_NAMESPACE, false);
        List<Schema.Field> fields = new ArrayList<>();

        for (ColumnDescriptor cd : tupleDescription) {
            fields.add(new Schema.Field(
                    cd.columnName(),
                    getFieldSchema(DataType.get(cd.columnTypeCode()), cd.columnName()),
                    "",
                    null
            ));
        }

        schema.setFields(fields);

        return schema;
    }

    private Schema getFieldSchema(DataType type, String colName) {
        List<Schema> unionList = new ArrayList<>();
        // in this version of gpdb, external table should not set 'notnull' attribute
        // so we should use union between NULL and another type everywhere
        unionList.add(Schema.create(Schema.Type.NULL));

        switch (type) {
            case BOOLEAN:
                unionList.add(Schema.create(Schema.Type.BOOLEAN));
                break;
            case BYTEA:
                unionList.add(Schema.create(Schema.Type.BYTES));
                break;
            case BIGINT:
                unionList.add(Schema.create(Schema.Type.LONG));
                break;
            case SMALLINT:
            case INTEGER:
                unionList.add(Schema.create(Schema.Type.INT));
                break;
            case REAL:
                unionList.add(Schema.create(Schema.Type.FLOAT));
                break;
            case FLOAT8:
                unionList.add(Schema.create(Schema.Type.DOUBLE));
                break;
            case BOOLARRAY:
                unionList.add(createArraySchema(Schema.Type.BOOLEAN));
                break;
            case BYTEAARRAY:
                unionList.add(createArraySchema(Schema.Type.BYTES));
                break;
            case INT2ARRAY:
            case INT4ARRAY:
                unionList.add(createArraySchema(Schema.Type.INT));
                break;
            case INT8ARRAY:
                unionList.add(createArraySchema(Schema.Type.LONG));
                break;
            case FLOAT4ARRAY:
                unionList.add(createArraySchema(Schema.Type.FLOAT));
                break;
            case FLOAT8ARRAY:
                unionList.add(createArraySchema(Schema.Type.DOUBLE));
                break;
            case TEXTARRAY:
                unionList.add(createArraySchema(Schema.Type.STRING));
                break;
            default:
                unionList.add(Schema.create(Schema.Type.STRING));
                break;
        }

        return Schema.createUnion(unionList);
    }

    /**
     * Helper method for creating a valid Avro schema for an array of the given element type.
     * @param arrayElemType Avro type of array elements
     * @return Avro schema for array with elements of given type
     */
    private Schema createArraySchema(Schema.Type arrayElemType) {
        return Schema.createArray(Schema.createUnion(
                Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(arrayElemType))));
    }

    /**
     * Finds the first Avro schema type that is not "null" in the list of schema types
     * @param types list of Avro union schema types
     * @return the first Avro schema type that is not "null"
     */
    Schema firstNotNullSchema(List<Schema> types) {
        for (Schema schema : types) {
            if (schema.getType() != Schema.Type.NULL) {
                return schema;
            }
        }

        throw new PxfRuntimeException("Avro union schema only contains null types");
    }

    private static class DefaultFileSearcher implements FileSearcher {

        @Override
        public File searchForFile(String schemaName) {
            try {
                File file = new File(schemaName);
                if (!file.exists()) {
                    URL url = AvroUtilities.class.getClassLoader().getResource(schemaName);

                    // Testing that the schema resource exists
                    if (url == null) {
                        return null;
                    }
                    file = new File(URLDecoder.decode(url.getPath(), "UTF-8"));
                }
                return file;
            } catch (UnsupportedEncodingException e) {
                LOG.info(e.toString());
                return null;
            }
        }
    }
}
