package org.greenplum.pxf.plugins.hdfs.avro;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

@Component
public final class AvroUtilities {

    private static final String COMMON_NAMESPACE = "public.avro";

    private final FileSearcher fileSearcher;
    private String schemaPath;
    private final static Logger LOG = LoggerFactory.getLogger(AvroUtilities.class);
    AvroSchemaFileReaderFactory schemaFileReaderFactory = AvroSchemaFileReaderFactory.getInstance();

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
        try {
            schemaPath = context.getDataSource();
            schema = readOrGenerateAvroSchema(context, hcfsType);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to obtain Avro schema from '%s'", schemaPath), e);
        }
        context.setMetadata(schema);
        return schema;
    }

    private Schema readOrGenerateAvroSchema(RequestContext context, HcfsType hcfsType) throws IOException {
        // user-provided schema trumps everything
        String userProvidedSchemaFile = context.getOption("SCHEMA");
        if (userProvidedSchemaFile != null) {
            schemaPath = userProvidedSchemaFile;
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

    private Schema getFieldSchema(DataType type, String colName) throws IOException {
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
            default:
                unionList.add(Schema.create(Schema.Type.STRING));
                break;
        }

        return Schema.createUnion(unionList);
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
