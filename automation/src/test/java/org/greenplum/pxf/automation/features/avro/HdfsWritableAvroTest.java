package org.greenplum.pxf.automation.features.avro;

import annotations.FailsWithFDW;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

@FailsWithFDW
public class HdfsWritableAvroTest extends BaseFeature {

    private ReadableExternalTable readableExternalTable;
    private ArrayList<File> filesToDelete;
    private static final String[] AVRO_PRIMITIVE_WRITABLE_TABLE_COLS = new String[]{
            "type_int         int",
            "type_smallint    smallint", // smallint
            "type_long        bigint",
            "type_float       real",
            "type_double      float8",
            "type_string      text",
            "type_bytes       bytea",
            "type_boolean     bool",
            "type_char        character(20)",
            "type_varchar     varchar(32)"
    };
    // values that were written from a smallint column (see above type_smallint)
    // must be read back into integer columns
    private static final String[] AVRO_PRIMITIVE_READABLE_TABLE_COLS = new String[]{
            "type_int        int",
            "type_smallint   int", // int
            "type_long       bigint",
            "type_float      real",
            "type_double     float8",
            "type_string     text",
            "type_bytes      bytea",
            "type_boolean    bool",
            "type_char       character(20)",
            "type_varchar    varchar(32)"
    };
    private static final String[] AVRO_COMPLEX_TABLE_COLS_WRITABLE = new String[]{
            "type_int             INT",
            "type_record          STRUCT", // see createComplexTypes()
            "type_enum_mood       MOOD", // see createComplexTypes()
            "type_long_array      BIGINT[]",
            "type_numeric_array   NUMERIC(8,1)[]",
            "type_string_array    TEXT[]",
            "type_date            DATE",
            "type_time            TIME",
            "type_timestamp       TIMESTAMP",
            "type_timestampz      TIMESTAMP WITH TIME ZONE"
    };
    // before pxf 6, we did not support arrays and all complex types were written as text. keep this behavior here
    private static final String[] AVRO_COMPLEX_TABLE_COLS_AS_TEXT_READABLE = new String[]{
            "type_int             INT",
            "type_record          TEXT",
            "type_enum_mood       TEXT",
            "type_long_array      TEXT",
            "type_numeric_array   TEXT",
            "type_string_array    TEXT",
            "type_date            TEXT",
            "type_time            TEXT",
            "type_timestamp       TEXT",
            "type_timestampz      TEXT"
    };
    // pxf autogenerates a schema with arrays for type long and string. read that back appropriately using this schema
    private static final String[] AVRO_COMPLEX_TABLE_COLS_W_ARRAYS_READABLE = new String[]{
            "type_int             INT",
            "type_record          TEXT",
            "type_enum_mood       TEXT",
            "type_long_array      BIGINT[]",
            "type_numeric_array   TEXT",
            "type_string_array    TEXT[]",
            "type_date            TEXT",
            "type_time            TEXT",
            "type_timestamp       TEXT",
            "type_timestampz      TEXT"
    };
    private static final String[] AVRO_ARRAY_TABLE_COLS_WRITABLE = new String[]{
            "type_int             INT",
            "type_long_array      BIGINT[]",
            "type_numeric_array   NUMERIC(8,1)[]",
            "type_string_array    TEXT[]"
    };
    private static final String[] AVRO_ARRAY_TABLE_COLS_READABLE = new String[]{
            "type_int             INT",
            "type_long_array      BIGINT[]",
            "type_numeric_array   TEXT[]",
            "type_string_array    TEXT[]"
    };
    private String gpdbTable;
    private String hdfsPath;
    private String publicStage;
    private String resourcePath;
    private String fullTestPath;
    private ProtocolEnum protocol;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/writableAvro/";
        String absolutePath = Objects.requireNonNull(getClass().getClassLoader().getResource("data")).getPath();
        resourcePath = absolutePath + "/avro/";

        protocol = ProtocolUtils.getProtocol();
    }

    @Override
    public void beforeMethod() throws Exception {
        filesToDelete = new ArrayList<>();
        publicStage = "/tmp/publicstage/pxf/";
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void generateSchemaPrimitive() throws Exception {
        gpdbTable = "writable_avro_primitive_generate_schema";
        fullTestPath = hdfsPath + "generate_schema_primitive_types";
        prepareWritableExternalTable(gpdbTable, AVRO_PRIMITIVE_WRITABLE_TABLE_COLS, fullTestPath);
        exTable.setUserParameters(new String[]{"COMPRESSION_CODEC=xz"});

        prepareReadableExternalTable(gpdbTable, AVRO_PRIMITIVE_READABLE_TABLE_COLS, fullTestPath);

        gpdb.createTableAndVerify(readableExternalTable);
        gpdb.createTableAndVerify(exTable);

        insertPrimitives(gpdbTable);

        publicStage += "generateSchemaPrimitive/";
        // fetch all the segment-generated avro files and make them into json records
        // confirm that the lines generated by the segments match what we expect
        fetchAndVerifyAvroHcfsFiles("primitives.json", "xz");

        // check using GPDB readable external table that what went into HCFS is correct
        runTincTest("pxf.features.hdfs.writable.avro.primitives_generate_schema.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void generateSchemaPrimitive_withNoCompression() throws Exception {
        gpdbTable = "writable_avro_primitive_no_compression";
        fullTestPath = hdfsPath + "generate_schema_primitive_types_with_no_compression";
        prepareWritableExternalTable(gpdbTable, AVRO_PRIMITIVE_WRITABLE_TABLE_COLS, fullTestPath);
        exTable.setUserParameters(new String[]{"COMPRESSION_CODEC=uncompressed"});

        prepareReadableExternalTable(gpdbTable, AVRO_PRIMITIVE_READABLE_TABLE_COLS, fullTestPath);

        gpdb.createTableAndVerify(readableExternalTable);
        gpdb.createTableAndVerify(exTable);

        insertPrimitives(gpdbTable);

        publicStage += "generateSchemaPrimitive_withNoCompression/";
        // fetch all the segment-generated avro files and make them into json records
        // confirm that the lines generated by the segments match what we expect
        fetchAndVerifyAvroHcfsFiles("primitives.json", "null");

        // check using GPDB readable external table that what went into HCFS is correct
        runTincTest("pxf.features.hdfs.writable.avro.primitives_generate_schema_with_no_compression.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void generateSchemaComplex() throws Exception {
        gpdbTable = "writable_avro_complex_generate_schema";
        createComplexTypes();
        fullTestPath = hdfsPath + "generate_schema_complex_types";
        prepareWritableExternalTable(gpdbTable, AVRO_COMPLEX_TABLE_COLS_WRITABLE, fullTestPath);
        exTable.setUserParameters(new String[]{"COMPRESSION_CODEC=bzip2"});

        prepareReadableExternalTable(gpdbTable, AVRO_COMPLEX_TABLE_COLS_W_ARRAYS_READABLE, fullTestPath);

        gpdb.createTableAndVerify(readableExternalTable);
        gpdb.createTableAndVerify(exTable);

        insertComplex(gpdbTable);

        publicStage += "generateSchemaComplex/";
        // fetch all the segment-generated avro files and make them into json records
        // confirm that the lines generated by the segments match what we expect
        fetchAndVerifyAvroHcfsFiles("complex_records.json", "bzip2");

        // check using GPDB readable external table that what went into HCFS is correct
        runTincTest("pxf.features.hdfs.writable.avro.complex_generate_schema.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void userProvidedSchemaFileOnHcfsPrimitive() throws Exception {
        gpdbTable = "writable_avro_primitive_user_provided_schema_on_hcfs";
        fullTestPath = hdfsPath + "primitive_user_provided_schema_on_hcfs";
        prepareWritableExternalTable(gpdbTable, AVRO_PRIMITIVE_WRITABLE_TABLE_COLS, fullTestPath);
        exTable.setUserParameters(new String[]{"COMPRESSION_CODEC=snappy"});

        prepareReadableExternalTable(gpdbTable, AVRO_PRIMITIVE_READABLE_TABLE_COLS, fullTestPath);
        gpdb.createTableAndVerify(readableExternalTable);

        String schemaPath = hdfsPath.replaceFirst("/$", "_schema/primitives_no_union.avsc");
        // copy a schema file to HCFS that has no UNION types, just the raw underlying types.
        // the Avro files should thus be different from those without user-provided schema
        hdfs.copyFromLocal(resourcePath + "primitives_no_union.avsc", schemaPath);

        schemaPath = "/" + schemaPath;
        exTable.setExternalDataSchema(schemaPath);
        gpdb.createTableAndVerify(exTable);

        insertPrimitives(gpdbTable);

        publicStage += "userProvidedSchemaFileOnHcfsPrimitive/";
        // fetch all the segment-generated avro files and make them into json records
        // confirm that the lines generated by the segments match what we expect
        fetchAndVerifyAvroHcfsFiles("primitives_no_union.json", "snappy");

        // check using GPDB readable external table that what went into HCFS is correct
        runTincTest("pxf.features.hdfs.writable.avro.primitives_user_provided_schema_on_hcfs.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void userProvidedSchemaFileOnClasspathComplexTypesAsText() throws Exception {
        createComplexTypes();
        gpdbTable = "writable_avro_complex_user_schema_on_classpath";
        fullTestPath = hdfsPath + "complex_user_schema_on_classpath";
        prepareWritableExternalTable(gpdbTable,
                AVRO_COMPLEX_TABLE_COLS_WRITABLE,
                fullTestPath);

        prepareReadableExternalTable(gpdbTable,
                AVRO_COMPLEX_TABLE_COLS_AS_TEXT_READABLE,
                fullTestPath);
        gpdb.createTableAndVerify(readableExternalTable);

        // copy a schema file to PXF's classpath on cluster that has no UNION types, just the raw underlying types.
        // the Avro files should thus be different from those without user-provided schema
        // this file is generated using Avro tools: http://avro.apache.org/releases.html
        // get current schema:
        // java -jar avro-tools-1.9.1.jar getschema automation/src/test/resources/data/avro/complex_no_union.avro > my_schema.avsc
        // edit my_schema.avsc...
        // we don't need any rows of data so /dev/null is fine, e.g.:
        // java -jar avro-tools-1.9.1.jar fromjson --schema-file my_schema.avsc /dev/null > automation/src/test/resources/data/avro/complex_no_union.avro
        cluster.copyFileToNodes(new File(resourcePath + "complex_no_union.avro").getAbsolutePath(),
                cluster.getPxfConfLocation(),
                false, false);
        exTable.setExternalDataSchema("complex_no_union.avro");
        gpdb.createTableAndVerify(exTable);

        insertComplex(gpdbTable);

        publicStage += "userProvidedSchemaFileOnClasspathComplex/";
        // fetch all the segment-generated avro files and make them into json records
        // confirm that the lines generated by the segments match what we expect
        fetchAndVerifyAvroHcfsFiles("complex_no_union.json", "deflate");

        // check using GPDB readable external table that what went into HCFS is correct
        runTincTest("pxf.features.hdfs.writable.avro.complex_user_provided_schema_on_classpath.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void userProvidedSchemaFileGPDBArraysAsAvroArraysWithNulls() throws Exception {
        gpdbTable = "writable_avro_array_user_schema_w_nulls";
        fullTestPath = hdfsPath + "array_user_schema_w_nulls";
        prepareWritableExternalTable(gpdbTable,
                AVRO_ARRAY_TABLE_COLS_WRITABLE,
                fullTestPath);

        prepareReadableExternalTable(gpdbTable,
                AVRO_ARRAY_TABLE_COLS_READABLE,
                fullTestPath);
        gpdb.createTableAndVerify(readableExternalTable);

        cluster.copyFileToNodes(new File(resourcePath + "array_with_nulls.avsc").getAbsolutePath(),
                cluster.getPxfConfLocation(),
                false, false);
        exTable.setExternalDataSchema("array_with_nulls.avsc");
        gpdb.createTableAndVerify(exTable);

        insertComplexNullArrays(gpdbTable);

        publicStage += "userProvidedSchemaArrayWithNullsComplex/";
        // fetch all the segment-generated avro files and make them into json records
        // confirm that the lines generated by the segments match what we expect
        fetchAndVerifyAvroHcfsFiles("array_with_nulls.json", "deflate");

        // check using GPDB readable external table that what went into HCFS is correct
        runTincTest("pxf.features.hdfs.writable.avro.array_user_schema_w_nulls.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void generateSchemaWithNullValuesComplex() throws Exception {
        gpdbTable = "writable_avro_null_values";
        createComplexTypes();
        fullTestPath = hdfsPath + "null_values";
        prepareWritableExternalTable(gpdbTable, AVRO_COMPLEX_TABLE_COLS_WRITABLE, fullTestPath);

        prepareReadableExternalTable(gpdbTable, AVRO_COMPLEX_TABLE_COLS_W_ARRAYS_READABLE, fullTestPath);

        gpdb.createTableAndVerify(readableExternalTable);
        gpdb.createTableAndVerify(exTable);

        insertComplexWithNulls(gpdbTable);

        publicStage += "nullValues/";
        // fetch all the segment-generated avro files and make them into json records
        // confirm that the lines generated by the segments match what we expect
        fetchAndVerifyAvroHcfsFiles("null_values.json", "deflate");

        // check using GPDB readable external table that what went into HCFS is correct
        runTincTest("pxf.features.hdfs.writable.avro.null_values.runTest");
    }

    @Override
    protected void afterMethod() throws Exception {
        super.afterMethod();
        if (ProtocolUtils.getPxfTestKeepData().equals("true")) {
            return;
        }
        if (filesToDelete != null) {
            for (File file : filesToDelete) {
                if (!file.delete()) {
                    ReportUtils.startLevel(null, getClass(), String.format("Problem deleting file '%s'", file));
                }
            }
        }
        dropComplexTypes();
    }

    private void createComplexTypes() throws Exception {
        dropComplexTypes();
        gpdb.runQuery("CREATE TYPE mood AS ENUM ('sad', 'happy')");
        gpdb.runQuery("CREATE TYPE struct AS (b boolean, i int)");
    }

    private void dropComplexTypes() throws Exception {
        if (gpdb != null) {
            gpdb.runQuery("DROP TYPE IF EXISTS struct CASCADE", true, false);
            gpdb.runQuery("DROP TYPE IF EXISTS mood CASCADE", true, false);
        }
    }

    private void insertPrimitives(String exTable) throws Exception {
        gpdb.runQuery("INSERT INTO " + exTable + "_writable " + "SELECT " +
                "i, " +                                             // type_int
                "i, " +                                             // type_smallint
                "i*100000000000, " +                                // type_long
                "i+1.0001, " +                                      // type_float
                "i*100000.0001, " +                                 // type_double
                "'row_' || i::varchar(255), " +                     // type_string
                "('bytes for ' || i::varchar(255))::bytea, " +      // type_bytes
                "CASE WHEN (i%2) = 0 THEN TRUE ELSE FALSE END, " +  // type_boolean
                "'character row ' || i::char(3)," +                 // type_character
                "'character varying row ' || i::varchar(3)" +       // type_varchar
                "from generate_series(1, 100) s(i);");
    }

    private void insertComplex(String gpdbTable) throws Exception {
        gpdb.runQuery("SET TIMEZONE=-7;" + "INSERT INTO " + gpdbTable + "_writable " + " SELECT " +
                "i, " +
                "('(' || CASE WHEN (i%2) = 0 THEN FALSE ELSE TRUE END || ',' || (i*2)::VARCHAR(255) || ')')::struct, " +
                "CASE WHEN (i%2) = 0 THEN 'sad' ELSE 'happy' END::mood," +
                "('{' || i::VARCHAR(255) || ',' || (i*10)::VARCHAR(255) || ',' || (i*100)::VARCHAR(255) || '}')::BIGINT[], " +
                "('{' || (i*1.0001)::VARCHAR(255) || ',' || ((i*10.00001)*10)::VARCHAR(255) || ',' || ((i*100.000001)*100)::VARCHAR(255) || '}')::NUMERIC(8,1)[], " +
                "('{\"item ' || ((i-1)*10)::VARCHAR(255) || '\",\"item ' || (i*10)::VARCHAR(255) || '\",\"item ' || ((i+1)*10)::VARCHAR(255) || '\"}')::TEXT[], " +
                "DATE '2001-09-28' + i, " +
                "TIME '00:00:00' + (i::VARCHAR(255) || ' seconds')::interval, " +
                "TIMESTAMP '2001-09-28 01:00' + (i::VARCHAR(255) || ' days')::interval + (i::VARCHAR(255) || ' hours')::interval, " +
                "TIMESTAMP WITH TIME ZONE '2001-09-28 01:00-07' + (i::VARCHAR(255) || ' days')::interval + (i::VARCHAR(255) || ' hours')::interval " +
                "FROM generate_series(1, 100) s(i);");
    }

    private void insertComplexWithNulls(String gpdbTable) throws Exception {
        gpdb.runQuery("SET TIMEZONE=-7;" + "INSERT INTO " + gpdbTable + "_writable " + " SELECT " +
                "i, " +
                "('(' || CASE WHEN (i%2) = 0 THEN FALSE ELSE TRUE END || ', ' || (i*2)::VARCHAR(255) || ')')::struct, " +
                "CASE WHEN (i%3) = 0 THEN 'sad' WHEN (i%2) = 0 THEN 'happy' ELSE NULL END::mood, " +
                "('{' || i::VARCHAR(255) || ',' || (i*10)::VARCHAR(255) || ',' || (i*100)::VARCHAR(255) || '}')::BIGINT[], " +
                "('{' || (i*1.0001)::VARCHAR(255) || ',' || ((i*10.00001)*10)::VARCHAR(255) || ',' || ((i*100.000001)*100)::VARCHAR(255) || '}')::NUMERIC(8,1)[], " +
                "('{\"item ' || ((i-1)*10)::VARCHAR(255) || '\",\"item ' || (i*10)::VARCHAR(255) || '\",\"item ' || ((i+1)*10)::VARCHAR(255) || '\"}')::TEXT[], " +
                "DATE '2001-09-28' + i, " +
                "TIME '00:00:00' + (i::VARCHAR(255) || ' seconds')::interval, " +
                "TIMESTAMP '2001-09-28 01:00' + (i::VARCHAR(255) || ' days')::interval + (i::VARCHAR(255) || ' hours')::interval, " +
                "TIMESTAMP WITH TIME ZONE '2001-09-28 01:00-07' + (i::VARCHAR(255) || ' days')::interval + (i::VARCHAR(255) || ' hours')::interval " +
                "FROM generate_series(1, 100) s(i);");
    }

    private void
    insertComplexNullArrays(String gpdbTable) throws Exception {
        gpdb.runQuery("INSERT INTO " + gpdbTable + "_writable " + " VALUES " +
                "(1, NULL,            '{1.0001,10.00001,100.000001}',  '{\"item 0\",\"item 10\",\"item 20\"}')," +
                "(2, '{2,20,200}',    NULL,                            '{\"item 0\",\"item 10\",\"item 20\"}')," +
                "(3, '{3,30,300}',    '{3.0001,30.00001,300.000001}',  NULL                                  )," +
                "(4, '{NULL,40,400}', '{4.0001,40.00001,400.000001}',  '{\"item 0\",\"item 10\",\"item 20\"}')," +
                "(5, '{5,50,500}',    '{5.0001,NULL,500.000001}',      '{\"item 0\",\"item 10\",\"item 20\"}')," +
                "(6, '{6,60,600}',    '{6.0001,60.00001,600.000001}',  '{\"item 0\",\"item 10\",NULL}'       )," +
                "(7, '{7,70,700}',    '{7.0001,70.00001,700.000001}',  '{\"item 0\",\"item 10\",\"item 20\"}');"
        );
    }

    private void fetchAndVerifyAvroHcfsFiles(String compareFile, String codec) throws Exception {
        int cnt = 0;
        Map<Integer, JsonNode> jsonFromHdfs = new HashMap<>();
        Map<Integer, JsonNode> jsonToCompare = new HashMap<>();

        addJsonNodesToMap(jsonToCompare, resourcePath + compareFile);

        // for HCFS on Cloud, wait a bit for async write in previous steps to finish
        if (protocol != ProtocolEnum.HDFS && protocol != ProtocolEnum.FILE) {
            sleep(10000);
        }
        for (String srcPath : hdfs.list(fullTestPath)) {
            final String fileName = "file_" + cnt++;
            final String filePath = publicStage + fileName;
            filesToDelete.add(new File(filePath + ".avro"));
            filesToDelete.add(new File(publicStage + "." + fileName + ".avro.crc"));
            // make sure the file is available, saw flakes on Cloud that listed files were not available
            int attempts = 0;
            while (!hdfs.doesFileExist(srcPath) && attempts++ < 20) {
                sleep(1000);
            }
            hdfs.copyToLocal(srcPath, filePath + ".avro");
            sleep(250);
            hdfs.writeJsonFileFromAvro("file://" + filePath + ".avro", filePath + ".json");
            hdfs.writeAvroMetadata("file://" + filePath + ".avro", filePath + ".meta");
            BufferedReader reader = new BufferedReader(new FileReader(filePath + ".meta"));
            String nextLine;
            int codecCount = 0;
            while ((nextLine = reader.readLine()) != null) {
                if (nextLine.matches("^avro\\.codec.*$")) {
                    codecCount++;
                    assertEquals(codec, nextLine.split("\t")[1]);
                }
            }
            assertEquals(1, codecCount);
            addJsonNodesToMap(jsonFromHdfs, filePath + ".json");
            filesToDelete.add(new File(filePath + ".json"));
        }

        for (Integer integer : jsonToCompare.keySet()) {
            assertEquals(jsonToCompare.get(integer), jsonFromHdfs.get(integer));
        }
    }

    private Integer getIntegerFromJsonNode(JsonNode node) {
        JsonNode typeInt = node.get("type_int");
        // an Avro int type: '{"type_int": 7}'
        if (typeInt.isInt()) {
            return typeInt.asInt();
        }

        // an Avro enum type: '{"type_int": {"int": 7}}'
        return typeInt.get("int").asInt();
    }

    private void addJsonNodesToMap(Map<Integer, JsonNode> map, String filePath) {
        ObjectMapper mapper = new ObjectMapper();
        BufferedReader reader;
        String line;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            line = reader.readLine();
            while (line != null) {
                JsonNode node = mapper.readTree(line);
                map.put(getIntegerFromJsonNode(node), node);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void prepareWritableExternalTable(String name, String[] fields, String path) {
        exTable = TableFactory.getPxfHcfsWritableTable(name + "_writable",
                fields, path, hdfs.getBasePath(), "avro");
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path) {
        readableExternalTable = TableFactory.getPxfHcfsReadableTable(name + "_readable",
                fields, path, hdfs.getBasePath(),"avro");
    }
}
