package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ORCVectorizedResolverReadTest extends ORCVectorizedBaseTest {

    private static final String ORC_TYPES_SCHEMA = "struct<t1:string,t2:string,num1:int,dub1:double,dec1:decimal(38,18),tm:timestamp,r:float,bg:bigint,b:boolean,tn:tinyint,sml:smallint,dt:date,vc1:varchar(5),c1:char(3),bin:binary>";
    private static final String ORC_TYPES_SCHEMA_COMPOUND = "struct<id:int,bool_arr:array<boolean>,int2_arr:array<smallint>,int_arr:array<int>,int8_arr:array<bigint>,float_arr:array<float>,float8_arr:array<double>,text_arr:array<string>,bytea_arr:array<binary>,char_arr:array<char(15)>,varchar_arr:array<varchar(15)>>";
    private static final String ORC_TYPES_SCHEMA_COMPOUND_MULTI = "struct<id:int,bool_arr:array<array<boolean>>,int2_arr:array<array<smallint>>,int_arr:array<array<int>>,int8_arr:array<array<bigint>>,float_arr:array<array<float>>,float8_arr:array<array<double>>,text_arr:array<array<string>>,bytea_arr:array<array<binary>>,char_arr:array<array<char(15)>>,varchar_arr:array<array<varchar(15)>>>";
    private ORCVectorizedResolver resolver;
    private RequestContext context;

    @BeforeEach
    public void setup() {
        super.setup();

        resolver = new ORCVectorizedResolver();
        context = new RequestContext();
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setUser("test-user");
        context.setTupleDescription(columnDescriptors);
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.setConfiguration(new Configuration());
    }

    @Test
    public void testInitialize() {
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
    }

    @Test
    public void testFailsOnMissingSchema() {
        context.setMetadata(null);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        Exception e = assertThrows(RuntimeException.class,
                () -> resolver.getFieldsForBatch(new OneRow()));
        assertEquals("No schema detected in request context", e.getMessage());
    }

    @Test
    public void testGetFieldsForBatchPrimitiveEmptySchema() throws IOException {
        // empty schema
        TypeDescription schema = TypeDescription.createStruct();
        // no column projection
        columnDescriptors.forEach(cd -> cd.setProjected(false));

        context.setMetadata(schema);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        VectorizedRowBatch batch = readOrcFile("orc_types.orc", 25, schema);
        OneRow batchOfRows = new OneRow(batch);
        List<List<OneField>> fieldsForBatch = resolver.getFieldsForBatch(batchOfRows);
        assertNotNull(fieldsForBatch);
        assertEquals(25, fieldsForBatch.size());

        // all OneField's values should be null since there is no projection
        for (List<OneField> oneFieldList : fieldsForBatch) {
            assertEquals(15, oneFieldList.size());
            for (OneField field : oneFieldList) {
                assertNull(field.val);
            }
        }
    }

    @Test
    public void testGetFieldsForBatchPrimitive() throws IOException {
        // This schema matches the columnDescriptors schema
        TypeDescription schema = TypeDescription.fromString(ORC_TYPES_SCHEMA);
        context.setMetadata(schema);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        VectorizedRowBatch batch = readOrcFile("orc_types.orc", 25, schema);

        OneRow batchOfRows = new OneRow(batch);
        List<List<OneField>> fieldsForBatch = resolver.getFieldsForBatch(batchOfRows);
        assertNotNull(fieldsForBatch);
        assertEquals(25, fieldsForBatch.size());

        assertDataReturned(ORC_TYPES_DATASET, fieldsForBatch);
    }

    @Test
    public void testGetFieldsForBatchCompound() throws IOException {
        // This schema matches the columnDescriptors schema
        TypeDescription schema = TypeDescription.fromString(ORC_TYPES_SCHEMA_COMPOUND);
        context.setMetadata(schema);


        context.setTupleDescription(columnDescriptorsCompound);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        VectorizedRowBatch batch = readOrcFile("orc_types_compound.orc", 6, schema);

        OneRow batchOfRows = new OneRow(batch);
        List<List<OneField>> fieldsForBatch = resolver.getFieldsForBatch(batchOfRows);
        assertNotNull(fieldsForBatch);
        assertEquals(6, fieldsForBatch.size());

        assertCompoundDataReturned(ORC_COMPOUND_TYPES_DATASET, fieldsForBatch);
    }

    @Test
    public void testGetFieldsForBatchCompoundWithProjection() throws IOException {
        // Only project indexes 1, 4, 5, 8
        IntStream.range(0, columnDescriptorsCompound.size()).forEach(idx ->
                columnDescriptorsCompound
                        .get(idx)
                        .setProjected(idx == 1 || idx == 4 || idx == 5 || idx == 8));

        // This schema matches the columnDescriptors schema
        TypeDescription schema = TypeDescription.fromString("struct<bool_arr:array<boolean>,int8_arr:array<bigint>,float_arr:array<float>,bytea_arr:array<binary>>");
        context.setMetadata(schema);

        context.setTupleDescription(columnDescriptorsCompound);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        VectorizedRowBatch batch = readOrcFile("orc_types_compound.orc", 6, schema);

        OneRow batchOfRows = new OneRow(batch);
        List<List<OneField>> fieldsForBatch = resolver.getFieldsForBatch(batchOfRows);
        assertNotNull(fieldsForBatch);
        assertEquals(6, fieldsForBatch.size());

        assertCompoundDataReturned(ORC_COMPOUND_TYPES_DATASET, fieldsForBatch);
    }

    @Test
    public void testGetFieldsForBatchCompoundMultiDimensional() throws IOException {
        // This schema matches the columnDescriptors schema
        TypeDescription schema = TypeDescription.fromString(ORC_TYPES_SCHEMA_COMPOUND_MULTI);
        context.setMetadata(schema);


        context.setTupleDescription(columnDescriptorsCompound);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        VectorizedRowBatch batch = readOrcFile("orc_types_compound_multi.orc", 6, schema);

        OneRow batchOfRows = new OneRow(batch);
        List<List<OneField>> fieldsForBatch = resolver.getFieldsForBatch(batchOfRows);
        assertNotNull(fieldsForBatch);
        assertEquals(6, fieldsForBatch.size());

        assertCompoundDataReturned(ORC_COMPOUND_MULTI_TYPES_DATASET, fieldsForBatch);
    }

    @Test
    public void testGetFieldsForBatchCompoundMixedType() {
        // This schema matches the columnDescriptors schema
        TypeDescription schema = TypeDescription.fromString("struct<id:int,bool_arr:array<struct<completed:boolean>>,int_arr:array<uniontype<int,int>>>");
        context.setMetadata(schema);


        context.setTupleDescription(columnDescriptorsCompound);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        // this should fail on the child columnvector not being of type listcolumnvector (see OrcVectorizedMappingFunctions::listMapper line 75
        SchemaEvolution.IllegalEvolutionException e = assertThrows(SchemaEvolution.IllegalEvolutionException.class,
                () -> readOrcFile("orc_types_compound_multi.orc", 5, schema));
        assertEquals("ORC does not support type conversion from file type array<boolean> (3) to reader type struct<completed:boolean> (3)", e.getMessage());
    }

    @Test
    public void testGetFieldsForBatchPrimitiveWithProjection() throws IOException {

        // Only project indexes 1,2,5,6,9,13
        IntStream.range(0, columnDescriptors.size()).forEach(idx ->
                columnDescriptors
                        .get(idx)
                        .setProjected(idx == 1 || idx == 2 || idx == 5 || idx == 6 || idx == 9 || idx == 13));

        // This schema matches the columnDescriptors schema
        TypeDescription schema = TypeDescription.fromString("struct<t2:string,num1:int,tm:timestamp,r:float,tn:tinyint,c1:char(3)>");
        context.setMetadata(schema);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        VectorizedRowBatch batch = readOrcFile("orc_types.orc", 25, schema);

        OneRow batchOfRows = new OneRow(batch);
        List<List<OneField>> fieldsForBatch = resolver.getFieldsForBatch(batchOfRows);
        assertNotNull(fieldsForBatch);
        assertEquals(25, fieldsForBatch.size());

        assertDataReturned(ORC_TYPES_DATASET, fieldsForBatch);
    }

    @Test
    public void testGetFieldsForBatchWithUnsupportedComplexTypes() throws IOException {
        TypeDescription schema = TypeDescription.fromString("struct<actor:struct<" +
                "avatar_url:string,gravatar_id:string,id:int,login:string,url:string>," +
                "num1:int>");

        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("actor", DataType.TEXT.getOID(), 0, "text", null));
        columnDescriptors.add(new ColumnDescriptor("num1", DataType.INTEGER.getOID(), 1, "int4", null));

        context.setMetadata(schema);
        context.setTupleDescription(columnDescriptors);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        VectorizedRowBatch batch = readOrcFile("orc_types.orc", 25, schema);

        OneRow batchOfRows = new OneRow(batch);

        UnsupportedTypeException e = assertThrows(UnsupportedTypeException.class,
                () -> resolver.getFieldsForBatch(batchOfRows));
        assertEquals("ORC type 'struct' is not supported for reading.", e.getMessage());
    }

    /**
     * The read schema is a superset of the file schema
     */
    @Test
    public void testGetFieldsForBatchPrimitiveUnorderedSubset() throws IOException {
        // This schema matches the columnDescriptors schema
        TypeDescription schema = TypeDescription.fromString(ORC_TYPES_SCHEMA);
        context.setMetadata(schema);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        VectorizedRowBatch batch = readOrcFile("orc_types_unordered_subset.orc", 17, schema);

        OneRow batchOfRows = new OneRow(batch);
        List<List<OneField>> fieldsForBatch = resolver.getFieldsForBatch(batchOfRows);
        assertNotNull(fieldsForBatch);
        assertEquals(17, fieldsForBatch.size());

        assertSubsetOfDataReturned(fieldsForBatch);
    }

    @Test
    public void testGetFieldsForBatchRepeatedPrimitive() throws IOException {
        // This schema matches the columnDescriptors schema
        TypeDescription schema = TypeDescription.fromString(ORC_TYPES_SCHEMA);
        context.setMetadata(schema);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        VectorizedRowBatch batch = readOrcFile("orc_types_repeated.orc", 3, schema);

        OneRow batchOfRows = new OneRow(batch);
        List<List<OneField>> fieldsForBatch = resolver.getFieldsForBatch(batchOfRows);
        assertNotNull(fieldsForBatch);
        assertEquals(3, fieldsForBatch.size());

        assertDataReturned(ORC_TYPES_REPEATED_DATASET, fieldsForBatch);
    }

    @Test
    public void testGetFieldsForMultipleBatches() throws IOException {
        // This schema matches the columnDescriptors schema
        TypeDescription schema = TypeDescription.fromString(ORC_TYPES_SCHEMA);
        context.setMetadata(schema);

        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        List<VectorizedRowBatch> batches = readBatchesFromOrcFile("orc_types.orc", 24, 2, schema);

        OneRow firstBatchOfRows = new OneRow(batches.get(0));
        List<List<OneField>> fieldsForBatch1 = resolver.getFieldsForBatch(firstBatchOfRows);
        assertNotNull(fieldsForBatch1);
        assertEquals(24, fieldsForBatch1.size());

        OneRow batchOfRows = new OneRow(batches.get(1));
        List<List<OneField>> fieldsForBatch2 = resolver.getFieldsForBatch(batchOfRows);
        assertNotNull(fieldsForBatch2);
        assertEquals(1, fieldsForBatch2.size());

        List<List<OneField>> fields = new ArrayList<>();
        fields.addAll(fieldsForBatch1);
        fields.addAll(fieldsForBatch2);
        assertDataReturned(ORC_TYPES_DATASET, fields);
    }

    @Test
    public void testUnsupportedFunctionality() {
        Exception e = assertThrows(UnsupportedOperationException.class, () -> resolver.getFields(new OneRow()));
        assertEquals("Current operation is not supported", e.getMessage());

        e = assertThrows(UnsupportedOperationException.class, () -> resolver.setFields(Collections.singletonList(new OneField())));
        assertEquals("Current operation is not supported", e.getMessage());
    }

    private void assertDataReturned(Object[][] expected, List<List<OneField>> fieldsForBatch) {
        for (int rowNum = 0; rowNum < fieldsForBatch.size(); rowNum++) {
            List<OneField> row = fieldsForBatch.get(rowNum);
            assertNotNull(row);
            assertTypes(row);

            List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
            for (int colNum = 0; colNum < tupleDescription.size(); colNum++) {
                ColumnDescriptor columnDescriptor = tupleDescription.get(colNum);
                Object value = row.get(colNum).val;
                if (columnDescriptor.isProjected()) {
                    Object expectedValue = expected[colNum][rowNum];
                    if (colNum == 4 && expectedValue != null) {
                        expectedValue = new HiveDecimalWritable(String.valueOf(expectedValue));
                    } else if (colNum == 11 && expectedValue != null) {
                        expectedValue = Date.valueOf(String.valueOf(expectedValue));
                    }
                    if (colNum == 14) {
                        if (expectedValue == null) {
                            assertNull(value, "Row " + rowNum + ", COL" + (colNum + 1));
                        } else {
                            assertEquals(expectedValue, ((byte[]) value)[0], "Row " + rowNum + ", COL" + (colNum + 1));
                        }
                    } else {
                        assertEquals(expectedValue, value, "Row " + rowNum + ", COL" + (colNum + 1));
                    }
                } else {
                    assertNull(value);
                }
            }
        }
    }

    private void assertCompoundDataReturned(Object[][] expected, List<List<OneField>> fieldsForBatch) {
        for (int rowNum = 0; rowNum < fieldsForBatch.size(); rowNum++) {
            List<OneField> row = fieldsForBatch.get(rowNum);
            assertNotNull(row);
            assertTypes(row);

            List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
            for (int colNum = 0; colNum < tupleDescription.size(); colNum++) {
                ColumnDescriptor columnDescriptor = tupleDescription.get(colNum);
                Object value = row.get(colNum).val;
                if (columnDescriptor.isProjected()) {
                    Object expectedValue = expected[colNum][rowNum];
                    assertEquals(expectedValue, value, "Row " + rowNum + ", COL" + (colNum + 1));
                } else {
                    assertNull(value);
                }
            }
        }
    }

    private void assertSubsetOfDataReturned(List<List<OneField>> fieldsForBatch) {
        for (int rowNum = 0; rowNum < fieldsForBatch.size(); rowNum++) {
            List<OneField> row = fieldsForBatch.get(rowNum);
            assertNotNull(row);
            assertTypes(row);

            List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
            for (int colNum = 0; colNum < tupleDescription.size(); colNum++) {
                ColumnDescriptor columnDescriptor = tupleDescription.get(colNum);
                Object value = row.get(colNum).val;
                if (columnDescriptor.isProjected()) {
                    Object expectedValue = null;
                    switch (colNum) {
                        case 0:
                            expectedValue = COL1_SUBSET[rowNum];
                            break;
                        case 2:
                            expectedValue = COL3_SUBSET[rowNum];
                            break;
                        case 4:
                            expectedValue = COL5_SUBSET[rowNum] == null ? null : new HiveDecimalWritable(COL5_SUBSET[rowNum]);
                            break;
                        case 5:
                            expectedValue = COL6_SUBSET[rowNum];
                            break;
                        case 8:
                            expectedValue = COL9_SUBSET[rowNum];
                            break;
                        case 10:
                            expectedValue = COL11_SUBSET[rowNum];
                            break;
                        case 12:
                            expectedValue = COL13_SUBSET[rowNum];
                            break;
                        case 1:
                        case 3:
                        case 6:
                        case 7:
                        case 9:
                        case 11:
                        case 13:
                        case 14:
                            expectedValue = null;
                            break;
                    }
                    assertEquals(expectedValue, value, "Row " + rowNum + ", COL" + (colNum + 1));
                } else {
                    assertNull(value);
                }
            }
        }
    }

    private void assertTypes(List<OneField> fieldList) {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();

        for (int i = 0; i < columnDescriptors.size(); i++) {
            if (columnDescriptors.get(i).isProjected()) {
                assertEquals(columnDescriptors.get(i).columnTypeCode(), fieldList.get(i).type);
            }
        }
    }

    private VectorizedRowBatch readOrcFile(String filename, long expectedSize, TypeDescription readSchema)
            throws IOException {
        String orcFile = Objects.requireNonNull(getClass().getClassLoader().getResource("orc/" + filename)).getPath();
        Path file = new Path(orcFile);
        Configuration configuration = new Configuration();

        Reader fileReader = OrcFile.createReader(file, OrcFile
                .readerOptions(configuration)
                .filesystem(file.getFileSystem(configuration)));

        // Build the reader options
        Reader.Options options = fileReader
                .options()
                .schema(readSchema);

        RecordReader recordReader = fileReader.rows(options);
        VectorizedRowBatch batch = readSchema.createRowBatch();
        assertTrue(recordReader.nextBatch(batch));
        assertEquals(expectedSize, batch.size);
        return batch;
    }

    /**
     * Helper method for returning a list of batches from a single ORC file
     * @param filename name of ORC file to read
     * @param rowBatchMaxSize max size of batch to read from file
     * @param expectedNumberOfBatches expected number of batches that should be returned by this method
     * @param readSchema description of types in ORC file
     * @return
     * @throws IOException
     */
    private List<VectorizedRowBatch> readBatchesFromOrcFile(String filename, int rowBatchMaxSize, int expectedNumberOfBatches, TypeDescription readSchema)
            throws IOException {
        String orcFile = Objects.requireNonNull(getClass().getClassLoader().getResource("orc/" + filename)).getPath();
        Path file = new Path(orcFile);
        Configuration configuration = new Configuration();

        Reader fileReader = OrcFile.createReader(file, OrcFile
                .readerOptions(configuration)
                .filesystem(file.getFileSystem(configuration)));

        // Build the reader options
        Reader.Options options = fileReader
                .options()
                .schema(readSchema);

        RecordReader recordReader = fileReader.rows(options);
        List<VectorizedRowBatch> batches = new ArrayList<>();
        boolean hasMore = true;
        while (hasMore) {
            VectorizedRowBatch batch = readSchema.createRowBatch(rowBatchMaxSize);
            hasMore = recordReader.nextBatch(batch);
            if (hasMore) {
                batches.add(batch);
            }
        }

        assertEquals(expectedNumberOfBatches, batches.size());

        return batches;
    }
}