package org.greenplum.pxf.plugins.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.PgArrayBuilder;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetOutputFormat.DICTIONARY_PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetOutputFormat.ENABLE_DICTIONARY;
import static org.apache.parquet.hadoop.ParquetOutputFormat.PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import static org.greenplum.pxf.plugins.hdfs.parquet.ParquetTypeConverter.bytesToTimestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParquetWriteTest {

    private Accessor accessor;
    private Resolver resolver;
    private RequestContext context;
    private Configuration configuration;
    private final PgUtilities pgUtilities = new PgUtilities();
    private PgArrayBuilder pgArrayBuilder = null;

    protected List<ColumnDescriptor> columnDescriptors;

    @TempDir
    File temp; // must be non-private

    @BeforeEach
    public void setup() {

        columnDescriptors = new ArrayList<>();

        accessor = new ParquetFileAccessor();
        resolver = new ParquetResolver();
        context = new RequestContext();
        configuration = new Configuration();
        configuration.set("pxf.fs.basePath", "/");

        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setSegmentId(4);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.setTupleDescription(columnDescriptors);
        context.setConfiguration(configuration);
    }

    @Test
    public void testDefaultWriteOptions() throws Exception {

        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        context.setDataSource(temp + "/out/");
        context.setTransactionId("XID-XYZ-123453");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        assertTrue(accessor.openForWrite());
        accessor.closeForWrite();

        assertEquals(1024 * 1024, configuration.getInt(PAGE_SIZE, -1));
        assertEquals(1024 * 1024, configuration.getInt(DICTIONARY_PAGE_SIZE, -1));
        assertTrue(configuration.getBoolean(ENABLE_DICTIONARY, false));
        assertEquals("PARQUET_1_0", configuration.get(WRITER_VERSION));
        assertEquals(8 * 1024 * 1024, configuration.getLong(BLOCK_SIZE, -1));
    }

    @Test
    public void testSetting_PAGE_SIZE_Option() throws Exception {

        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        context.setDataSource(temp + "/out/");
        context.setTransactionId("XID-XYZ-123453");
        context.addOption("PAGE_SIZE", "5242880");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        assertTrue(accessor.openForWrite());
        accessor.closeForWrite();

        assertEquals(5 * 1024 * 1024, configuration.getInt(PAGE_SIZE, -1));
    }

    @Test
    public void testSetting_DICTIONARY_PAGE_SIZE_Option() throws Exception {

        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        context.setDataSource(temp + "/out/");
        context.setTransactionId("XID-XYZ-123453");
        context.addOption("DICTIONARY_PAGE_SIZE", "5242880");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        assertTrue(accessor.openForWrite());
        accessor.closeForWrite();

        assertEquals(5 * 1024 * 1024, configuration.getInt(DICTIONARY_PAGE_SIZE, -1));
    }

    @Test
    public void testSetting_ENABLE_DICTIONARY_Option() throws Exception {

        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        context.setDataSource(temp + "/out/");
        context.setTransactionId("XID-XYZ-123453");
        context.addOption("ENABLE_DICTIONARY", "false");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        assertTrue(accessor.openForWrite());
        accessor.closeForWrite();

        assertFalse(configuration.getBoolean(ENABLE_DICTIONARY, true));
    }

    @Test
    public void testSetting_PARQUET_VERSION_Option() throws Exception {

        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        context.setDataSource(temp + "/out/");
        context.setTransactionId("XID-XYZ-123453");
        context.addOption("PARQUET_VERSION", "v2");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        assertTrue(accessor.openForWrite());
        accessor.closeForWrite();

        assertEquals("PARQUET_2_0", configuration.get(WRITER_VERSION));
    }

    @Test
    public void testSetting_ROWGROUP_SIZE_Option() throws Exception {

        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        context.setDataSource(temp + "/out/");
        context.setTransactionId("XID-XYZ-123453");
        context.addOption("ROWGROUP_SIZE", "33554432");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        assertTrue(accessor.openForWrite());
        accessor.closeForWrite();

        assertEquals(32 * 1024 * 1024, configuration.getInt(BLOCK_SIZE, -1));
    }

    @Test
    public void testWriteInt() throws Exception {

        String path = temp + "/out/int/";

        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123456");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with INT values from 0 to 9
        for (int i = 0; i < 10; i++) {
            List<OneField> record = Collections.singletonList(new OneField(DataType.INTEGER.getOID(), i));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // There is no logical annotation, the physical type is INT32
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.INT32, type.asPrimitiveType().getPrimitiveTypeName());
        assertNull(type.getLogicalTypeAnnotation());
        assertEquals(0, fileReader.read().getInteger(0, 0));
        assertEquals(1, fileReader.read().getInteger(0, 0));
        assertEquals(2, fileReader.read().getInteger(0, 0));
        assertEquals(3, fileReader.read().getInteger(0, 0));
        assertEquals(4, fileReader.read().getInteger(0, 0));
        assertEquals(5, fileReader.read().getInteger(0, 0));
        assertEquals(6, fileReader.read().getInteger(0, 0));
        assertEquals(7, fileReader.read().getInteger(0, 0));
        assertEquals(8, fileReader.read().getInteger(0, 0));
        assertEquals(9, fileReader.read().getInteger(0, 0));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteText() throws Exception {
        String path = temp + "/out/text/";
        columnDescriptors.add(new ColumnDescriptor("name", DataType.TEXT.getOID(), 0, "text", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123457");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with TEXT values of a repeated i + 1 times
        for (int i = 0; i < 10; i++) {
            List<OneField> record = Collections.singletonList(new OneField(DataType.TEXT.getOID(), StringUtils.repeat("a", i + 1)));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is binary, logical type is String
        assertNotNull(schema.getColumns());
        assertEquals(1, schema.getColumns().size());
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, type.asPrimitiveType().getPrimitiveTypeName());
        assertTrue(type.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation);
        assertEquals("a", fileReader.read().getString(0, 0));
        assertEquals("aa", fileReader.read().getString(0, 0));
        assertEquals("aaa", fileReader.read().getString(0, 0));
        assertEquals("aaaa", fileReader.read().getString(0, 0));
        assertEquals("aaaaa", fileReader.read().getString(0, 0));
        assertEquals("aaaaaa", fileReader.read().getString(0, 0));
        assertEquals("aaaaaaa", fileReader.read().getString(0, 0));
        assertEquals("aaaaaaaa", fileReader.read().getString(0, 0));
        assertEquals("aaaaaaaaa", fileReader.read().getString(0, 0));
        assertEquals("aaaaaaaaaa", fileReader.read().getString(0, 0));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteDate() throws Exception {
        String path = temp + "/out/date/";
        columnDescriptors.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 0, "date", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123458");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with DATE from 2020-08-01 to 2020-08-10
        for (int i = 0; i < 10; i++) {
            List<OneField> record = Collections.singletonList(new OneField(DataType.DATE.getOID(), String.format("2020-08-%02d", i + 1)));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is INT32, logical type is DATE
        assertNotNull(schema.getColumns());
        assertEquals(1, schema.getColumns().size());
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.INT32, type.asPrimitiveType().getPrimitiveTypeName());
        assertTrue(type.getLogicalTypeAnnotation() instanceof DateLogicalTypeAnnotation);

        // Days since epoch: 18475 days from 1970-01-01 -> 2020-08-01
        assertEquals(18475, fileReader.read().getInteger(0, 0));
        assertEquals(18476, fileReader.read().getInteger(0, 0));
        assertEquals(18477, fileReader.read().getInteger(0, 0));
        assertEquals(18478, fileReader.read().getInteger(0, 0));
        assertEquals(18479, fileReader.read().getInteger(0, 0));
        assertEquals(18480, fileReader.read().getInteger(0, 0));
        assertEquals(18481, fileReader.read().getInteger(0, 0));
        assertEquals(18482, fileReader.read().getInteger(0, 0));
        assertEquals(18483, fileReader.read().getInteger(0, 0));
        assertEquals(18484, fileReader.read().getInteger(0, 0));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteFloat8() throws Exception {
        String path = temp + "/out/float/";
        columnDescriptors.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 0, "float8", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123459");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with float8 values
        for (int i = 0; i < 10; i++) {
            List<OneField> record = Collections.singletonList(new OneField(DataType.FLOAT8.getOID(), 1.1 * i));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is double
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.DOUBLE, type.asPrimitiveType().getPrimitiveTypeName());
        assertNull(type.getLogicalTypeAnnotation());
        assertEquals(0, fileReader.read().getDouble(0, 0), 0.01);
        assertEquals(1.1, fileReader.read().getDouble(0, 0), 0.01);
        assertEquals(2.2, fileReader.read().getDouble(0, 0), 0.01);
        assertEquals(3.3, fileReader.read().getDouble(0, 0), 0.01);
        assertEquals(4.4, fileReader.read().getDouble(0, 0), 0.01);
        assertEquals(5.5, fileReader.read().getDouble(0, 0), 0.01);
        assertEquals(6.6, fileReader.read().getDouble(0, 0), 0.01);
        assertEquals(7.7, fileReader.read().getDouble(0, 0), 0.01);
        assertEquals(8.8, fileReader.read().getDouble(0, 0), 0.01);
        assertEquals(9.9, fileReader.read().getDouble(0, 0), 0.01);
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteBoolean() throws Exception {
        String path = temp + "/out/boolean/";
        columnDescriptors.add(new ColumnDescriptor("b", DataType.BOOLEAN.getOID(), 5, "bool", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123460");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with boolean values
        for (int i = 0; i < 10; i++) {
            List<OneField> record = Collections.singletonList(new OneField(DataType.BOOLEAN.getOID(), i % 2 == 0));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is boolean
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.BOOLEAN, type.asPrimitiveType().getPrimitiveTypeName());
        assertNull(type.getLogicalTypeAnnotation());
        assertTrue(fileReader.read().getBoolean(0, 0));
        assertFalse(fileReader.read().getBoolean(0, 0));
        assertTrue(fileReader.read().getBoolean(0, 0));
        assertFalse(fileReader.read().getBoolean(0, 0));
        assertTrue(fileReader.read().getBoolean(0, 0));
        assertFalse(fileReader.read().getBoolean(0, 0));
        assertTrue(fileReader.read().getBoolean(0, 0));
        assertFalse(fileReader.read().getBoolean(0, 0));
        assertTrue(fileReader.read().getBoolean(0, 0));
        assertFalse(fileReader.read().getBoolean(0, 0));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteTimestamp() throws Exception {
        String path = temp + "/out/timestamp/";
        columnDescriptors.add(new ColumnDescriptor("tm", DataType.TIMESTAMP.getOID(), 0, "timestamp", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123462");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with timestamp values
        for (int i = 0; i < 10; i++) {

            Instant timestamp = Instant.parse(String.format("2020-08-%02dT04:00:05Z", i + 1)); // UTC
            ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
            String localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // should be "2020-08-%02dT04:00:05Z" in PST

            List<OneField> record = Collections.singletonList(new OneField(DataType.TIMESTAMP.getOID(), localTimestampString));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is INT96
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.INT96, type.asPrimitiveType().getPrimitiveTypeName());
        assertNull(type.getLogicalTypeAnnotation());

        for (int i = 0; i < 10; i++) {

            Instant timestamp = Instant.parse(String.format("2020-08-%02dT04:00:05Z", i + 1)); // UTC
            ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
            String localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // should be "2020-08-%02dT04:00:05Z" in PST

            assertEquals(localTimestampString, bytesToTimestamp(fileReader.read().getInt96(0, 0).getBytes()));
        }
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteBigInt() throws Exception {
        String path = temp + "/out/bigint/";
        columnDescriptors.add(new ColumnDescriptor("bg", DataType.BIGINT.getOID(), 0, "bigint", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123463");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with bigint values
        for (int i = 0; i < 10; i++) {
            long value = (long) Integer.MAX_VALUE + i;
            List<OneField> record = Collections.singletonList(new OneField(DataType.BIGINT.getOID(), value));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is INT64
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.INT64, type.asPrimitiveType().getPrimitiveTypeName());
        assertNull(type.getLogicalTypeAnnotation());
        assertEquals(2147483647L, fileReader.read().getLong(0, 0));
        assertEquals(2147483648L, fileReader.read().getLong(0, 0));
        assertEquals(2147483649L, fileReader.read().getLong(0, 0));
        assertEquals(2147483650L, fileReader.read().getLong(0, 0));
        assertEquals(2147483651L, fileReader.read().getLong(0, 0));
        assertEquals(2147483652L, fileReader.read().getLong(0, 0));
        assertEquals(2147483653L, fileReader.read().getLong(0, 0));
        assertEquals(2147483654L, fileReader.read().getLong(0, 0));
        assertEquals(2147483655L, fileReader.read().getLong(0, 0));
        assertEquals(2147483656L, fileReader.read().getLong(0, 0));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteBytea() throws Exception {
        String path = temp + "/out/bytea/";
        columnDescriptors.add(new ColumnDescriptor("bin", DataType.BYTEA.getOID(), 0, "bytea", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123464");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with bytea values
        for (int i = 0; i < 10; i++) {
            byte[] value = Binary.fromString(StringUtils.repeat("a", i + 1)).getBytes();
            List<OneField> record = Collections.singletonList(new OneField(DataType.BYTEA.getOID(), value));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is BINARY
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, type.asPrimitiveType().getPrimitiveTypeName());
        assertNull(type.getLogicalTypeAnnotation());
        assertEquals(Binary.fromString("a"), fileReader.read().getBinary(0, 0));
        assertEquals(Binary.fromString("aa"), fileReader.read().getBinary(0, 0));
        assertEquals(Binary.fromString("aaa"), fileReader.read().getBinary(0, 0));
        assertEquals(Binary.fromString("aaaa"), fileReader.read().getBinary(0, 0));
        assertEquals(Binary.fromString("aaaaa"), fileReader.read().getBinary(0, 0));
        assertEquals(Binary.fromString("aaaaaa"), fileReader.read().getBinary(0, 0));
        assertEquals(Binary.fromString("aaaaaaa"), fileReader.read().getBinary(0, 0));
        assertEquals(Binary.fromString("aaaaaaaa"), fileReader.read().getBinary(0, 0));
        assertEquals(Binary.fromString("aaaaaaaaa"), fileReader.read().getBinary(0, 0));
        assertEquals(Binary.fromString("aaaaaaaaaa"), fileReader.read().getBinary(0, 0));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteSmallInt() throws Exception {
        String path = temp + "/out/smallint/";
        columnDescriptors.add(new ColumnDescriptor("sml", DataType.SMALLINT.getOID(), 0, "int2", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123465");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with bigint values
        for (int i = 0; i < 10; i++) {
            List<OneField> record = Collections.singletonList(new OneField(DataType.SMALLINT.getOID(), (short) i));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is INT32
        assertNotNull(schema.getColumns());
        assertEquals(1, schema.getColumns().size());
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.INT32, type.asPrimitiveType().getPrimitiveTypeName());
        assertTrue(type.getLogicalTypeAnnotation() instanceof IntLogicalTypeAnnotation);
        assertEquals(0, fileReader.read().getInteger(0, 0));
        assertEquals(1, fileReader.read().getInteger(0, 0));
        assertEquals(2, fileReader.read().getInteger(0, 0));
        assertEquals(3, fileReader.read().getInteger(0, 0));
        assertEquals(4, fileReader.read().getInteger(0, 0));
        assertEquals(5, fileReader.read().getInteger(0, 0));
        assertEquals(6, fileReader.read().getInteger(0, 0));
        assertEquals(7, fileReader.read().getInteger(0, 0));
        assertEquals(8, fileReader.read().getInteger(0, 0));
        assertEquals(9, fileReader.read().getInteger(0, 0));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteReal() throws Exception {
        String path = temp + "/out/real/";
        columnDescriptors.add(new ColumnDescriptor("r", DataType.REAL.getOID(), 0, "real", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123466");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with real values
        for (int i = 0; i < 10; i++) {
            List<OneField> record = Collections.singletonList(new OneField(DataType.REAL.getOID(), 1.1F * i));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is FLOAT
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.FLOAT, type.asPrimitiveType().getPrimitiveTypeName());
        assertNull(type.getLogicalTypeAnnotation());
        assertEquals(0F, fileReader.read().getFloat(0, 0), 0.001);
        assertEquals(1.1F, fileReader.read().getFloat(0, 0), 0.001);
        assertEquals(2.2F, fileReader.read().getFloat(0, 0), 0.001);
        assertEquals(3.3F, fileReader.read().getFloat(0, 0), 0.001);
        assertEquals(4.4F, fileReader.read().getFloat(0, 0), 0.001);
        assertEquals(5.5F, fileReader.read().getFloat(0, 0), 0.001);
        assertEquals(6.6F, fileReader.read().getFloat(0, 0), 0.001);
        assertEquals(7.7F, fileReader.read().getFloat(0, 0), 0.001);
        assertEquals(8.8F, fileReader.read().getFloat(0, 0), 0.001);
        assertEquals(9.9F, fileReader.read().getFloat(0, 0), 0.001);
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteVarchar() throws Exception {
        String path = temp + "/out/varchar/";
        columnDescriptors.add(new ColumnDescriptor("vc1", DataType.VARCHAR.getOID(), 0, "varchar", new Integer[]{5}));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123467");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with varchar values
        for (int i = 0; i < 10; i++) {
            String s = StringUtils.repeat("b", i % 5);
            List<OneField> record = Collections.singletonList(new OneField(DataType.VARCHAR.getOID(), s));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is BINARY
        assertNotNull(schema.getColumns());
        assertEquals(1, schema.getColumns().size());
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, type.asPrimitiveType().getPrimitiveTypeName());
        assertTrue(type.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation);
        assertEquals("", fileReader.read().getString(0, 0));
        assertEquals("b", fileReader.read().getString(0, 0));
        assertEquals("bb", fileReader.read().getString(0, 0));
        assertEquals("bbb", fileReader.read().getString(0, 0));
        assertEquals("bbbb", fileReader.read().getString(0, 0));
        assertEquals("", fileReader.read().getString(0, 0));
        assertEquals("b", fileReader.read().getString(0, 0));
        assertEquals("bb", fileReader.read().getString(0, 0));
        assertEquals("bbb", fileReader.read().getString(0, 0));
        assertEquals("bbbb", fileReader.read().getString(0, 0));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteChar() throws Exception {
        String path = temp + "/out/char/";
        columnDescriptors.add(new ColumnDescriptor("c1", DataType.BPCHAR.getOID(), 0, "char", new Integer[]{3}));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123468");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with char values
        for (int i = 0; i < 10; i++) {
            String s = StringUtils.repeat("c", i % 3);
            List<OneField> record = Collections.singletonList(new OneField(DataType.BPCHAR.getOID(), s));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is BINARY
        assertNotNull(schema.getColumns());
        assertEquals(1, schema.getColumns().size());
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, type.asPrimitiveType().getPrimitiveTypeName());
        assertTrue(type.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation);
        assertEquals("", fileReader.read().getString(0, 0));
        assertEquals("c", fileReader.read().getString(0, 0));
        assertEquals("cc", fileReader.read().getString(0, 0));
        assertEquals("", fileReader.read().getString(0, 0));
        assertEquals("c", fileReader.read().getString(0, 0));
        assertEquals("cc", fileReader.read().getString(0, 0));
        assertEquals("", fileReader.read().getString(0, 0));
        assertEquals("c", fileReader.read().getString(0, 0));
        assertEquals("cc", fileReader.read().getString(0, 0));
        assertEquals("", fileReader.read().getString(0, 0));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteNumeric() throws Exception {
        String path = temp + "/out/numeric/";
        // precision is 38 and scale is 18
        columnDescriptors.add(new ColumnDescriptor("dec1", DataType.NUMERIC.getOID(), 0, "numeric", new Integer[]{38, 18}));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123469");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{
                "1.2",
                "22.2345",
                "333.34567",
                "4444.456789",
                "55555.5678901",
                "666666.67890123",
                "7777777.789012345",
                "88888888.8901234567",
                "999999999.90123456789",
                "12345678901234567890.123456789012345678"
        };

        // write parquet file with numeric values
        for (String value : values) {
            List<OneField> record = Collections.singletonList(new OneField(DataType.NUMERIC.getOID(), value));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is FIXED_LEN_BYTE_ARRAY
        assertNotNull(schema.getColumns());
        assertEquals(1, schema.getColumns().size());
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, type.asPrimitiveType().getPrimitiveTypeName());
        assertTrue(type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation);


        assertEquals(new BigDecimal("1.200000000000000000"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("22.234500000000000000"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("333.345670000000000000"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("4444.456789000000000000"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("55555.567890100000000000"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("666666.678901230000000000"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("7777777.789012345000000000"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("88888888.890123456700000000"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("999999999.901234567890000000"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("12345678901234567890.123456789012345678"), new BigDecimal(new BigInteger(fileReader.read().getBinary(0, 0).getBytes()), 18));
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteIntArray() throws Exception {
        String path = temp + "/out/int_array/";

        columnDescriptors.add(new ColumnDescriptor("integer_arr", DataType.INT4ARRAY.getOID(), 0, "int4_arr", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123470");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
        insertParquetArrayData(DataType.INT4ARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();


        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = Integer.parseInt(values[i]);
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.INT32, null);
        fileReader.close();

    }

    @Test
    public void testWriteTextArray() throws Exception {
        String path = temp + "/out/text_array/";

        columnDescriptors.add(new ColumnDescriptor("text_array", DataType.TEXTARRAY.getOID(), 0, "text_array", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123471");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{"a", "aa", "aaa", "aaaa", "aaaaa", "aaaaaa", "aaaaaaa", "aaaaaaaa", "aaaaaaaaa", "aaaaaaaaaa"};
        insertParquetArrayData(DataType.TEXTARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = values[i];
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType());

        fileReader.close();
    }

    @Test
    public void testWriteDateArray() throws Exception {
        String path = temp + "/out/date_array/";

        columnDescriptors.add(new ColumnDescriptor("date_array", DataType.DATEARRAY.getOID(), 0, "datearray", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123472");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with DATE from 2020-08-01 to 2020-08-10
        // physical type is int32
        String[] values = new String[]{String.format("2020-08-%02d", 1), String.format("2020-08-%02d", 2),
                String.format("2020-08-%02d", 3), String.format("2020-08-%02d", 4), String.format("2020-08-%02d", 5),
                String.format("2020-08-%02d", 6), String.format("2020-08-%02d", 7), String.format("2020-08-%02d", 8),
                String.format("2020-08-%02d", 9), String.format("2020-08-%02d", 10)};
        insertParquetArrayData(DataType.DATEARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = 18475 + i;
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.DateLogicalTypeAnnotation.dateType());

        fileReader.close();
    }

    @Test
    public void testWriteFloat8Array() throws Exception {
        String path = temp + "/out/float_arr/";

        columnDescriptors.add(new ColumnDescriptor("float8_array", DataType.FLOAT8ARRAY.getOID(), 0, "float8array", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123473");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{String.valueOf((double) 0), String.valueOf(1.01),
                String.valueOf(2.02), String.valueOf(3.03), String.valueOf(4.04),
                String.valueOf(5.05), String.valueOf(6.06), String.valueOf(7.07),
                String.valueOf(8.08), String.valueOf(9.09)};
        insertParquetArrayData(DataType.FLOAT8ARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = Double.parseDouble(values[i]);
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.DOUBLE, null);

        fileReader.close();

    }

    @Test
    public void testWriteBooleanArray() throws Exception {
        String path = temp + "/out/boolean_array/";

        columnDescriptors.add(new ColumnDescriptor("bool_arr", DataType.BOOLARRAY.getOID(), 0, "bool_arr", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123474");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{"t", "f", "t", "f", "t", "f", "t", "f", "t", "f"};
        insertParquetArrayData(DataType.BOOLARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = Boolean.parseBoolean(values[i]);
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.BOOLEAN, null);

        fileReader.close();
    }

    @Test
    public void testWriteTimestampArray() throws Exception {
        String path = temp + "/out/timestamp_array/";

        columnDescriptors.add(new ColumnDescriptor("tm_array", DataType.TIMESTAMPARRAY.getOID(), 0, "tm_array", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123475");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = generateLocalTimestampStrings(null);
        insertParquetArrayData(DataType.TIMESTAMPARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = values[i];
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.INT96, null);

        fileReader.close();
    }

    @Test
    public void testWriteBigIntArray() throws Exception {
        String path = temp + "/out/big_int_arr/";

        columnDescriptors.add(new ColumnDescriptor("big_int_arr", DataType.INT8ARRAY.getOID(), 0, "int8array", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123476");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{String.valueOf((long) Integer.MAX_VALUE), String.valueOf((long) Integer.MAX_VALUE + 1),
                String.valueOf((long) Integer.MAX_VALUE + 2), String.valueOf((long) Integer.MAX_VALUE + 3),
                String.valueOf((long) Integer.MAX_VALUE + 4), String.valueOf((long) Integer.MAX_VALUE + 5),
                String.valueOf((long) Integer.MAX_VALUE + 6), String.valueOf((long) Integer.MAX_VALUE + 7),
                String.valueOf((long) Integer.MAX_VALUE + 8), String.valueOf((long) Integer.MAX_VALUE + 9)};
        insertParquetArrayData(DataType.INT8ARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = Long.parseLong(values[i]);
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.INT64, null);

        fileReader.close();

    }

    @Test
    public void testWriteByteaArray() throws Exception {
        String path = temp + "/out/bytea_array/";

        columnDescriptors.add(new ColumnDescriptor("bytea_array", DataType.BYTEAARRAY.getOID(), 0, "byteaArray", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123477");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with bytea array values
        String[] values = new String[]{"a", "aa", "aaa", "aaaa", "aaaaa", "aaaaaa", "aaaaaaa", "aaaaaaaa", "aaaaaaaaa", "aaaaaaaaaa"};
        insertParquetArrayData(DataType.BYTEAARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Binary[] expectedResults = new Binary[]{
                Binary.fromString("a"),
                Binary.fromString("aa"),
                Binary.fromString("aaa"),
                Binary.fromString("aaaa"),
                Binary.fromString("aaaaa"),
                Binary.fromString("aaaaaa"),
                Binary.fromString("aaaaaaa"),
                Binary.fromString("aaaaaaaa"),
                Binary.fromString("aaaaaaaaa"),
                null
        };
        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = expectedResults[i];
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.BINARY, null);

        fileReader.close();
    }

    @Test
    public void testWriteSmallIntArray() throws Exception {
        String path = temp + "/out/small_int_array/";

        columnDescriptors.add(new ColumnDescriptor("small_int_arr", DataType.INT2ARRAY.getOID(), 0, "int2array", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123478");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();


        assertTrue(accessor.openForWrite());

        String[] values = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
        insertParquetArrayData(DataType.INT2ARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = Short.parseShort(values[i]);
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, true));

        fileReader.close();
    }

    @Test
    public void testWriteRealArray() throws Exception {
        String path = temp + "/out/real_array/";

        columnDescriptors.add(new ColumnDescriptor("real_array", DataType.FLOAT4ARRAY.getOID(), 0, "float4array", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123479");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{String.valueOf((float) 0), String.valueOf((float) 1.01),
                String.valueOf((float) 2.02), String.valueOf((float) 3.03),
                String.valueOf((float) 4.04), String.valueOf((float) 5.05),
                String.valueOf((float) 6.06), String.valueOf((float) 7.07),
                String.valueOf((float) 8.08), String.valueOf((float) 9.09)};
        insertParquetArrayData(DataType.FLOAT4ARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = Float.parseFloat(values[i]);
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.FLOAT, null);

        fileReader.close();
    }

    @Test
    public void testWriteVarcharArray() throws Exception {
        String path = temp + "/out/varchar_arr/";

        columnDescriptors.add(new ColumnDescriptor("varchar_arr", DataType.VARCHARARRAY.getOID(), 0, "varchararray", new Integer[]{5}));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123480");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{"", "b", "bb", "bbb", "bbbb", "", "b", "bb", "bbb", "bbbb"};
        insertParquetArrayData(DataType.VARCHARARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = values[i];
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType());

        fileReader.close();
    }

    @Test
    public void testWriteCharArray() throws Exception {
        String path = temp + "/out/char_arr/";

        columnDescriptors.add(new ColumnDescriptor("char_arr", DataType.BPCHARARRAY.getOID(), 0, "bpchararray", new Integer[]{3}));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123481");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{"", "c", "cc", "", "c", "cc", "", "c", "cc", ""};
        insertParquetArrayData(DataType.BPCHARARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = values[i];
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType());

        fileReader.close();
    }

    @Test
    public void testWriteNumericArray() throws Exception {
        String path = temp + "/out/numeric_array/";

        columnDescriptors.add(new ColumnDescriptor("numeric_array", DataType.NUMERICARRAY.getOID(), 0, "numericarray", new Integer[]{38, 18}));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123482");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = new String[]{
                "12345678901234567890.123456789012345678",
                "22.2345",
                "333.34567",
                "4444.456789",
                "55555.5678901",
                "666666.67890123",
                "7777777.789012345",
                "88888888.8901234567",
                "999999999.90123456789",
                null
        };
        insertParquetArrayData(DataType.NUMERICARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        BigDecimal[] expectedResults = new BigDecimal[]{
                new BigDecimal("12345678901234567890.123456789012345678"),
                new BigDecimal("22.234500000000000000"),
                new BigDecimal("333.345670000000000000"),
                new BigDecimal("4444.456789000000000000"),
                new BigDecimal("55555.567890100000000000"),
                new BigDecimal("666666.678901230000000000"),
                new BigDecimal("7777777.789012345000000000"),
                new BigDecimal("88888888.890123456700000000"),
                new BigDecimal("999999999.901234567890000000")
        };
        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    expectedValues[i][j] = expectedResults[i];
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.decimalType(18, 38));

        fileReader.close();
    }

    @Test
    public void testWriteMultipleTypes() throws Exception {
        String path = temp + "/out/multiple/";
        columnDescriptors.add(new ColumnDescriptor("dec1", DataType.NUMERIC.getOID(), 0, "numeric", null));
        columnDescriptors.add(new ColumnDescriptor("c1", DataType.BPCHAR.getOID(), 1, "char", new Integer[]{3}));
        columnDescriptors.add(new ColumnDescriptor("tm", DataType.TIMESTAMP.getOID(), 2, "timestamp", null));
        columnDescriptors.add(new ColumnDescriptor("bin", DataType.BYTEA.getOID(), 3, "bytea", null));
        columnDescriptors.add(new ColumnDescriptor("name", DataType.TEXT.getOID(), 4, "text", null));
        columnDescriptors.add(new ColumnDescriptor("int_arr", DataType.INT4ARRAY.getOID(), 5, "int_arr", null));
        columnDescriptors.add(new ColumnDescriptor("float_arr", DataType.FLOAT4ARRAY.getOID(), 6, "float_arr", null));
        columnDescriptors.add(new ColumnDescriptor("tm_arr", DataType.TIMESTAMPARRAY.getOID(), 7, "timestamp_arr", null));
        columnDescriptors.add(new ColumnDescriptor("bin_arr", DataType.BYTEAARRAY.getOID(), 8, "bytea_arr", null));
        columnDescriptors.add(new ColumnDescriptor("str_arr", DataType.TEXTARRAY.getOID(), 9, "text_arr", null));

        context.setDataSource(path);

        context.setTransactionId("XID-XYZ-123483");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        for (int i = 0; i < 3; i++) {
            List<OneField> record = new ArrayList<>();
            record.add(new OneField(DataType.NUMERIC.getOID(), String.format("%d.%d", (i + 1), (i + 2))));

            String s = StringUtils.repeat("d", i % 3);
            record.add(new OneField(DataType.BPCHAR.getOID(), s));

            Instant timestamp = Instant.parse(String.format("2020-08-%02dT04:00:05Z", i + 1)); // UTC
            ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
            String localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // should be "2020-08-%02dT04:00:05Z" in PST
            record.add(new OneField(DataType.TIMESTAMP.getOID(), localTimestampString));

            byte[] bytes = Binary.fromString(StringUtils.repeat("e", i + 1)).getBytes();
            record.add(new OneField(DataType.BYTEA.getOID(), bytes));


            String text = StringUtils.repeat("f", i + 1);
            record.add(new OneField(DataType.TEXT.getOID(), text));


            pgArrayBuilder = new PgArrayBuilder(pgUtilities);
            pgArrayBuilder.startArray();
            pgArrayBuilder.addElement((String) null);
            pgArrayBuilder.addElement(String.valueOf(i));
            pgArrayBuilder.addElement(String.valueOf(i));
            pgArrayBuilder.endArray();
            record.add(new OneField(DataType.INT4ARRAY.getOID(), pgArrayBuilder.toString()));

            pgArrayBuilder = new PgArrayBuilder(pgUtilities);
            pgArrayBuilder.startArray();
            pgArrayBuilder.addElement((String) null);
            pgArrayBuilder.addElement(String.valueOf(i + 0.01F));
            pgArrayBuilder.addElement(String.valueOf(i + 0.01F));
            pgArrayBuilder.endArray();
            record.add(new OneField(DataType.FLOAT4ARRAY.getOID(), pgArrayBuilder.toString()));

            pgArrayBuilder = new PgArrayBuilder(pgUtilities);
            pgArrayBuilder.startArray();
            pgArrayBuilder.addElement((String) null);
            pgArrayBuilder.addElement(localTimestampString);
            pgArrayBuilder.addElement(localTimestampString);
            pgArrayBuilder.endArray();
            record.add(new OneField(DataType.TIMESTAMPARRAY.getOID(), pgArrayBuilder));

            pgArrayBuilder = new PgArrayBuilder(pgUtilities);
            pgArrayBuilder.startArray();
            pgArrayBuilder.addElement((String) null);
            pgArrayBuilder.addElement(StringUtils.repeat("e", i + 1));
            pgArrayBuilder.addElement(StringUtils.repeat("e", i + 1));
            pgArrayBuilder.endArray();
            record.add(new OneField(DataType.BYTEAARRAY.getOID(), pgArrayBuilder.toString()));

            pgArrayBuilder = new PgArrayBuilder(pgUtilities);
            pgArrayBuilder.startArray();
            pgArrayBuilder.addElement((String) null);
            pgArrayBuilder.addElement(text);
            pgArrayBuilder.addElement(text);
            pgArrayBuilder.endArray();
            record.add(new OneField(DataType.TEXTARRAY.getOID(), pgArrayBuilder.toString()));

            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile, 10, 3);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        assertNotNull(schema.getColumns());
        assertEquals(10, schema.getColumns().size());
        assertEquals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, schema.getType(0).asPrimitiveType().getPrimitiveTypeName());
        assertTrue(schema.getType(0).getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation); //numeric
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, schema.getType(1).asPrimitiveType().getPrimitiveTypeName());
        assertTrue(schema.getType(1).getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation); //bpchar
        assertEquals(PrimitiveType.PrimitiveTypeName.INT96, schema.getType(2).asPrimitiveType().getPrimitiveTypeName());
        assertNull(schema.getType(2).getLogicalTypeAnnotation()); //timestamp
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, schema.getType(3).asPrimitiveType().getPrimitiveTypeName());
        assertNull(schema.getType(3).getLogicalTypeAnnotation()); //bytea
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, schema.getType(4).asPrimitiveType().getPrimitiveTypeName());
        assertTrue(schema.getType(4).getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation); //text

        assertListElementType(schema.asGroupType().getType(5).asGroupType().getType(0), 3, PrimitiveType.PrimitiveTypeName.INT32, null);//int_arr
        assertListElementType(schema.asGroupType().getType(6).asGroupType().getType(0), 3, PrimitiveType.PrimitiveTypeName.FLOAT, null);//float_arr
        assertListElementType(schema.asGroupType().getType(7).asGroupType().getType(0), 3, PrimitiveType.PrimitiveTypeName.INT96, null);//tm_arr
        assertListElementType(schema.asGroupType().getType(8).asGroupType().getType(0), 3, PrimitiveType.PrimitiveTypeName.BINARY, null);//bytea_arr
        assertListElementType(schema.asGroupType().getType(9).asGroupType().getType(0), 3, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType());//text_arr

        Group row0 = fileReader.read();
        Group row1 = fileReader.read();
        Group row2 = fileReader.read();

        assertEquals(new BigDecimal("1.200000000000000000"), new BigDecimal(new BigInteger(row0.getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("2.300000000000000000"), new BigDecimal(new BigInteger(row1.getBinary(0, 0).getBytes()), 18));
        assertEquals(new BigDecimal("3.400000000000000000"), new BigDecimal(new BigInteger(row2.getBinary(0, 0).getBytes()), 18));

        assertEquals("", row0.getString(1, 0));
        assertEquals("d", row1.getString(1, 0));
        assertEquals("dd", row2.getString(1, 0));

        Instant timestamp0 = Instant.parse("2020-08-01T04:00:05Z"); // UTC
        ZonedDateTime localTime0 = timestamp0.atZone(ZoneId.systemDefault());
        String localTimestampString0 = localTime0.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // should be "2020-08-%02dT04:00:05Z" in PST
        assertEquals(localTimestampString0, bytesToTimestamp(row0.getInt96(2, 0).getBytes()));
        Instant timestamp1 = Instant.parse("2020-08-02T04:00:05Z"); // UTC
        ZonedDateTime localTime1 = timestamp1.atZone(ZoneId.systemDefault());
        String localTimestampString1 = localTime1.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // should be "2020-08-%02dT04:00:05Z" in PST
        assertEquals(localTimestampString1, bytesToTimestamp(row1.getInt96(2, 0).getBytes()));
        Instant timestamp2 = Instant.parse("2020-08-03T04:00:05Z"); // UTC
        ZonedDateTime localTime2 = timestamp2.atZone(ZoneId.systemDefault());
        String localTimestampString2 = localTime2.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // should be "2020-08-%02dT04:00:05Z" in PST
        assertEquals(localTimestampString2, bytesToTimestamp(row2.getInt96(2, 0).getBytes()));

        assertEquals(Binary.fromString("e"), row0.getBinary(3, 0));
        assertEquals(Binary.fromString("ee"), row1.getBinary(3, 0));
        assertEquals(Binary.fromString("eee"), row2.getBinary(3, 0));

        assertEquals("f", row0.getString(4, 0));
        assertEquals("ff", row1.getString(4, 0));
        assertEquals("fff", row2.getString(4, 0));

        assertListElementValue(row0.asGroup().getGroup(5, 0), 1, PrimitiveType.PrimitiveTypeName.INT32, null, new Object[]{0});
        assertListElementValue(row1.asGroup().getGroup(5, 0), 1, PrimitiveType.PrimitiveTypeName.INT32, null, new Object[]{1});
        assertListElementValue(row2.asGroup().getGroup(5, 0), 1, PrimitiveType.PrimitiveTypeName.INT32, null, new Object[]{2});

        assertListElementValue(row0.asGroup().getGroup(6, 0), 1, PrimitiveType.PrimitiveTypeName.FLOAT, null, new Object[]{(float) 0.01});
        assertListElementValue(row1.asGroup().getGroup(6, 0), 1, PrimitiveType.PrimitiveTypeName.FLOAT, null, new Object[]{(float) 1.01});
        assertListElementValue(row2.asGroup().getGroup(6, 0), 1, PrimitiveType.PrimitiveTypeName.FLOAT, null, new Object[]{(float) 2.01});

        assertListElementValue(row0.asGroup().getGroup(7, 0), 1, PrimitiveType.PrimitiveTypeName.INT96, null, new Object[]{localTimestampString0});
        assertListElementValue(row1.asGroup().getGroup(7, 0), 1, PrimitiveType.PrimitiveTypeName.INT96, null, new Object[]{localTimestampString1});
        assertListElementValue(row2.asGroup().getGroup(7, 0), 1, PrimitiveType.PrimitiveTypeName.INT96, null, new Object[]{localTimestampString2});

        assertListElementValue(row0.asGroup().getGroup(8, 0), 1, PrimitiveType.PrimitiveTypeName.BINARY, null, new Object[]{Binary.fromString("e")});
        assertListElementValue(row1.asGroup().getGroup(8, 0), 1, PrimitiveType.PrimitiveTypeName.BINARY, null, new Object[]{Binary.fromString("ee")});
        assertListElementValue(row2.asGroup().getGroup(8, 0), 1, PrimitiveType.PrimitiveTypeName.BINARY, null, new Object[]{Binary.fromString("eee")});

        assertListElementValue(row0.asGroup().getGroup(9, 0), 1, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType(), new Object[]{"f"});
        assertListElementValue(row1.asGroup().getGroup(9, 0), 1, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType(), new Object[]{"ff"});
        assertListElementValue(row2.asGroup().getGroup(9, 0), 1, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType(), new Object[]{"fff"});

        assertNull(fileReader.read());
        fileReader.close();
    }

    // parquet doesn't keep timezone information, gpdb will convert tmtz into UTC timestamp
    @Test
    public void testWriteTimestampWithTimezone() throws Exception {
        String path = temp + "/out/timestamp_with_timezone/";
        columnDescriptors.add(new ColumnDescriptor("tmtz", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), 0, "timestamp_with_timezone", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123484");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        for (int i = 0; i < 10; i++) {
            Instant timestamp = Instant.parse("2020-08-01T04:00:05Z"); // UTC
            ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
            String localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx")); // 2020-06-28 04:30:00-07:00
            List<OneField> record = Collections.singletonList(new OneField(DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), localTimestampString));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        // Physical type is INT96
        Type type = schema.getType(0);
        assertEquals(PrimitiveType.PrimitiveTypeName.INT96, type.asPrimitiveType().getPrimitiveTypeName());
        assertNull(type.getLogicalTypeAnnotation());

        for (int i = 0; i < 10; i++) {
            Instant timestamp = Instant.parse("2020-08-01T04:00:05Z"); // UTC
            ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
            //parquet doesn't keep timezone information
            String localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // 2020-06-28 04:30:00
            assertEquals(localTimestampString, bytesToTimestamp(fileReader.read().getInt96(0, 0).getBytes()));
        }
        assertNull(fileReader.read());
        fileReader.close();
    }

    @Test
    public void testWriteTimestampWithTimezoneArray() throws Exception {
        String path = temp + "/out/timestamp_with_timezone_array/";

        columnDescriptors.add(new ColumnDescriptor("tmtz_array", DataType.TIMESTAMP_WITH_TIMEZONE_ARRAY.getOID(), 0, "tmtz_array", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123485");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        String[] values = generateLocalTimestampStrings(ZoneId.systemDefault());
        insertParquetArrayData(DataType.TIMESTAMP_WITH_TIMEZONE_ARRAY.getOID(), values, resolver, accessor);

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        Object[][] expectedValues = new Object[values.length][3];
        for (int i = 0; i < values.length; i++) {
            if (i == values.length - 1) {
                continue;
            }
            for (int j = 0; j < 3; j++) {
                if (j != 0) {
                    // Convert timestamp with timezone string into instance with given offset
                    OffsetDateTime odtInstanceAtOffset = OffsetDateTime.parse(values[i], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"));
                    // Instance in UTC
                    OffsetDateTime odtInstanceAtUTC = odtInstanceAtOffset.withOffsetSameInstant(ZoneOffset.UTC);
                    // Formatting to UTC string
                    String timestampInUTC = odtInstanceAtUTC.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"));
                    Instant timestamp = Instant.parse(timestampInUTC);
                    // Reformat UTC string into local timestamp string
                    ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
                    //parquet doesn't keep timezone information
                    String localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    expectedValues[i][j] = localTimestampString;
                }
            }
        }
        assertList(schema.getType(0), fileReader, expectedValues, PrimitiveType.PrimitiveTypeName.INT96, null);

        fileReader.close();
    }

    @Test
    public void testWriteUnsupportedComplexWithSchemaFile() throws Exception {
        String path = temp + "/out/unsupported_map/";

        columnDescriptors.add(new ColumnDescriptor("unsupported_map", DataType.UNSUPPORTED_TYPE.getOID(), 0, "unsupported_map", null));

        String filepath = this.getClass().getClassLoader().getResource("parquet/unsupported_map_type.schema").getPath();
        context.addOption("SCHEMA", filepath);
        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123486");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> accessor.openForWrite());
        assertEquals("Parquet complex type MAP is not supported.", e.getMessage());
    }

    @Test
    public void testWriteMultiDimensionalList() throws Exception {
        String path = temp + "/out/unsupported_list_of_lists/";

        columnDescriptors.add(new ColumnDescriptor("list_of_lists", DataType.INT4ARRAY.getOID(), 0, "list_of_lists", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123487");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());
        List<OneField> record = Collections.singletonList(new OneField(DataType.INT4ARRAY.getOID(), "{{1,2,3},{4,5,6}}"));
        Exception e = assertThrows(PxfRuntimeException.class,
                () -> resolver.setFields(record));
        assertEquals("Error parsing array element: {1,2,3} was not of expected type INT32", e.getMessage());
    }

    @Test
    public void testWriteMultiDimensionalListWithSchemaFile() {
        String path = temp + "/out/unsupported_list_of_lists/";

        columnDescriptors.add(new ColumnDescriptor("list_of_lists", DataType.INT4ARRAY.getOID(), 0, "list_of_lists", null));

        String filepath = this.getClass().getClassLoader().getResource("parquet/unsupported_list_of_lists_type.schema").getPath();
        context.addOption("SCHEMA", filepath);

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123488");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> accessor.openForWrite());
        assertEquals("Parquet LIST of LIST is not supported.", e.getMessage());
    }

    @Test
    public void testWriteInvalidListSchema() {
        String path = temp + "/out/invalid_list_schema/";

        columnDescriptors.add(new ColumnDescriptor("invalid_list_schema", DataType.BOOLARRAY.getOID(), 0, "invalid_list_schema", null));

        String filepath = this.getClass().getClassLoader().getResource("parquet/invalid_list_schema.schema").getPath();
        context.addOption("SCHEMA", filepath);

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123489");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        // need to add an elementType validation, but when should we throw exception? when getting schema or when parsing values?
        Exception e = assertThrows(PxfRuntimeException.class,
                () -> accessor.openForWrite());
        assertEquals("Invalid Parquet List schema: optional group bool_arr (LIST) {\n" +
                "  repeated group bag {\n" +
                "  }\n" +
                "}.", e.getMessage());
    }

    @Test
    public void testWriteInconsistencyBetweenProvidedSchemaAndDataType() {
        String path = temp + "/out/test_inconsistency/";

        columnDescriptors.add(new ColumnDescriptor("test_inconsistency", DataType.BOOLEAN.getOID(), 0, "test_inconsistency", null));

        String filepath = this.getClass().getClassLoader().getResource("parquet/test_inconsistency.schema").getPath();
        context.addOption("SCHEMA", filepath);

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123490");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        // need to add an elementType validation, but when should we throw exception? when getting schema or when parsing values?
        Exception e = assertThrows(PxfRuntimeException.class,
                () -> accessor.openForWrite());
        assertEquals("The Greenplum data type 1000 converted from schema at index 0 doesn't match expected Greenplum data type 16.", e.getMessage());
    }

    private MessageType validateFooter(Path parquetFile) throws IOException {
        return validateFooter(parquetFile, 1, 10);
    }

    private MessageType validateFooter(Path parquetFile, int numCols, int numRows) throws IOException {

        ParquetReadOptions parquetReadOptions = HadoopReadOptions
                .builder(configuration)
                .build();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(parquetFile, configuration);

        try (ParquetFileReader parquetFileReader =
                     ParquetFileReader.open(inputFile, parquetReadOptions)) {
            FileMetaData metadata = parquetFileReader.getFileMetaData();

            ParquetMetadata readFooter = parquetFileReader.getFooter();
            assertEquals(1, readFooter.getBlocks().size()); // one block

            BlockMetaData block0 = readFooter.getBlocks().get(0);
            assertEquals(numCols, block0.getColumns().size()); // one column
            assertEquals(numRows, block0.getRowCount()); // 10 rows in this block

            ColumnChunkMetaData column0 = block0.getColumns().get(0);
            assertEquals(CompressionCodecName.SNAPPY, column0.getCodec());
            return metadata.getSchema();
        }
    }

    // all the inserted values pxf gets from greenplum are converted into String along with their datatype.
    private void insertParquetArrayData(int oid, String[] values, Resolver resolver, Accessor accessor) throws Exception {
        for (int i = 0; i < values.length; i++) {
            List<OneField> record;
            if (i != values.length - 1) {
                pgArrayBuilder = new PgArrayBuilder(pgUtilities);
                pgArrayBuilder.startArray();
                pgArrayBuilder.addElement((String) null);
                pgArrayBuilder.addElement(values[i]);
                pgArrayBuilder.addElement(values[i]);
                pgArrayBuilder.endArray();
                record = Collections.singletonList(new OneField(oid, pgArrayBuilder.toString()));
            } else {
                record = Collections.singletonList(new OneField(oid, null));
            }
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }
    }

    private String[] generateLocalTimestampStrings(ZoneId zoneId) {
        String[] localTimestampStrings = new String[10];
        for (int i = 0; i < 10; i++) {
            if (i != 9) {
                Instant timestamp = Instant.parse(String.format("2020-08-%02dT04:00:05Z", i + 1)); // UTC
                if (zoneId == null) {
                    ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
                    localTimestampStrings[i] = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                } else {
                    ZonedDateTime localTime = timestamp.atZone(zoneId);
                    localTimestampStrings[i] = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx"));
                }
            }
        }
        return localTimestampStrings;
    }

    private void assertList(Type outerType, ParquetReader<Group> fileReader, Object[][] expectedValues, PrimitiveType.PrimitiveTypeName elementTypeName, LogicalTypeAnnotation logicalTypeAnnotation) throws IOException {
        for (int i = 0; i < expectedValues.length; i++) {
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            Group outerGroup = fileReader.read();
            if (i != expectedValues.length - 1) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);
                assertListElementType(outerType.asGroupType().getType(0), repetitionCount, elementTypeName, logicalTypeAnnotation);
                assertListElementValue(outerGroup.getGroup(0, 0), repetitionCount, elementTypeName, logicalTypeAnnotation, expectedValues[i]);
            } else {
                // if the array is a null array, the outer group should have no field
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }

        }
    }

    private void assertListElementValue(Group repeatedGroup, int repetitionCount, PrimitiveType.PrimitiveTypeName elementTypeName, LogicalTypeAnnotation logicalTypeAnnotation, Object[] expectedValues) {
        for (int j = 0; j < repetitionCount; j++) {
            Group elementGroup = repeatedGroup.getGroup(0, j);
            // the first element is null, so repetitonCount should be 0
            if (j == 0) {
                assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                continue;
            }
            // only one  element in the repeated list, the repetition count should be 1
            assertEquals(1, elementGroup.getFieldRepetitionCount(0));
            switch (elementTypeName) {
                case INT32:
                    if (logicalTypeAnnotation != null &&
                            logicalTypeAnnotation.toOriginalType() == LogicalTypeAnnotation.IntLogicalTypeAnnotation.intType(16, true).toOriginalType()) {
                        assertEquals((short) expectedValues[j], (short) elementGroup.getInteger(0, 0));
                    } else {
                        assertEquals((Integer) expectedValues[j], elementGroup.getInteger(0, 0));
                    }
                    break;
                case INT96:
                    assertEquals(expectedValues[j], bytesToTimestamp(elementGroup.getInt96(0, 0).getBytes()));
                    break;
                case FLOAT:
                    assertEquals((Float) expectedValues[j], elementGroup.getFloat(0, 0));
                    break;
                case BINARY:
                    if (logicalTypeAnnotation != null) {
                        assertEquals(expectedValues[j], elementGroup.getBinary(0, 0).toStringUsingUTF8());
                    } else {
                        assertEquals(expectedValues[j], elementGroup.getBinary(0, 0));
                    }
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    assertEquals(expectedValues[j], new BigDecimal(new BigInteger(elementGroup.getBinary(0, 0).getBytes()), 18));
                    break;
                default:
                    break;
            }

        }
    }

    private void assertListElementType(Type repeatedType, int repetitionCount, PrimitiveType.PrimitiveTypeName expectedElementTypeName, LogicalTypeAnnotation logicalTypeAnnotation) {
        //repeated group must use "repeated" keyword
        assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
        for (int j = 0; j < repetitionCount; j++) {
            Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
            // Physical type is binary, logical type is String
            assertEquals(expectedElementTypeName, elementType.asPrimitiveType().getPrimitiveTypeName());
            assertEquals(elementType.getLogicalTypeAnnotation(), logicalTypeAnnotation);
        }
    }
}
