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
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
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

public class ParquetWriteTest {

    protected List<ColumnDescriptor> columnDescriptors;
    @TempDir
    File temp; // must be non-private
    private Accessor accessor;
    private Resolver resolver;
    private RequestContext context;
    private Configuration configuration;

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

        // write parquet file with int array values
        for (int i = 0; i < 10; i++) {
            List<Integer> intList = null;
            if (i != 9) {
                intList = new ArrayList<>();
                intList.add(null);
                intList.add(i);
                intList.add(i);
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.INT4ARRAY.getOID(), intList));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();
            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one  element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    assertEquals(PrimitiveType.PrimitiveTypeName.INT32, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertNull(elementType.getLogicalTypeAnnotation());
                    Integer res = elementGroup.getInteger(0, 0);
                    assertEquals(i, res);
                }
            } else {// the last row is a null array
                // if the array is a null array, the outer group should have no field
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }

        }
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

        // write parquet array with TEXT values of a and b, repeated i + 1 times
        for (int i = 0; i < 10; i++) {
            List<String> textList = null;
            if (i != 9) {
                textList = new ArrayList<>();
                textList.add(null);
                textList.add(StringUtils.repeat("a", i + 1));
                textList.add(StringUtils.repeat("b", i + 1));
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.TEXTARRAY.getOID(), textList));
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

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();

            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one  element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    // Physical type is binary, logical type is String
                    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertTrue(elementType.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation);
                    Binary binary = elementGroup.getBinary(0, 0);
                    String str = binary.toStringUsingUTF8();
                    if (j % 2 == 1) {
                        assertEquals(StringUtils.repeat("a", i + 1), str);
                    } else {
                        assertEquals(StringUtils.repeat("b", i + 1), str);
                    }
                }
            } else {
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }
        }
        fileReader.close();
    }

    //TODO
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
        for (int i = 0; i < 10; i++) {
            List<String> dateList = null;
            if (i != 9) {
                dateList = new ArrayList<>();
                String date = String.format("2020-08-%02d", i + 1);
                dateList.add(null);
                dateList.add(date);
                dateList.add(date);
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.DATE.getOID(), dateList));
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

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();

            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one  element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    // Physical type is INT32, logical type is DATE
                    assertEquals(PrimitiveType.PrimitiveTypeName.INT32, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertTrue(elementType.getLogicalTypeAnnotation() instanceof DateLogicalTypeAnnotation);

                    int epoch = elementGroup.getInteger(0, 0);
                    assertEquals(18475 + i, epoch);
                }
            } else {
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }
        }
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

        // write parquet file with double array values
        for (int i = 0; i < 10; i++) {
            List<Double> doubleList = null;
            if (i != 9) {
                doubleList = new ArrayList<>();
                doubleList.add(null);
                doubleList.add((double) i * 1.01);
                doubleList.add((double) i * 1.01);
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.FLOAT8ARRAY.getOID(), doubleList));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();
            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one  element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    assertEquals(PrimitiveType.PrimitiveTypeName.DOUBLE, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertNull(elementType.getLogicalTypeAnnotation());
                    Double res = elementGroup.getDouble(0, 0);
                    assertEquals(i * 1.01, res, 0.001);
                }
            } else {// the last row is a null array
                // if the array is a null array, the outer group should have no field
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }

        }
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

        // write parquet file with boolean array values
        Boolean b = true;
        for (int i = 0; i < 10; i++) {
            List<Boolean> boolList = null;
            if (i != 9) {
                boolList = new ArrayList<>();
                boolList.add(null);
                boolList.add(b);
                boolList.add(b);
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.BOOLARRAY.getOID(), boolList));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
            b ^= true;
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();
            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    assertEquals(PrimitiveType.PrimitiveTypeName.BOOLEAN, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertNull(elementType.getLogicalTypeAnnotation());
                    Boolean res = elementGroup.getBoolean(0, 0);
                    if (i % 2 == 0) {
                        assertTrue(res);
                    } else if (i % 2 == 1) {
                        assertFalse(res);
                    }
                }
            } else {// the last row is a null array
                // if the array is a null array, the outer group should have no field
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }

        }
        fileReader.close();
    }

    //TODO
    @Test
    public void testWriteTimestampArray() throws Exception {
        String path = temp + "/out/int/";

        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123475");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
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

        // write parquet file with int array values
        for (int i = 0; i < 10; i++) {
            List<Long> bigIntList = null;
            if (i != 9) {
                bigIntList = new ArrayList<>();
                long value = (long) Integer.MAX_VALUE + i;
                bigIntList.add(null);
                bigIntList.add(value);
                bigIntList.add(value);
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.INT8ARRAY.getOID(), bigIntList));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();
            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one  element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    assertEquals(PrimitiveType.PrimitiveTypeName.INT64, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertNull(elementType.getLogicalTypeAnnotation());
                    Long res = elementGroup.getLong(0, 0);
                    assertEquals((long) Integer.MAX_VALUE + i, res);
                }
            } else {// the last row is a null array
                // if the array is a null array, the outer group should have no field
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }

        }
        fileReader.close();

    }

    //TODO
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

        // write parquet file with int array values
        for (int i = 0; i < 10; i++) {
            List<Integer> smallIntList = null;
            if (i != 9) {
                smallIntList = new ArrayList<>();
                smallIntList.add(null);
                smallIntList.add(i);
                smallIntList.add(i);
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.INT2ARRAY.getOID(), smallIntList));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();
            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one  element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    assertEquals(PrimitiveType.PrimitiveTypeName.INT32, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertEquals(LogicalTypeAnnotation.intType(16, true), elementType.getLogicalTypeAnnotation());
                    Integer res = elementGroup.getInteger(0, 0);
                    assertEquals(i, res);
                }
            } else {// the last row is a null array
                // if the array is a null array, the outer group should have no field
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }

        }
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

        // write parquet file with double array values
        for (int i = 0; i < 10; i++) {
            List<Float> floatList = null;
            if (i != 9) {
                floatList = new ArrayList<>();
                floatList.add(null);
                floatList.add(i * 1.01F);
                floatList.add(i * 1.01F);
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.FLOAT4ARRAY.getOID(), floatList));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();
            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one  element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    assertEquals(PrimitiveType.PrimitiveTypeName.FLOAT, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertNull(elementType.getLogicalTypeAnnotation());
                    Float res = elementGroup.getFloat(0, 0);
                    assertEquals(i * 1.01F, res, 0.01);
                }
            } else {// the last row is a null array
                // if the array is a null array, the outer group should have no field
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }

        }
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

        // write parquet file with char values
        for (int i = 0; i < 10; i++) {
            List<String> bpcharList = null;
            if (i != 9) {
                bpcharList = new ArrayList<>();
                bpcharList.add(null);
                String s = StringUtils.repeat("b", i % 5);
                bpcharList.add(s);
                bpcharList.add(s);
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.VARCHARARRAY.getOID(), bpcharList));
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

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();

            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one  element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    // Physical type is binary, logical type is String
                    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertTrue(elementType.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation);

                    Binary binary = elementGroup.getBinary(0, 0);
                    String str = binary.toStringUsingUTF8();
                    assertEquals(StringUtils.repeat("b", i % 5), str);
                }
            } else {
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }
        }
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

        // write parquet file with char values
        for (int i = 0; i < 10; i++) {
            List<String> bpcharList = null;
            if (i != 9) {
                bpcharList = new ArrayList<>();
                bpcharList.add(null);
                String s = StringUtils.repeat("c", i % 3);
                bpcharList.add(s);
                bpcharList.add(s);
            }
            List<OneField> record = Collections.singletonList(new OneField(DataType.BPCHARARRAY.getOID(), bpcharList));
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

        for (int i = 0; i < 10; i++) {
            Type outerType = schema.getType(0);
            assertNotNull(outerType.getLogicalTypeAnnotation());
            assertEquals(LogicalTypeAnnotation.listType(), outerType.getLogicalTypeAnnotation());

            // get the outer group
            Group outerGroup = fileReader.read();

            if (i != 9) {
                // if the array is not a null array, the outer group should only have one field
                assertEquals(1, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));

                // get the repeated list group
                Group repeatedGroup = outerGroup.getGroup(0, 0);
                Type repeatedType = outerType.asGroupType().getType(0);
                //repeated group must use "repeated" keyword
                assertEquals(Type.Repetition.REPEATED, repeatedType.getRepetition());
                int repetitionCount = repeatedGroup.getFieldRepetitionCount(repeatedType.asGroupType().getName());
                assertEquals(3, repetitionCount);

                for (int j = 0; j < repetitionCount; j++) {
                    Group elementGroup = repeatedGroup.getGroup(0, j);
                    if (j == 0) {// have a null element in the repeated list, the repetition count should be 0
                        assertEquals(0, elementGroup.getFieldRepetitionCount(0));
                        continue;
                    }
                    // only one  element in the repeated list, the repetition count should be 1
                    assertEquals(1, elementGroup.getFieldRepetitionCount(0));
                    Type elementType = repeatedType.asGroupType().getType(0).asPrimitiveType();
                    // Physical type is binary, logical type is String
                    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, elementType.asPrimitiveType().getPrimitiveTypeName());
                    assertTrue(elementType.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation);

                    Binary binary = elementGroup.getBinary(0, 0);
                    String str = binary.toStringUsingUTF8();
                    assertEquals(StringUtils.repeat("c", i % 3), str);
                }
            } else {
                assertEquals(0, outerGroup.getFieldRepetitionCount(outerType.asGroupType().getName()));
            }
        }
        fileReader.close();
    }

    //TODO
    @Test
    public void testWriteNumericArray() throws Exception {
        String path = temp + "/out/int/";

        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123482");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
    }

    //TODO
    @Test
    public void testWriteMultipleTypes() throws Exception {
        String path = temp + "/out/multiple/";
        columnDescriptors.add(new ColumnDescriptor("dec1", DataType.NUMERIC.getOID(), 0, "numeric", null));
        columnDescriptors.add(new ColumnDescriptor("c1", DataType.BPCHAR.getOID(), 1, "char", new Integer[]{3}));
        columnDescriptors.add(new ColumnDescriptor("tm", DataType.TIMESTAMP.getOID(), 2, "timestamp", null));
        columnDescriptors.add(new ColumnDescriptor("bin", DataType.BYTEA.getOID(), 3, "bytea", null));
        columnDescriptors.add(new ColumnDescriptor("name", DataType.TEXT.getOID(), 4, "text", null));

        context.setDataSource(path);
        context.setTransactionId("XID-XYZ-123483");

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        assertTrue(accessor.openForWrite());

        // write parquet file with bigint values
        for (int i = 0; i < 3; i++) {
            List<OneField> record = new ArrayList<>();
            record.add(new OneField(DataType.NUMERIC.getOID(), String.format("%d.%d", (i + 1), (i + 2))));

            String s = StringUtils.repeat("d", i % 3);
            record.add(new OneField(DataType.BPCHAR.getOID(), s));

            Instant timestamp = Instant.parse(String.format("2020-08-%02dT04:00:05Z", i + 1)); // UTC
            ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
            String localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); // should be "2020-08-%02dT04:00:05Z" in PST
            record.add(new OneField(DataType.TIMESTAMP.getOID(), localTimestampString));

            byte[] value = Binary.fromString(StringUtils.repeat("e", i + 1)).getBytes();
            record.add(new OneField(DataType.BYTEA.getOID(), value));

            record.add(new OneField(DataType.TEXT.getOID(), StringUtils.repeat("f", i + 1)));
            OneRow rowToWrite = resolver.setFields(record);
            assertTrue(accessor.writeNextObject(rowToWrite));
        }

        accessor.closeForWrite();

        // Validate write
        Path expectedFile = new Path(HcfsType.FILE.getUriForWrite(context) + ".snappy.parquet");
        assertTrue(expectedFile.getFileSystem(configuration).exists(expectedFile));

        MessageType schema = validateFooter(expectedFile, 5, 3);

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), expectedFile)
                .withConf(configuration)
                .build();

        assertNotNull(schema.getColumns());
        assertEquals(5, schema.getColumns().size());
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
        assertNull(fileReader.read());
        fileReader.close();
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
}
