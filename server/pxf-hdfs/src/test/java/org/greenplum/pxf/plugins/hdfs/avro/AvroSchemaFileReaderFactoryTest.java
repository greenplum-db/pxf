package org.greenplum.pxf.plugins.hdfs.avro;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroSchemaFileReaderFactoryTest {

    private AvroSchemaFileReaderFactory avroSchemaFileReaderFactory;

    @BeforeEach
    public void setup() {
        avroSchemaFileReaderFactory = new AvroSchemaFileReaderFactory();
    }

    @Test
    public void testGetJsonReader() {
        assertTrue(avroSchemaFileReaderFactory.getAvroSchemaFileReader("foobar.avsc") instanceof JsonAvroSchemaFileReader);
    }

    @Test
    public void testGetBinaryReader() {
        assertTrue(avroSchemaFileReaderFactory.getAvroSchemaFileReader("foobar.avro") instanceof BinaryAvroSchemaFileReader);
    }
}
