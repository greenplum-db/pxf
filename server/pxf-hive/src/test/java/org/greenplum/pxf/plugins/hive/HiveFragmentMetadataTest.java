package org.greenplum.pxf.plugins.hive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.api.utilities.FragmentMetadataSerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HiveFragmentMetadataTest {

    private HiveFragmentMetadata metadata;
    private List<Integer> indexes;
    private Properties properties;

    @BeforeEach
    public void setup() {
        properties = new Properties();
        indexes = Arrays.asList(5, 6);
        metadata = new HiveFragmentMetadata(
                5L,
                25L,
                "input format name",
                "serde class name",
                properties,
                "partition keys",
                true,
                "delimiter",
                "column types",
                208,
                indexes,
                "all column names",
                "all column types"
        );
    }

    @Test
    public void testJsonCreatorConstructor() {

        assertEquals(5L, metadata.getStart());
        assertEquals(25L, metadata.getLength());
        assertEquals("input format name", metadata.getInputFormatName());
        assertEquals("serde class name", metadata.getSerdeClassName());
        assertSame(properties, metadata.getProperties());
        assertEquals("partition keys", metadata.getPartitionKeys());
        assertTrue(metadata.isFilterInFragmenter());
        assertEquals("delimiter", metadata.getDelimiter());
        assertEquals("column types", metadata.getColTypes());
        assertEquals(208, metadata.getSkipHeader());
        assertEquals(indexes, metadata.getHiveIndexes());
        assertEquals("all column names", metadata.getAllColumnNames());
        assertEquals("all column types", metadata.getAllColumnTypes());
    }

    @Test
    public void testEmptyBuilder() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
        assertEquals("org.greenplum.pxf.plugins.hive.HiveFragmentMetadata", metadata.getClassName());
    }

    @Test
    public void testBuilderWithFileSplit() {

        FileSplit mockFileSplit = mock(FileSplit.class);
        when(mockFileSplit.getStart()).thenReturn(25L);
        when(mockFileSplit.getLength()).thenReturn(150L);

        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withFileSplit(mockFileSplit)
                .build();

        assertEquals(25L, metadata.getStart());
        assertEquals(150L, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithStart() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withStart(21L)
                .build();

        assertEquals(21L, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithLength() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withLength(84L)
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(84L, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithInputFormatName() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withInputFormatName("someInputFormatName")
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertEquals("someInputFormatName", metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithSerdeClassName() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withSerdeClassName("some serializer deserializer")
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertEquals("some serializer deserializer", metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithProperties() {

        Properties properties = new Properties();
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withProperties(properties)
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertSame(properties, metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithPartitionKeys() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withPartitionKeys("partition keys")
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertEquals("partition keys", metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithFilterInFragmenter() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withFilterInFragmenter(true)
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertTrue(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithDelimiter() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withDelimiter("delim")
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertEquals("delim", metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithColTypes() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withColTypes("column types")
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertEquals("column types", metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithSkipHeader() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withSkipHeader(125)
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(125, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithHiveIndexes() {
        List<Integer> indexes = Arrays.asList(5, 8);
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withHiveIndexes(indexes)
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNotNull(metadata.getHiveIndexes());
        assertEquals(2, metadata.getHiveIndexes().size());
        assertEquals(5, metadata.getHiveIndexes().get(0));
        assertEquals(8, metadata.getHiveIndexes().get(1));
        assertNull(metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithAllColumnNames() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withAllColumnNames("all column names")
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertEquals("all column names", metadata.getAllColumnNames());
        assertNull(metadata.getAllColumnTypes());
    }

    @Test
    public void testBuilderWithAllColumnTypes() {
        HiveFragmentMetadata metadata = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withAllColumnTypes("all column types")
                .build();

        assertEquals(0, metadata.getStart());
        assertEquals(0, metadata.getLength());
        assertNull(metadata.getInputFormatName());
        assertNull(metadata.getSerdeClassName());
        assertNull(metadata.getProperties());
        assertNull(metadata.getPartitionKeys());
        assertFalse(metadata.isFilterInFragmenter());
        assertNull(metadata.getDelimiter());
        assertNull(metadata.getColTypes());
        assertEquals(0, metadata.getSkipHeader());
        assertNull(metadata.getHiveIndexes());
        assertNull(metadata.getAllColumnNames());
        assertEquals("all column types", metadata.getAllColumnTypes());
    }

    @Test
    public void testSerialization() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(FragmentMetadata.class, FragmentMetadataSerDe.getInstance());
        mapper.registerModule(module);

        assertEquals("\"{\\\"start\\\":5,\\\"length\\\":25,\\\"inputFormatName\\\":\\\"input format name\\\",\\\"serdeClassName\\\":\\\"serde class name\\\",\\\"properties\\\":{},\\\"partitionKeys\\\":\\\"partition keys\\\",\\\"filterInFragmenter\\\":true,\\\"delimiter\\\":\\\"delimiter\\\",\\\"colTypes\\\":\\\"column types\\\",\\\"skipHeader\\\":208,\\\"hiveIndexes\\\":[5,6],\\\"allColumnNames\\\":\\\"all column names\\\",\\\"allColumnTypes\\\":\\\"all column types\\\",\\\"className\\\":\\\"org.greenplum.pxf.plugins.hive.HiveFragmentMetadata\\\"}\"",
                mapper.writeValueAsString(metadata));
    }

    @Test
    public void testDeserialization() throws JsonProcessingException {
        String json = "{\"start\":5,\"length\":25,\"inputFormatName\":\"input format name\",\"serdeClassName\":\"serde class name\",\"properties\":{},\"partitionKeys\":\"partition keys\",\"filterInFragmenter\":true,\"delimiter\":\"delimiter\",\"colTypes\":\"column types\",\"skipHeader\":208,\"hiveIndexes\":[5,6],\"allColumnNames\":\"all column names\",\"allColumnTypes\":\"all column types\",\"className\":\"org.greenplum.pxf.plugins.hive.HiveFragmentMetadata\"}";

        FragmentMetadata testMetadata = FragmentMetadataSerDe.getInstance().deserialize(json);
        assertNotNull(testMetadata);
        assertTrue(testMetadata instanceof HiveFragmentMetadata);
        metadata = (HiveFragmentMetadata) testMetadata;
        assertEquals(5L, metadata.getStart());
        assertEquals(25L, metadata.getLength());
        assertEquals("input format name", metadata.getInputFormatName());
        assertEquals("serde class name", metadata.getSerdeClassName());
        assertEquals(properties, metadata.getProperties());
        assertEquals("partition keys", metadata.getPartitionKeys());
        assertTrue(metadata.isFilterInFragmenter());
        assertEquals("delimiter", metadata.getDelimiter());
        assertEquals("column types", metadata.getColTypes());
        assertEquals(208, metadata.getSkipHeader());
        assertEquals(indexes, metadata.getHiveIndexes());
        assertEquals("all column names", metadata.getAllColumnNames());
        assertEquals("all column types", metadata.getAllColumnTypes());
    }

}