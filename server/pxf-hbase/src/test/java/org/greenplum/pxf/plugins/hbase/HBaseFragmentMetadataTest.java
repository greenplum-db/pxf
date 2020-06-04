package org.greenplum.pxf.plugins.hbase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.api.utilities.FragmentMetadataSerDe;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HBaseFragmentMetadataTest {

    @Test
    public void testHRegionInfoConstructor() {
        final byte[] startKey = new byte[0];
        final byte[] endKey = new byte[0];
        final byte[] fooValue = new byte[0];
        Map<String, byte[]> columnMapping = new HashMap<>();
        columnMapping.put("foo", fooValue);

        HRegionInfo hRegionInfo = mock(HRegionInfo.class);
        when(hRegionInfo.getStartKey()).thenReturn(startKey);
        when(hRegionInfo.getEndKey()).thenReturn(endKey);

        HBaseFragmentMetadata metadata = new HBaseFragmentMetadata(hRegionInfo, columnMapping);
        assertNotNull(metadata);
        assertSame(startKey, metadata.getStartKey());
        assertSame(endKey, metadata.getEndKey());
        assertSame(columnMapping, metadata.getColumnMapping());
        assertSame(fooValue, metadata.getColumnMapping().get("foo"));
    }

    @Test
    public void testConstructor() {
        final byte[] startKey = new byte[0];
        final byte[] endKey = new byte[0];
        final byte[] fooValue = new byte[0];
        Map<String, byte[]> columnMapping = new HashMap<>();
        columnMapping.put("foo", fooValue);

        HBaseFragmentMetadata metadata = new HBaseFragmentMetadata(startKey, endKey, columnMapping);
        assertNotNull(metadata);
        assertSame(startKey, metadata.getStartKey());
        assertSame(endKey, metadata.getEndKey());
        assertSame(columnMapping, metadata.getColumnMapping());
        assertSame(fooValue, metadata.getColumnMapping().get("foo"));
    }

    @Test
    public void testSerialization() throws JsonProcessingException {
        final byte[] startKey = "a".getBytes(StandardCharsets.UTF_8);
        final byte[] endKey = "b".getBytes(StandardCharsets.UTF_8);
        final byte[] entry1 = "entry-1".getBytes(StandardCharsets.UTF_8);
        final byte[] entry2 = "entry-2".getBytes(StandardCharsets.UTF_8);

        Map<String, byte[]> columnMapping = new HashMap<>();
        columnMapping.put("1", entry1);
        columnMapping.put("2", entry2);

        HBaseFragmentMetadata metadata = new HBaseFragmentMetadata(startKey, endKey, columnMapping);

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(FragmentMetadata.class, FragmentMetadataSerDe.getInstance());
        mapper.registerModule(module);

        assertEquals("\"{\\\"startKey\\\":\\\"YQ==\\\",\\\"endKey\\\":\\\"Yg==\\\",\\\"columnMapping\\\":{\\\"1\\\":\\\"ZW50cnktMQ==\\\",\\\"2\\\":\\\"ZW50cnktMg==\\\"},\\\"className\\\":\\\"org.greenplum.pxf.plugins.hbase.HBaseFragmentMetadata\\\"}\"",
                mapper.writeValueAsString(metadata));
    }

    @Test
    public void testDeserialization() throws JsonProcessingException {
        String json = "{\"startKey\":\"YQ==\",\"endKey\":\"Yg==\",\"columnMapping\":{\"f\":\"RnJhbmNpc2Nv\",\"g\":\"UFhG\"},\"className\":\"org.greenplum.pxf.plugins.hbase.HBaseFragmentMetadata\"}";

        FragmentMetadata testMetadata = FragmentMetadataSerDe.getInstance().deserialize(json);
        assertNotNull(testMetadata);
        assertTrue(testMetadata instanceof HBaseFragmentMetadata);
        HBaseFragmentMetadata metadata = (HBaseFragmentMetadata) testMetadata;
        assertEquals("a", new String(metadata.getStartKey()));
        assertEquals("b", new String(metadata.getEndKey()));
        assertNotNull(metadata.getColumnMapping());
        assertTrue(metadata.getColumnMapping().containsKey("f"));
        assertTrue(metadata.getColumnMapping().containsKey("g"));
        assertEquals("Francisco", new String(metadata.getColumnMapping().get("f")));
        assertEquals("PXF", new String(metadata.getColumnMapping().get("g")));
    }

}