package org.greenplum.pxf.api.utilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.greenplum.pxf.api.examples.DemoFragmentMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FragmentMetadataSerDeTest {

    private FragmentMetadataSerDe metadataSerDe;

    @BeforeEach
    public void setup() {
        metadataSerDe = new FragmentMetadataSerDe(new SerializationService());
    }

    @Test
    public void testSerialize() throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        SimpleModule module = new SimpleModule();
        module.addSerializer(FragmentMetadata.class, metadataSerDe);
        mapper.registerModule(module);

        DemoFragmentMetadata metadata = new DemoFragmentMetadata("abc");
        assertEquals("\"AQBvcmcuZ3JlZW5wbHVtLnB4Zi5hcGkuZXhhbXBsZXMuRGVtb0ZyYWdtZW50TWV0YWRhdOEBAWFi4w==\"",
                mapper.writeValueAsString(metadata));

        TestFragmentMetadata testMetadata = new TestFragmentMetadata("test", 5, 10, new Date(1590649200000L), "foo".getBytes(StandardCharsets.UTF_8));
        assertEquals("\"AQDPAW9yZy5ncmVlbnBsdW0ucHhmLmFwaS51dGlsaXRpZXMuRnJhZ21lbnRNZXRhZGF0YVNlckRlVGVzdCRUZXN0RnJhZ21lbnRNZXRhZGF0YQEBdGVz9AoUAQFqYXZhLnNxbC5EYXTlAYC70tClLgEEZm9v\"",
                mapper.writeValueAsString(testMetadata));
    }

    @Test
    public void testDeserialize() {

        String metadataString = "\"AQBvcmcuZ3JlZW5wbHVtLnB4Zi5hcGkuZXhhbXBsZXMuRGVtb0ZyYWdtZW50TWV0YWRhdOEBAWFi4w==\"";

        FragmentMetadata metadata = metadataSerDe.deserialize(metadataString);
        assertNotNull(metadata);
        assertTrue(metadata instanceof DemoFragmentMetadata);
        assertEquals("abc", ((DemoFragmentMetadata) metadata).getPath());

        String testMetadataString = "\"AQDPAW9yZy5ncmVlbnBsdW0ucHhmLmFwaS51dGlsaXRpZXMuRnJhZ21lbnRNZXRhZGF0YVNlckRlVGVzdCRUZXN0RnJhZ21lbnRNZXRhZGF0YQEBdGVz9AoUAQFqYXZhLnNxbC5EYXTlAYC70tClLgEEZm9v\"";

        FragmentMetadata testMetadata = metadataSerDe.deserialize(testMetadataString);
        assertNotNull(testMetadata);
        assertTrue(testMetadata instanceof TestFragmentMetadata);
        TestFragmentMetadata testFragmentMetadata = (TestFragmentMetadata) testMetadata;
        assertEquals("test", testFragmentMetadata.getA());
        assertEquals(5, testFragmentMetadata.getB());
        assertEquals(10, testFragmentMetadata.getC());
        assertEquals(new Date(1590649200000L), testFragmentMetadata.getD());
        assertEquals("foo", new String(testFragmentMetadata.getE(), StandardCharsets.UTF_8));
    }

    @NoArgsConstructor
    @Getter
    static class TestFragmentMetadata implements FragmentMetadata {

        private String a;
        private int b;
        private int c;
        private Date d;
        private byte[] e;

        public TestFragmentMetadata(String a, int b, int c, Date d, byte[] e) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
            this.e = e;
        }
    }
}