package org.greenplum.pxf.api.utilities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import lombok.Getter;
import org.greenplum.pxf.api.examples.DemoFragmentMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FragmentMetadataSerDeTest {

    private FragmentMetadataSerDe metadataSerDe;

    @BeforeEach
    public void setup() {
        metadataSerDe = FragmentMetadataSerDe.getInstance();
    }

    @Test
    public void testSingleton() {
        assertSame(metadataSerDe, FragmentMetadataSerDe.getInstance());
    }

    @Test
    public void testSerialize() throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        SimpleModule module = new SimpleModule();
        module.addSerializer(FragmentMetadata.class, FragmentMetadataSerDe.getInstance());
        mapper.registerModule(module);

        DemoFragmentMetadata metadata = new DemoFragmentMetadata("abc");
        assertEquals("\"{\\\"path\\\":\\\"abc\\\",\\\"className\\\":\\\"org.greenplum.pxf.api.examples.DemoFragmentMetadata\\\"}\"", mapper.writeValueAsString(metadata));

        TestFragmentMetadata testMetadata = new TestFragmentMetadata("test", 5, 10, new Date(1590649200000L));
        assertEquals("\"{\\\"a\\\":\\\"test\\\",\\\"b\\\":5,\\\"c\\\":10,\\\"d\\\":1590649200000,\\\"className\\\":\\\"org.greenplum.pxf.api.utilities.FragmentMetadataSerDeTest$TestFragmentMetadata\\\"}\"",
                mapper.writeValueAsString(testMetadata));
    }

    @Test
    public void testDeserialize() throws JsonProcessingException {

        String metadataJson = "{\"path\": \"deserialize me\", \"className\": \"org.greenplum.pxf.api.examples.DemoFragmentMetadata\" }";

        FragmentMetadata metadata = metadataSerDe.deserialize(metadataJson);
        assertNotNull(metadata);
        assertTrue(metadata instanceof DemoFragmentMetadata);
        assertEquals("deserialize me", ((DemoFragmentMetadata) metadata).getPath());

        String testMetadataJson = "{\"b\": 25, \"c\": 150, \"a\": \"test me\", \"d\": \"1590649200000\", \"className\": \"org.greenplum.pxf.api.utilities.FragmentMetadataSerDeTest$TestFragmentMetadata\"}";

        FragmentMetadata testMetadata = metadataSerDe.deserialize(testMetadataJson);
        assertNotNull(testMetadata);
        assertTrue(testMetadata instanceof TestFragmentMetadata);
        TestFragmentMetadata testFragmentMetadata = (TestFragmentMetadata) testMetadata;
        assertEquals("test me", testFragmentMetadata.getA());
        assertEquals(25, testFragmentMetadata.getB());
        assertEquals(150, testFragmentMetadata.getC());
        assertEquals(new Date(1590649200000L), testFragmentMetadata.getD());
    }

    static class TestFragmentMetadata implements FragmentMetadata {

        @Getter
        private final String a;

        @Getter
        private final int b;

        @Getter
        private final int c;

        @Getter
        private final Date d;

        @JsonCreator
        public TestFragmentMetadata(
                @JsonProperty("a") String a,
                @JsonProperty("b") int b,
                @JsonProperty("c") int c,
                @JsonProperty("d") Date d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }
    }

}