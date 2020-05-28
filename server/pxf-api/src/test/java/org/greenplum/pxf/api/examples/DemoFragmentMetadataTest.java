package org.greenplum.pxf.api.examples;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DemoFragmentMetadataTest {

    @Test
    public void testDemoFragmentMetadata() {
        DemoFragmentMetadata fragmentMetadata = new DemoFragmentMetadata("my-path");
        assertEquals("my-path", fragmentMetadata.getPath());
        assertEquals("org.greenplum.pxf.api.examples.DemoFragmentMetadata", fragmentMetadata.getClassName());
    }
}