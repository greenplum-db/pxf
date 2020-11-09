package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HiveFragmentMetadataTest {

    private HiveFragmentMetadata metadata;
    private byte[] kryoProperties;

    @BeforeEach
    public void setup() {
        kryoProperties = new HiveUtilities().toKryo(new Properties());
        metadata = new HiveFragmentMetadata(5L, 25L, kryoProperties);
    }

    @Test
    public void testJsonCreatorConstructor() {

        assertEquals(5L, metadata.getStart());
        assertEquals(25L, metadata.getLength());
        assertSame(kryoProperties, metadata.getKryoProperties());
    }

    @Test
    public void testBuilderWithFileSplit() {

        FileSplit mockFileSplit = mock(FileSplit.class);
        when(mockFileSplit.getStart()).thenReturn(25L);
        when(mockFileSplit.getLength()).thenReturn(150L);

        HiveFragmentMetadata metadata = new HiveFragmentMetadata(mockFileSplit, kryoProperties);

        assertEquals(25L, metadata.getStart());
        assertEquals(150L, metadata.getLength());
        assertSame(kryoProperties, metadata.getKryoProperties());
    }
}