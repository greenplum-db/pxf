package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HcfsFragmentMetadataTest {

    @Test
    public void testFileSplitConstructor() {
        HcfsFragmentMetadata metadata = new HcfsFragmentMetadata(new FileSplit(new Path("foo"), 5, 25, (String[]) null));
        assertEquals(5, metadata.getStart());
        assertEquals(25, metadata.getLength());
        assertEquals("org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata", metadata.getClassName());
    }

    @Test
    public void testConstructor() {
        HcfsFragmentMetadata metadata = new HcfsFragmentMetadata(10, 200);
        assertEquals(10, metadata.getStart());
        assertEquals(200, metadata.getLength());
        assertEquals("org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata", metadata.getClassName());
    }
}