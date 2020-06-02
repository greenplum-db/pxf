package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PxfInputFormatTest {

    @Test
    public void testGetRecordReader() throws IOException {
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> new PxfInputFormat().getRecordReader(null, null, null));
        assertEquals("PxfInputFormat should not be used for reading data, but only for obtaining the splits of a file", e.getMessage());
    }

    @Test
    public void isSplittableCodec() throws IOException {
        testIsSplittableCodec("no codec - splittable",
                "some/innocent.file", true);
        testIsSplittableCodec("gzip codec - not splittable",
                "/gzip.gz", false);
        testIsSplittableCodec("default codec - not splittable",
                "/default.deflate", false);
        testIsSplittableCodec("bzip2 codec - splittable",
                "bzip2.bz2", true);
    }

    private void testIsSplittableCodec(String description, String pathName, boolean expected)
            throws IOException {
        Path path = new Path(pathName);
        Configuration configuration = new Configuration();
        FileSystem fs = path.getFileSystem(configuration);

        boolean result = new PxfInputFormat().isSplitable(fs, path);
        assertEquals(result, expected, description);
    }
}
