package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PxfInputFormatTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGetRecordReader() throws IOException {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("PxfInputFormat should not be used for reading data, but only for obtaining the splits of a file");
        new PxfInputFormat().getRecordReader(null, null, null);
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
        assertEquals(description, result, expected);
    }
}