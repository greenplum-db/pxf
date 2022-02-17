package org.greenplum.pxf.plugins.json.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;
import org.greenplum.pxf.plugins.json.JsonRecordReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class JsonRecordReaderTest {

    private static final String RECORD_MEMBER_IDENTIFIER = "json.input.format.record.identifier";
    private File file;
    private JobConf jobConf;
    private FileSplit fileSplit;
    private LongWritable key;
    private Text data;
    private RequestContext context;
    private Path path;
    private String[] hosts = null;
    private JsonRecordReader jsonRecordReader;

    @BeforeEach
    public void setup() throws IOException, URISyntaxException {
        context = new RequestContext();
        context.setConfiguration(new Configuration());

        jobConf = new JobConf(context.getConfiguration(), PartitionedJsonParserNoSeekTest.class);
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "cüstömerstätüs");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json").toURI());
        context.setDataSource(file.getPath());
        path = new Path(file.getPath());
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(jobConf);
        CompressionCodec c = new BZip2Codec();
        jobConf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");


        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
    }

    @Test
    public void testWithCodec() throws URISyntaxException, IOException {

        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json.bz2").toURI());
        path = new Path(file.getPath());
        // This file spilt will be ignored since the codec is involved
        fileSplit = new FileSplit(path, 0, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        int recordCount = 0;
        while(jsonRecordReader.next(key,data))
        {
            recordCount++;
        }
        assertEquals(5,recordCount);
    }

    @Test
    /**
     *  Here the record overlaps between Split-1 and Split-2.
     *  reader will start reading the first record in the
     *  middle of the split. It won't find the start { of that record but will
     *  read till the end. It will successfully return the second record from the Split-2
     */
    public void testInBetweenSplits() throws IOException {

        long start = 32;
        fileSplit = new FileSplit(path, start, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        jsonRecordReader.next(key, data);
        assertEquals(107, data.toString().getBytes(StandardCharsets.UTF_8).length);

        // since the FileSplit starts at 32, which is the middle of the first record.
        // so the reader reads the first record till it finds the end } and then starts reading the next record in the split
        // it discards the previous read data but keeps track of the bytes read.

        assertEquals(184, jsonRecordReader.getPos() - start);

        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"yī\", \"year\": \"2020\", \"address\": \"anöther city\", \"zip\": \"12345\"}", data.toString());
    }

    @Test
    /**
     * The split size is only 50 bytes. The reader is expected to read the
     * full 1 record here.
     */
    public void testSplitIsSmallerThanRecord() throws IOException {

        long start = 0;
        fileSplit = new FileSplit(path, start, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        jsonRecordReader.next(key, data);
        // reads the full 1 record here
        assertEquals(105, data.toString().getBytes(StandardCharsets.UTF_8).length);

        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"äää\", \"year\": \"2022\", \"address\": \"söme city\", \"zip\": \"95051\"}", data.toString());
    }

    @Test
    /**
     * The Split size is large only single spilt will be able to read all the records.
     */
    public void testRecordSizeSmallerThanSplit() throws IOException {

        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        int recordCount = 0;
        while(jsonRecordReader.next(key, data))
        {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(5,recordCount);

        // assert the last record json
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"你好\", \"year\": \"2033\", \"address\": \"0\uD804\uDC13a\", \"zip\": \"19348\"}", data.toString());
    }

    @Test
    public void testEmptyFile() throws URISyntaxException, IOException {

        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/empty_input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 10, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        int recordCount = 0;
        if(jsonRecordReader.next(key, data))
        {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(0,recordCount);
        assertEquals(0, jsonRecordReader.getPos());
        assertEquals("", data.toString());
    }

    @Test
    public void testEmptyJsonObject() throws URISyntaxException, IOException {

        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/empty_json_object.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 10, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        int recordCount = 0;
        if(jsonRecordReader.next(key, data))
        {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(0,recordCount);
        assertEquals(12, jsonRecordReader.getPos());
        assertEquals("", data.toString());
    }

    @Test
    public void testNonMatchingMember() throws URISyntaxException, IOException {

        // search for a non mathing member in the file
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "abc");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 10, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        int recordCount = 0;
        if(jsonRecordReader.next(key, data))
        {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(0,recordCount);
        // This will return count of all the bytes in the file
        assertEquals(553, jsonRecordReader.getPos());
        assertEquals("", data.toString());
    }

    @Test
    public void testMixedJsonRecords() throws URISyntaxException, IOException {

        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/mixed_input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        int recordCount = 0;
        while(jsonRecordReader.next(key, data))
        {
            assertNotNull(data);
            recordCount++;
        }

        //cüstömerstätüs member will retrieve 4 records
        assertEquals(4,recordCount);

        // Test another member company-name
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "company-name");

        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        recordCount = 0;
        while(jsonRecordReader.next(key, data))
        {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(1,recordCount);

        // assert the last record json
        assertEquals("{\"company-name\":\"VMware\",\"name\": \"äää\", \"year\": \"2022\", \"address\": \"söme city\", \"zip\": \"95051\"}", data.toString());
    }

    @Test
    public void testMemberNotAtTopLevel() throws URISyntaxException, IOException {

        jobConf.set(RECORD_MEMBER_IDENTIFIER, "cüstömerstätüs");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/complex_input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        int recordCount = 0;
        while(jsonRecordReader.next(key, data))
        {
            assertNotNull(data);
            recordCount++;
        }

        //cüstömerstätüs member will retrieve 4 records
        assertEquals(2,recordCount);
    }

    private LongWritable createKey() {
        return new LongWritable();
    }

    private Text createValue() {
        return new Text();
    }
}
