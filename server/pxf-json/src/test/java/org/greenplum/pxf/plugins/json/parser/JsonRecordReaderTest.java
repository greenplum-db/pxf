package org.greenplum.pxf.plugins.json.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.json.JsonRecordReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    public void setup() throws URISyntaxException {
        context = new RequestContext();
        context.setConfiguration(new Configuration());

        jobConf = new JobConf(context.getConfiguration());
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "cüstömerstätüs");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json").toURI());
        context.setDataSource(file.getPath());
        path = new Path(file.getPath());
    }

    @Test
    public void testWithCodec() throws URISyntaxException, IOException {

        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json.bz2").toURI());
        path = new Path(file.getPath());
        // This file split will be ignored since the codec is involved
        fileSplit = new FileSplit(path, 100, 200, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        assertEquals(0, jsonRecordReader.getPos());
        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            recordCount++;
        }
        assertEquals(5, recordCount);
    }

    @Test
    /**
     *  Here the record overlaps between Split-1 and Split-2.
     *  reader will start reading the first record in the
     *  middle of the split. It won't find the start { of that record but will
     *  read till the end. It will successfully return the second record from the Split-2
     */
    public void testInBetweenSplits() throws IOException {

        // Split starts at 32 (after the [ {"cüstömerstätüs":"välid" ) and split length is 100, it will read till 132 bytes ( 32 + 132)
        // the first record will end at 107 bytes and this record will be ignored since the split started after {
        long start = 32;
        fileSplit = new FileSplit(path, start, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(32, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        jsonRecordReader.next(key, data);

        assertEquals(107, data.toString().getBytes(StandardCharsets.UTF_8).length);

        // since the FileSplit starts at 32, which is the middle of the first record.
        // so the reader reads the first record till it finds the end } and then starts reading the next record in the split
        // it discards the previous read data but keeps track of the bytes read.

        // since we read the entire line to get the position, we will include whatever newline/carriage return characters that were read
        assertEquals(184, jsonRecordReader.getPos() - start);

        // The second record started with in the first split boundary so it will read the full second record.
        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"yī\", \"year\": \"2020\", \"address\": \"anöther city\", \"zip\": \"12345\"}", data.toString());
    }

    @Test
    /**
     * The split size is only 50 bytes. The reader is expected to read the
     * full 1 record here.
     */
    public void testSplitIsSmallerThanRecord() throws IOException {

        // Since the split starts at the beginning of the file, even though the split
        // is small it will continue reading the one full record which started in the split.
        long start = 0;
        fileSplit = new FileSplit(path, start, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        jsonRecordReader.next(key, data);
        // reads the full 1 record here
        assertEquals(105, data.toString().getBytes(StandardCharsets.UTF_8).length);

        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"äää\", \"year\": \"2022\", \"address\": \"söme city\", \"zip\": \"95051\"}", data.toString());
    }

    @Test
    /**
     * The Split size is large so a single split will be able to read all the records.
     */
    public void testRecordSizeSmallerThanSplit() throws IOException {

        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(5, recordCount);

        // The reader will count all the bytes from the file
        assertEquals(553, jsonRecordReader.getPos());
        // assert the last record json
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"你好\", \"year\": \"2033\", \"address\": \"0\uD804\uDC13a\", \"zip\": \"19348\"}", data.toString());
    }

    @Test
    public void testEmptyFile() throws URISyntaxException, IOException {

        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/empty_input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 10, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        if (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
        }

        assertFalse(jsonRecordReader.next(key, data));
        assertEquals(0, jsonRecordReader.getPos());
    }

    @Test
    public void testEmptyJsonObject() throws URISyntaxException, IOException {

        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/empty_json_object.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 10, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        if (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
        }

        assertFalse(jsonRecordReader.next(key, data));
        assertEquals(12, jsonRecordReader.getPos());
    }

    @Test
    public void testNonMatchingMemberLargeSplit() throws URISyntaxException, IOException {

        // search for a non-matching member in the file
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "abc");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        if (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(0, recordCount);
        assertFalse(jsonRecordReader.next(key, data));
        // This will return count of all the bytes in the file
        assertEquals(553, jsonRecordReader.getPos());
    }

    @Test
    public void testNonMatchingMemberSmallSplit() throws URISyntaxException, IOException {

        // search for a non-matching member in the file
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "abc");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 10, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        if (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(0, recordCount);
        assertFalse(jsonRecordReader.next(key, data));
        // This will return count of all the bytes read to finish the object for that split
        assertEquals(109, jsonRecordReader.getPos());
    }

    @Test
    public void testAllOnOneLineSmallSplit() throws URISyntaxException, IOException {

        // search for a non-matching member in the file
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "cüstömerstätüs");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/all_on_one_line.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(5, recordCount);

        assertFalse(jsonRecordReader.next(key, data));
        // This will return count of all the bytes read to finish the object for that split
        assertEquals(551, jsonRecordReader.getPos());
    }

    @Test
    public void testMultipleObjectsPerLineSmallSplit() throws URISyntaxException, IOException {

        // each record is about 100 char
        // search for a non-matching member in the file
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "cüstömerstätüs");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/multiple_object_per_line.json").toURI());
        path = new Path(file.getPath());
        key = createKey();
        data = createValue();
        int recordCount = 0;

        // split 1
        fileSplit = new FileSplit(path, 0, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(2, recordCount);
        // the second record should be the following
        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"yī\", \"year\": \"2020\", \"address\": \"anöther city\", \"zip\": \"12345\"}", data.toString());
        assertFalse(jsonRecordReader.next(key, data));
        // expected to go into next split to finish the second object
        assertEquals(220, jsonRecordReader.getPos());

        // split 2
        fileSplit = new FileSplit(path, 100, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        // there should be no change because split 1 took care of all of split 2
        assertEquals(2, recordCount);

        // split 3
        fileSplit = new FileSplit(path, 200, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(4, recordCount);
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"हिमांशु\", \"year\": \"1234\", \"address\": \"Same ₡i\uD804\uDC13y\", \"zip\": \"00010\"}", data.toString());
        assertFalse(jsonRecordReader.next(key, data));
        assertEquals(454, jsonRecordReader.getPos());

        // split 4
        fileSplit = new FileSplit(path, 300, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        // there should be no change because split 3 took care of all of split 4
        assertEquals(4, recordCount);

        // split 5
        fileSplit = new FileSplit(path, 400, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(5, recordCount);
        // the last record should be the following:
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"你好\", \"year\": \"2033\", \"address\": \"0\uD804\uDC13a\", \"zip\": \"19348\"}", data.toString());
        assertFalse(jsonRecordReader.next(key, data));
        assertEquals(558, jsonRecordReader.getPos());

        // split 6
        fileSplit = new FileSplit(path, 500, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        assertEquals(5, recordCount);
        assertFalse(jsonRecordReader.next(key, data));
        /// no objects found, but reads \n and ending bracket
        assertEquals(559, jsonRecordReader.getPos());
    }

    @Test
    public void testStraddleSplitSmallSplitSize() throws URISyntaxException, IOException {

        // each record is about 100 char
        //   2 line 1 is open array bracket - 2 bytes
        //  69 line 2 has half a record - 67 bytes
        // 224 line 3 has the remaining half, and a full record -- 155 bytes
        // 292 line 4 has half a record -- 68 bytes
        // 511 line 5 has remaining half, a full record, and another half -- 219 bytes
        // 566 line 6 has the last half record -- 55 bytes
        // 567 line 7 is closing array bracket -- 1 bytes
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "cüstömerstätüs");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/straddle_split.json").toURI());
        path = new Path(file.getPath());
        key = createKey();
        data = createValue();
        int recordCount = 0;

        // split 1 (starts line 1 continues into line 2, should read into line 3 to finish the object)
        fileSplit = new FileSplit(path, 0, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(1, recordCount);
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"äää\", \"year\": \"2022\",\n" +
                "    \"address\": \"söme city\", \"zip\": \"95051\"}", data.toString());
        assertFalse(jsonRecordReader.next(key, data));
        // reads the full second line... even if it doesn't use the full line
        assertEquals(120, jsonRecordReader.getPos());

        // split 2 (starts mid line 2 continues into line 3, skip incomplete object and read full object)
        fileSplit = new FileSplit(path, 50, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(2, recordCount);
        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"yī\", \"year\": \"2020\", \"address\": \"anöther city\", \"zip\": \"12345\"}", data.toString());
        assertFalse(jsonRecordReader.next(key, data));
        // expected to go into next split to finish the second object
        assertEquals(224, jsonRecordReader.getPos());

        // split 3 (starts mid line 3) no change
        fileSplit = new FileSplit(path, 100, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        assertEquals(2, recordCount);

        // split 4 (starts mid line 3) no change
        fileSplit = new FileSplit(path, 150, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        assertEquals(2, recordCount);

        // split 5 (starts mid line 3, reads into line 4)
        fileSplit = new FileSplit(path, 200, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        assertEquals(3, recordCount);
        assertEquals("{\"cüstömerstätüs\":\"invälid\",\"name\": \"₡¥\", \"year\": \"2022\",\n" +
                "    \"address\": \"\uD804\uDC13exas\", \"zip\": \"12345\"}", data.toString());
        assertFalse(jsonRecordReader.next(key, data));
        assertEquals(366, jsonRecordReader.getPos());

        // split 6 (starts mid line 4, split continues into line 5,)
        fileSplit = new FileSplit(path, 250, 50, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        // there should be no change because split 6 took care of the rest of the file
        assertEquals(5, recordCount);
        // the last record should be the following:
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"你好\",\n" +
                "  \"year\": \"2033\", \"address\": \"0\uD804\uDC13a\", \"zip\": \"19348\"}", data.toString());
        assertFalse(jsonRecordReader.next(key, data));
        // finished reading the object but not necessarily the file
        assertEquals(565, jsonRecordReader.getPos());

        // split 7 - 12 should have no new records to count
        for (int start=300; start<600; start+=50) {
            fileSplit = new FileSplit(path, start, 50, hosts);
            jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
            while (jsonRecordReader.next(key, data)) {
                assertNotNull(data);
                recordCount++;
            }
            assertEquals(5, recordCount);
            assertFalse(jsonRecordReader.next(key, data));
        }

        assertEquals(5, recordCount);
        assertFalse(jsonRecordReader.next(key, data));
        assertEquals(567, jsonRecordReader.getPos());
    }

    @Test
    public void testStraddleSplitMedSplitSize() throws URISyntaxException, IOException {

        // each record is about 100 char
        // line 1 is open array bracket
        // line 2 has half a record
        // line 3 has the remaining half, and a full record
        // line 4 has half a record
        // line 5 has remaining half, a full record, and another half
        // line 6 has the last half record
        // line 7 is closing array bracket
        // search for a non-matching member in the file
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "cüstömerstätüs");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/straddle_split.json").toURI());
        path = new Path(file.getPath());
        key = createKey();
        data = createValue();
        int recordCount = 0;

        // split 1 (starts line 1, ends mid line 3 in second object)
        fileSplit = new FileSplit(path, 0, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(2, recordCount);
        assertFalse(jsonRecordReader.next(key, data));
        assertEquals(224, jsonRecordReader.getPos());

        // split 2 (starts mid line 3) no change
        fileSplit = new FileSplit(path, 100, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        assertEquals(2, recordCount);

        // split 3 (starts mid line 3, reads into line 4 reads until end of file)
        fileSplit = new FileSplit(path, 200, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        assertEquals(5, recordCount);
        assertFalse(jsonRecordReader.next(key, data));
        assertEquals(565, jsonRecordReader.getPos());

        // split 4 (starts mid line 4, reads until end of file)
        fileSplit = new FileSplit(path, 300, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        // split 5 (starts mid line 4, reads until end of file)
        fileSplit = new FileSplit(path, 400, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        // split 6 (starts mid line 4, reads until end of file)
        fileSplit = new FileSplit(path, 500, 100, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }
        // there should be no change because split 3 took care of the rest of the file
        assertEquals(5, recordCount);
        assertFalse(jsonRecordReader.next(key, data));
        assertEquals(567, jsonRecordReader.getPos());
    }

    @Test
    public void testContainsCarriageReturnAndNewLines() throws URISyntaxException, IOException {

        jobConf.set(RECORD_MEMBER_IDENTIFIER, "name");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/carriage_returns.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        //cüstömerstätüs identifier will retrieve 4 records
        assertEquals(5, recordCount);

        assertFalse(jsonRecordReader.next(key, data));
        // This will return count of all the bytes read to finish the object for that split
        assertEquals(559, jsonRecordReader.getPos());
   }

    @Test
    public void testContainsMixedCarriageReturns() throws URISyntaxException, IOException {

        jobConf.set(RECORD_MEMBER_IDENTIFIER, "name");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/mixed_carriage_returns.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        //cüstömerstätüs identifier will retrieve 4 records
        assertEquals(5, recordCount);

        assertFalse(jsonRecordReader.next(key, data));
        // This will return count of all the bytes read to finish the object for that split
        assertEquals(556, jsonRecordReader.getPos());
    }

    @Test
    public void testMultipleCarriageReturns() throws URISyntaxException, IOException {

        jobConf.set(RECORD_MEMBER_IDENTIFIER, "name");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/multiple_carriage_returns.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 1;
        // assert the first record
        jsonRecordReader.next(key, data);
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"äää\", \"year\": \"2022\", \"address\": \"söme city\", \"zip\": \"95051\"}", data.toString());

        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        //cüstömerstätüs identifier will retrieve 4 records
        assertEquals(5, recordCount);

        assertFalse(jsonRecordReader.next(key, data));
        // This will return count of all the bytes read to finish the object for that split
        // 553 base + 2 CR per line (6 lines total) = 565
        assertEquals(565, jsonRecordReader.getPos());

        // assert the last record json
        assertEquals("{\"cüstömerstätüs\":\"välid\",\"name\": \"你好\", \"year\": \"2033\", \"address\": \"0\uD804\uDC13a\", \"zip\": \"19348\"}", data.toString());

    }

    @Test
    public void testMixedJsonRecords() throws URISyntaxException, IOException {

        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/mixed_input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        //cüstömerstätüs identifier will retrieve 4 records
        assertEquals(4, recordCount);

        // The reader will count all the bytes from the file
        assertEquals(846, jsonRecordReader.getPos());

        // Test another identifier company-name
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "company-name");

        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);
        key = createKey();
        data = createValue();
        recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(2, recordCount);

        // The reader will count all the bytes from the file
        assertEquals(846, jsonRecordReader.getPos());

        // assert the last record json
        assertEquals("{\"company-name\":\"VMware\",\"name\": \"हिमांशु\", \"year\": \"1234\", \"address\": \"anöther city\", \"zip\": \"00010\"}", data.toString());
    }

    @Test
    public void testMemberNotAtTopLevel() throws URISyntaxException, IOException {
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "name");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/complex_input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        //cüstömerstätüs identifier will retrieve 2 records
        assertEquals(2, recordCount);
        // The reader will count all the bytes from the file
        assertEquals(328, jsonRecordReader.getPos());
    }

    @Test
    public void testSpecialCharsInJson() throws URISyntaxException, IOException {
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "name");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/offset/special_chars.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        //cüstömerstätüs identifier will retrieve 2 records
        assertEquals(2, recordCount);
    }
    @Test
    public void testSplitBeforeMemberName() throws URISyntaxException, IOException {
        // If the Split After the BEGIN_OBJECT ( i.e. { ), we expect the record reader to skip over this object entirely.
        //Here the split start after the <SEEK> keyword in the file ( Line # 3 ) and right before the identifier.
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "name");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/seek/seek-into-mid-object-1/input.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 31, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(31, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(1, recordCount);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(String.valueOf(data));
        String color = node.get("color").asText();

        assertEquals("red", color);

        String name = node.get("name").asText();

        assertEquals("123.45", name);

        String nodeVal = node.get("v").asText();

        assertEquals("vv", nodeVal);

    }

    @Test
    public void testJsonWithSameParentChildMemberName() throws URISyntaxException, IOException {

        // without split this will return one record
        jobConf.set(RECORD_MEMBER_IDENTIFIER, "name");
        file = new File(this.getClass().getClassLoader().getResource("parser-tests/noseek/array_objects_same_name_in_child.json").toURI());
        path = new Path(file.getPath());
        fileSplit = new FileSplit(path, 0, 1000, hosts);
        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(0, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        int recordCount = 0;
        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        assertEquals(1, recordCount);
        // The split starts after line 6 ( after the "name":false, ) in the file
        // this will ignore the parent record and will count the two child records with "name" identifier
        fileSplit = new FileSplit(path, 92, 1000, hosts);

        jsonRecordReader = new JsonRecordReader(jobConf, fileSplit);

        assertEquals(92, jsonRecordReader.getPos());

        key = createKey();
        data = createValue();
        recordCount = 0;

        while (jsonRecordReader.next(key, data)) {
            assertNotNull(data);
            recordCount++;
        }

        //"name" identifier will retrieve 2 records
        assertEquals(2, recordCount);
    }

    private LongWritable createKey() {
        return new LongWritable();
    }

    private Text createValue() {
        return new Text();
    }
}
