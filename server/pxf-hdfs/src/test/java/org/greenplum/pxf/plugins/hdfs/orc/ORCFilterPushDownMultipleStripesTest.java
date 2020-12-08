package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ORCFilterPushDownMultipleStripesTest extends ORCVectorizedBaseTest {

    private RequestContext context;

    // Stripe stats for the orc_file_predicate_pushdown.orc file used in this query for reference
    // Stripe Statistics:
    //  Stripe 1:
    //    Column 0: count: 1049 hasNull: false
    //    Column 1: count: 1046 hasNull: true min: -3 max: 124 sum: 62430
    //    Column 2: count: 1046 hasNull: true min: 256 max: 511 sum: 398889
    //    Column 3: count: 1049 hasNull: false min: 65536 max: 65791 sum: 68881051
    //    Column 4: count: 1049 hasNull: false min: 4294967296 max: 4294967551 sum: 4505420825953
    //    Column 5: count: 1049 hasNull: false min: 0.07999999821186066 max: 99.91999816894531 sum: 52744.70002820343
    //    Column 6: count: 1049 hasNull: false min: 0.02 max: 49.85 sum: 26286.349999999966
    //    Column 7: count: 1049 hasNull: false true: 526
    //    Column 8: count: 1049 hasNull: false min:  max: zach zipper sum: 13443
    //    Column 9: count: 1049 hasNull: false min: 2013-03-01 09:11:58.703 max: 2013-03-01 09:11:58.703999999
    //    Column 10: count: 1049 hasNull: false min: 0.08 max: 99.94 sum: 53646.16
    //    Column 11: count: 1049 hasNull: false sum: 13278
    //  Stripe 2:
    //    Column 0: count: 1049 hasNull: false
    //    Column 1: count: 1049 hasNull: false min: -100 max: -100 sum: -104900
    //    Column 2: count: 1049 hasNull: false min: -1000 max: -1000 sum: -1049000
    //    Column 3: count: 1049 hasNull: false min: -10000 max: -10000 sum: -10490000
    //    Column 4: count: 1049 hasNull: false min: -1000000 max: -1000000 sum: -1049000000
    //    Column 5: count: 1049 hasNull: false min: -100.0 max: -100.0 sum: -104900.0
    //    Column 6: count: 1049 hasNull: false min: -10.0 max: -10.0 sum: -10490.0
    //    Column 7: count: 1049 hasNull: false true: 0
    //    Column 8: count: 0 hasNull: true
    //    Column 9: count: 0 hasNull: true
    //    Column 10: count: 0 hasNull: true
    //    Column 11: count: 0 hasNull: true

    @BeforeEach
    public void setup() {
        super.setup();
        context = new RequestContext();

        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("byte1", DataType.SMALLINT.getOID(), 1, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("short1", DataType.SMALLINT.getOID(), 2, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("int1", DataType.INTEGER.getOID(), 3, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("long1", DataType.BIGINT.getOID(), 4, "int8", null));
        columnDescriptors.add(new ColumnDescriptor("float1", DataType.REAL.getOID(), 5, "real", null));
        columnDescriptors.add(new ColumnDescriptor("double1", DataType.FLOAT8.getOID(), 6, "float8", null));
        columnDescriptors.add(new ColumnDescriptor("boolean1", DataType.BOOLEAN.getOID(), 7, "bool", null));
        columnDescriptors.add(new ColumnDescriptor("string1", DataType.TEXT.getOID(), 8, "text", null));
        columnDescriptors.add(new ColumnDescriptor("timestamp1", DataType.TIMESTAMP.getOID(), 9, "timestamp", null));
        columnDescriptors.add(new ColumnDescriptor("decimal1", DataType.NUMERIC.getOID(), 10, "numeric", new Integer[]{4, 2}));
        columnDescriptors.add(new ColumnDescriptor("bytes1", DataType.BYTEA.getOID(), 11, "bin", null));

        String path = Objects.requireNonNull(getClass().getClassLoader().getResource("orc/orc_file_predicate_pushdown.orc")).getPath();
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setUser("test-user");
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.setConfiguration(new Configuration());
        context.setDataSource(path);
        context.setTupleDescription(columnDescriptors);
        context.addOption("MAP_BY_POSITION", "true");
        context.setFragmentMetadata(new HcfsFragmentMetadata(0, 42550));
    }

    @Test
    public void testNoFilter() throws Exception {
        // the file has 2098 rows, there are 2 stripes, each stripe has 1049
        // rows. The default batch size is 1024, so we should expect 4 batches
        // when reading data without a filter.
        runTestScenario(4);
    }

    @Test
    public void testTinyInt() throws Exception {
        // byte1 < -4 -> stripe 2
        context.setFilterString("a0c23s2d-4o1");
        runTestScenario(2);

        // byte1 > 101 -> stripe 1
        context.setFilterString("a0c23s3d101o2");
        runTestScenario(2);

        // byte1 <= -20 -> stripe 2
        context.setFilterString("a0c23s3d-20o3");
        runTestScenario(2);

        // byte1 >= 125 -> no stripes
        context.setFilterString("a0c23s3d125o4");
        runTestScenario(0);

        // byte1 = -4 -> no stripes
        context.setFilterString("a0c23s2d-4o5");
        runTestScenario(0);

        // byte1 <> -100 -> stripe 1 and 2
        context.setFilterString("a0c23s4d-100o6");
        runTestScenario(4);

        // byte1 IS NULL -> stripe 1
        context.setFilterString("a0o8");
        runTestScenario(2);

        // byte1 IS NOT NULL -> stripe 1 and 2
        context.setFilterString("a0o9");
        runTestScenario(4);
    }

    @Test
    public void testShort() throws Exception {
        // short1 < 256 -> stripe 2
        context.setFilterString("a1c23s3d256o1");
        runTestScenario(2);

        // short1 > 30 -> stripe 1
        context.setFilterString("a1c23s2d30o2");
        runTestScenario(2);

        // short1 <= 256 -> stripe 1 and 2
        context.setFilterString("a1c23s3d256o3");
        runTestScenario(4);

        // short1 >= 512 -> no stripes
        context.setFilterString("a1c23s3d512o4");
        runTestScenario(0);

        // short1 = 0 -> no stripes
        context.setFilterString("a1c23s1d0o5");
        runTestScenario(0);

        // short1 <> -1000 -> stripe 1 and 2
        context.setFilterString("a1c23s5d-1000o6");
        runTestScenario(4);

        // short1 IS NULL -> stripe 1
        context.setFilterString("a1o8");
        runTestScenario(2);

        // short1 IS NOT NULL -> stripe 1 and 2
        context.setFilterString("a1o9");
        runTestScenario(4);
    }

    @Test
    public void testInt() throws Exception {
        // int1 < 0 -> stripe 2
        context.setFilterString("a2c23s1d0o1");
        runTestScenario(2);

        // int1 > -9999 -> stripe 1
        context.setFilterString("a2c23s5d-9999o2");
        runTestScenario(2);

        // int1 <= 256 -> stripe 2
        context.setFilterString("a2c23s3d256o3");
        runTestScenario(2);

        // int1 >= -10000 -> stripe 1 and 2
        context.setFilterString("a2c23s6d-10000o4");
        runTestScenario(4);

        // int1 = 0 -> no stripes
        context.setFilterString("a2c23s1d0o5");
        runTestScenario(0);

        // int1 <> -1000 -> stripe 1 and 2
        context.setFilterString("a2c23s5d-1000o6");
        runTestScenario(4);

        // int1 IS NULL -> no stripes
        context.setFilterString("a2o8");
        runTestScenario(0);

        // int1 IS NOT NULL -> stripe 1 and 2
        context.setFilterString("a2o9");
        runTestScenario(4);
    }

    @Test
    public void testLong() throws Exception {
        // long1 < 0 -> stripe 2
        context.setFilterString("a3c23s1d0o1");
        runTestScenario(2);

        // long1 > 0 -> stripe 1
        context.setFilterString("a3c23s1d0o2");
        runTestScenario(2);

        // long1 <= 256 -> stripe 2
        context.setFilterString("a3c23s3d256o3");
        runTestScenario(2);

        // long1 >= 100 -> stripe 1
        context.setFilterString("a3c23s3d100o4");
        runTestScenario(2);

        // long1 = 4294967551 -> stripe 1
        context.setFilterString("a3c20s10d4294967551o5");
        runTestScenario(2);

        // long1 = 4294967295 -> no stripes
        context.setFilterString("a3c20s10d4294967295o5");
        runTestScenario(0);

        // long1 <> -1000 -> stripe 1 and 2
        context.setFilterString("a3c23s5d-1000o6");
        runTestScenario(4);

        // long1 IS NULL -> no stripes
        context.setFilterString("a3o8");
        runTestScenario(0);

        // long1 IS NOT NULL -> stripe 1 and 2
        context.setFilterString("a3o9");
        runTestScenario(4);
    }

    @Test
    public void testReal() throws Exception {
        // float1 < 0.07 -> stripe 2
        context.setFilterString("a4c701s4d0.07o1");
        runTestScenario(2);

        // float1 > 100.5 -> no stripes
        context.setFilterString("a4c701s5d100.5o2");
        runTestScenario(0);

        // float1 <= 0.08 -> stripe 1 and 2
        context.setFilterString("a4c701s4d0.08o3");
        runTestScenario(4);

        // float1 >= 99.9 -> stripe 1
        context.setFilterString("a4c701s4d99.9o4");
        runTestScenario(2);

        // float1 = 0 -> no stripes
        context.setFilterString("a4c701s1d0o5");
        runTestScenario(0);

        // float1 <> -1000 -> stripe 1 and 2
        context.setFilterString("a4c701s5d-1000o6");
        runTestScenario(4);

        // float1 IS NULL -> no stripes
        context.setFilterString("a4o8");
        runTestScenario(0);

        // float1 IS NOT NULL -> stripe 1 and 2
        context.setFilterString("a4o9");
        runTestScenario(4);
    }

    @Test
    public void testFloat8() throws Exception {
        // double1 < -10.0 -> no stripes
        context.setFilterString("a5c701s3d-10o1");
        runTestScenario(0);

        // double1 > 49.85 -> no stripes
        context.setFilterString("a5c701s5d49.85o2");
        runTestScenario(0);

        // double1 <= 0.02 -> stripe 1 and 2
        context.setFilterString("a5c701s4d0.02o3");
        runTestScenario(4);

        // double1 >= 49.85 -> stripe 1
        context.setFilterString("a5c701s5d49.85o4");
        runTestScenario(2);

        // double1 = 0 -> no stripes
        context.setFilterString("a5c701s1d0o5");
        runTestScenario(0);

        // double1 <> -1000 -> stripe 1 and 2
        context.setFilterString("a5c701s5d-1000o6");
        runTestScenario(4);

        // double1 IS NULL -> no stripes
        context.setFilterString("a5o8");
        runTestScenario(0);

        // double1 IS NOT NULL -> stripe 1 and 2
        context.setFilterString("a5o9");
        runTestScenario(4);
    }

    @Test
    public void testBoolean() throws Exception {
        // boolean1 -> stripe 1
        context.setFilterString("a6c16s4dtrueo0");
        runTestScenario(2);

        // not boolean1 -> stripe 1 and 2
        context.setFilterString("a6c16s4dtrueo0l2");
        runTestScenario(4);

        // boolean1 IS NULL -> no stripes
        context.setFilterString("a6o8");
        runTestScenario(0);

        // boolean1 IS NOT NULL -> stripe 1 and 2
        context.setFilterString("a6o9");
        runTestScenario(4);
    }

    @Test
    public void testText() throws Exception {
        // string1 < 'zach zipper' -> stripe 1
        context.setFilterString("a7c25s11dzach zippero1");
        runTestScenario(2);

        // string1 > 'zach zipper' -> no stripes
        context.setFilterString("a7c25s11dzach zippero2");
        runTestScenario(0);

        // string1 <= 'a' -> stripe 1
        context.setFilterString("a7c25s1dao3");
        runTestScenario(2);

        // string1 >= 'a' -> stripe 1
        context.setFilterString("a7c25s1dao4");
        runTestScenario(2);

        // string1 = 'foobar' -> no stripes
        context.setFilterString("a7c25s6dfoobaro5");
        runTestScenario(0);

        // string1 <> 'foobar' -> stripe 1
        context.setFilterString("a7c25s6dfoobaro6");
        runTestScenario(2);

        // string1 IS NULL -> stripe 2
        context.setFilterString("a7o8");
        runTestScenario(2);

        // string1 IS NOT NULL -> stripe 1
        context.setFilterString("a7o9");
        runTestScenario(2);
    }

    private void runTestScenario(int expectedBatches) throws Exception {
        OneRow batchOfRows;
        Accessor accessor = new ORCVectorizedAccessor();
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        assertTrue(accessor.openForRead());
        for (int i = 0; i < expectedBatches; i++) {
            batchOfRows = accessor.readNextObject();
            assertNotNull(batchOfRows);
        }
        assertNull(accessor.readNextObject(), "No more batches expected");
        accessor.closeForRead();
    }
}
