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

    private Accessor accessor;
    private ORCVectorizedResolver resolver;
    private RequestContext context;

    // Stripe stats for the file used in this query for reference
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

        accessor = new ORCVectorizedAccessor();
        resolver = new ORCVectorizedResolver();
        context = new RequestContext();

        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("byte1", DataType.SMALLINT.getOID(), 1, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("short1", DataType.SMALLINT.getOID(), 2, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("int1", DataType.INTEGER.getOID(), 3, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("long1", DataType.BIGINT.getOID(), 4, "int8", null));
        columnDescriptors.add(new ColumnDescriptor("float1", DataType.REAL.getOID(), 5, "real", null));
        columnDescriptors.add(new ColumnDescriptor("double1", DataType.FLOAT8.getOID(), 6, "float8", null));
        columnDescriptors.add(new ColumnDescriptor("boolean1", DataType.BOOLEAN.getOID(), 0, "bool", null));
        columnDescriptors.add(new ColumnDescriptor("string1", DataType.TEXT.getOID(), 8, "text", null));
        columnDescriptors.add(new ColumnDescriptor("timestamp1", DataType.TIMESTAMP.getOID(), 5, "timestamp", null));
        columnDescriptors.add(new ColumnDescriptor("decimal1", DataType.NUMERIC.getOID(), 4, "numeric", new Integer[]{4, 2}));
        columnDescriptors.add(new ColumnDescriptor("bytes1", DataType.BYTEA.getOID(), 7, "bin", null));

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

        accessor.setRequestContext(context);
        resolver.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.afterPropertiesSet();
    }

    @Test
    public void testNoFilter() throws Exception {
        // the file has 2098 rows, there are 2 stripes, each stripe has 1049
        // rows. The default batch size is 1024, so we should expect 4 batches
        // when reading data without a filter.
        runTestScenario(4);
    }

//    @Test
//    public void testTinyInt() throws Exception {
//        // byte1 = -4 -> stripe 2
//        context.setFilterString("a0c23s2d-4o5");
//        runTestScenario(4);
//
//        // byte1 < -4 -> stripe 2
//        context.setFilterString("a0c23s2d-4o1");
//        runTestScenario(2);
//
//        // byte1 > 101 -> stripe 1
//        context.setFilterString("a0c23s3d101o2");
//        runTestScenario(2);
//
//        // byte1 <= -20 -> stripe 2
//        context.setFilterString("a0c23s3d-20o3");
//        runTestScenario(2);
//
//        // byte1 >= 125 -> no stripes
//        context.setFilterString("a0c23s3d125o4");
//        runTestScenario(0);
//
//        // byte1 <> -100 -> stripe 1
//        context.setFilterString("a0c23s4d-100o6");
//        runTestScenario(2);
//
//        // byte1 IS NULL -> stripe 1
//        context.setFilterString("a0o8");
//        runTestScenario(2);
//
//        // byte1 IS NOT NULL -> stripe 1 and 2
//        context.setFilterString("a0o9");
//        runTestScenario(4);
//    }

    private void runTestScenario(int expectedBatches) throws Exception {
        OneRow batchOfRows;
        assertTrue(accessor.openForRead());
        for (int i = 0; i < expectedBatches; i++) {
            batchOfRows = accessor.readNextObject();
            assertNotNull(batchOfRows);
        }
        assertNull(accessor.readNextObject(), "No more batches expected");
        accessor.closeForRead();
    }
}
