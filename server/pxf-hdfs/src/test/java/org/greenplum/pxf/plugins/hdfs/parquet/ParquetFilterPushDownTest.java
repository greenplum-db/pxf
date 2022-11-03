package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor;
import org.greenplum.pxf.plugins.hdfs.ParquetResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetFilterPushDownTest extends ParquetBaseTest {

    // From resources/parquet/parquet_types.csv
    private static final int[] COL1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
    private static final String[] COL2 = {"row1", "row2", "row3", "row4", "row5", "row6", "row7", "row8", "row9", "row10", "row11", "row12_text_null", "row13_int_null", "row14_double_null", "row15_decimal_null", "row16_timestamp_null", "row17_real_null", "row18_bigint_null", "row19_bool_null", "row20", "row21_smallint_null", "row22_date_null", "row23_varchar_null", "row24_char_null", "row25_binary_null"};
    private static final String[] COL3 = {"2019-12-01", "2019-12-02", "2019-12-03", "2019-12-04", "2019-12-05", "2019-12-06", "2019-12-07", "2019-12-08", "2019-12-09", "2019-12-10", "2019-12-11", "2019-12-12", "2019-12-13", "2019-12-14", "2019-12-15", "2019-12-16", "2019-12-17", "2019-12-18", "2019-12-19", "2019-12-20", "2019-12-21", null, "2019-12-23", "2019-12-24", "2019-12-25"};
    private static final Double[] COL4 = {1200.0, 1300.0, 1400.0, 1500.0, 1600.0, 1700.0, 1800.0, 1900.0, 2000.0, 2100.0, 2200.0, 2300.0, 2400.0, null, 2500.0, 2550.0, 2600.0, 2600.0, 2600.0, 2600.0, 2600.0, 2600.0, 2600.0, 2600.0, 2600.0,};
    private static final String[] COL5 = {"good", "excellent", "good", "excellent", "good", "bad", "good", "bad", "excellent", "bad", "good", null, "good", "excellent", "good", "bad", "good", "bad", "good", "excellent", "good", "excellent", "good", "bad", "good"};
    private static final Boolean[] COL6 = {false, true, false, true, false, true, false, true, false, true, false, false, false, false, false, false, false, false, null, false, false, false, false, false, false};
    private static final String[] COL7 = {"2013-07-14T04:00:00Z", "2013-07-14T04:00:00Z", "2013-07-16T04:00:00Z", "2013-07-17T04:00:00Z", "2013-07-18T04:00:00Z", "2013-07-19T04:00:00Z", "2013-07-20T04:00:00Z", "2013-07-21T04:00:00Z", "2013-07-22T04:00:00Z", "2013-07-23T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-25T04:00:00Z", null, "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z", "2013-07-24T04:00:00Z"};
    private static final Long[] COL8 = {2147483647L, 2147483648L, 2147483649L, 2147483650L, 2147483651L, 2147483652L, 2147483653L, 2147483654L, 2147483655L, 2147483656L, 2147483657L, 2147483658L, 2147483659L, 2147483660L, 2147483661L, 2147483662L, 2147483663L, null, -1L, -2147483643L, -2147483644L, -2147483645L, -2147483646L, -2147483647L, -2147483648L};
    private static final Byte[] COL9 = {0b00110001, 0b00110010, 0b00110011, 0b00110100, 0b00110101, 0b00110110, 0b00110111, 0b00111000, 0b00111001, 0b00110000, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, 0b00110001, null};
    private static final Short[] COL10 = {-32768, -31500, -31000, -30000, -20000, -10000, -1000, -550, -320, -120, -40, -1, 0, 1, 100, 1000, 10000, 20000, 30000, 31000, null, 32100, 32200, 32500, 32767};
    private static final Float[] COL11 = {7.7F, 8.7F, 9.7F, 10.7F, 11.7F, 12.7F, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F, null, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F, 7.7F};
    private static final String[] COL12 = {"s_6", "s_7", "s_8", "s_9", "s_10", "s_11", "s_12", "s_13", "s_14", "s_15", "s_16", "s_16", "s_16", "s_16", "s_17", "s_160", "s_161", "s_162", "s_163", "s_164", "s_165", "s_166", null, "s_168", "s_169"};
    private static final String[] COL13 = {"USD", "USD", "USD", "USD", "USD", "USD", "USD", "EUR", "UAH", "USD", "UAH", "EUR", "USD", "UAH", "USD", "USD", "EUR", "USD", "USD", "UAH", "USD", "EUR", "EUR", null, "USD"};
    private static final Double[] COL14 = {1.23456, 1.23456, -1.23456, 123456789.1, 1E-12, 1234.889, 0.0001, 45678.00002, 23457.1, 45678.00002, 0.123456789, 0.123456789, 0.123456789, 0.123456789, null, 0.123456789, 0.123456789, 0.123456789, 0.123456789, 0.123456789, 0.123456789, 0.123456789, 0.123456789, 0.123456789, 0.123456789};
    private static final Double[] COL15 = {0.0, 123.45, -1.45, 0.25, -.25, 999.99, -999.99, 1.0, -1.0, 789.0, -789.0, 0.99, -0.99, 1.99, null, -1.99, 15.99, -15.99, -299.99, 299.99, 555.55, 0.15, 3.89, 3.14, 8.0};
    private static final Double[] COL16 = {0.12345, -0.12345, 12345678.90123, -12345678.90123, 99999999.0, -99999999.0, -99999999.99999, 99999999.99999, 0.0, 1.0, -1.0, 0.9, -0.9, 45.0, null, -45.0, 3.14159, -3.14159, 2.71828, -2.71828, 45.99999, -45.99999, 450.45001, 0.00001, -0.00001};
    private static final Integer[] COL17 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, null, 11, 12, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11};
    private static final int[] ALL = COL1;

    private Accessor accessor;
    private Resolver resolver;
    private RequestContext context;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();

        accessor = new ParquetFileAccessor();
        resolver = new ParquetResolver();
        context = new RequestContext();

        String path = Objects.requireNonNull(getClass().getClassLoader().getResource("parquet/parquet_types.parquet")).getPath();

        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.setDataSource(path);
        context.setFragmentMetadata(new HcfsFragmentMetadata(0, 4196));
        context.setTupleDescription(super.columnDescriptors);
        context.setConfiguration(new Configuration());

        accessor.setRequestContext(context);
        resolver.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.afterPropertiesSet();
    }

    @Test
    public void testNoFilter() throws Exception {
        // all rows are expected
        assertRowsReturned(ALL);
    }

    @Test
    public void testIdPushDown() throws Exception {

        for (int i = 1; i <= 25; i++) {
            // id = i
            String index = String.valueOf(i);
            String filterString = String.format("a0c20s%dd%so5", index.length(), index);
            context.setFilterString(filterString);
            assertRowsReturned(new int[]{i});
        }
    }

    @Test
    public void testIdPushDownWithProjectedColumns() throws Exception {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();
        columnDescriptors.forEach(d -> d.setProjected(false));
        columnDescriptors.get(0).setProjected(true);

        for (int i = 1; i <= 25; i++) {
            // id = i
            String index = String.valueOf(i);
            String filterString = String.format("a0c20s%dd%so5", index.length(), index);
            context.setFilterString(filterString);
            assertRowsReturned(new int[]{i});
        }
    }

    @Test
    public void testBooleanPushDown() throws Exception {
        int[] expectedRows = {2, 4, 6, 8, 10};
        // a5 == true
        context.setFilterString("a5c16s4dtrueo0");
        assertRowsReturned(expectedRows);

        // a5 <> true
        expectedRows = new int[]{1, 3, 5, 7, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a5c16s4dtrueo6");
        assertRowsReturned(expectedRows);

        // a5 == false
        expectedRows = new int[]{1, 3, 5, 7, 9, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a5c16s5dfalseo0");
        assertRowsReturned(expectedRows);

        // a5 <> false
        expectedRows = new int[]{2, 4, 6, 8, 10, 19};
        context.setFilterString("a5c16s5dfalseo6");
        assertRowsReturned(expectedRows);

        // a5 IS NULL
        expectedRows = new int[]{19};
        context.setFilterString("a5o8");
        assertRowsReturned(expectedRows);

        // a5 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a5o9");
        assertRowsReturned(expectedRows);

    }

    @Test
    public void testNotBooleanPushDown() throws Exception {
        int[] expectedRows = {1, 3, 5, 7, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        // NOT (a5 == true)
        context.setFilterString("a5c16s4dtrueo0l2");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testIntPushDown() throws Exception {
        // a16 = 11
        int[] expectedRows = {11, 12, 14, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a16c23s2d11o5");
        assertRowsReturned(expectedRows);

        // a16 < 11
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        context.setFilterString("a16c23s2d11o1");
        assertRowsReturned(expectedRows);

        // a16 > 11
        expectedRows = new int[]{15};
        context.setFilterString("a16c23s2d11o2");
        assertRowsReturned(expectedRows);

        // a16 <= 2
        expectedRows = new int[]{1, 2};
        context.setFilterString("a16c23s1d2o3");
        assertRowsReturned(expectedRows);

        // a16 >= 11
        expectedRows = new int[]{11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a16c23s2d11o4");
        assertRowsReturned(expectedRows);

        // a16 <> 11
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 15};
        context.setFilterString("a16c23s2d11o6");
        assertRowsReturned(expectedRows);

        // a16 IS NULL
        expectedRows = new int[]{13};
        context.setFilterString("a16o8");
        assertRowsReturned(expectedRows);

        // a16 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a16o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testLongPushDown() throws Exception {
        // a7 = 2147483655
        int[] expectedRows = {9};
        context.setFilterString("a7c20s10d2147483655o5");
        assertRowsReturned(expectedRows);

        // a7 < 0
        expectedRows = new int[]{19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a7c23s1d0o1");
        assertRowsReturned(expectedRows);

        // a7 > 2147483655
        expectedRows = new int[]{10, 11, 12, 13, 14, 15, 16, 17};
        context.setFilterString("a7c20s10d2147483655o2");
        assertRowsReturned(expectedRows);

        // a7 <= -2147483643
        expectedRows = new int[]{20, 21, 22, 23, 24, 25};
        context.setFilterString("a7c23s11d-2147483643o3");
        assertRowsReturned(expectedRows);

        // a7 >= 2147483655
        expectedRows = new int[]{9, 10, 11, 12, 13, 14, 15, 16, 17};
        context.setFilterString("a7c20s10d2147483655o4");
        assertRowsReturned(expectedRows);

        // a7 <> -1
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a7c23s2d-1o6");
        assertRowsReturned(expectedRows);

        // a7 IS NULL
        expectedRows = new int[]{18};
        context.setFilterString("a7o8");
        assertRowsReturned(expectedRows);

        // a7 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a7o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testRealPushDown() throws Exception {
        // a10 = 8.7
        int[] expectedRows = {2};
        context.setFilterString("a10c701s3d8.7o5");
        assertRowsReturned(expectedRows);

        // a10 < 8.7
        expectedRows = new int[]{1, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a10c701s3d8.7o1");
        assertRowsReturned(expectedRows);

        // a10 > 8.7
        expectedRows = new int[]{3, 4, 5, 6};
        context.setFilterString("a10c701s3d8.7o2");
        assertRowsReturned(expectedRows);

        // a10 <= 8.7
        expectedRows = new int[]{1, 2, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a10c701s3d8.7o3");
        assertRowsReturned(expectedRows);

        // a10 >= 8.7
        expectedRows = new int[]{2, 3, 4, 5, 6};
        context.setFilterString("a10c701s3d8.7o4");
        assertRowsReturned(expectedRows);

        // a10 <> 8.7
        expectedRows = new int[]{1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a10c701s3d8.7o6");
        assertRowsReturned(expectedRows);

        // a10 IS NULL
        expectedRows = new int[]{17};
        context.setFilterString("a10o8");
        assertRowsReturned(expectedRows);

        // a10 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a10o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testTextPushDown() throws Exception {
        // a4 = 'excellent'
        int[] expectedRows = {2, 4, 9, 14, 20, 22};
        context.setFilterString("a4c25s9dexcellento5");
        assertRowsReturned(expectedRows);

        // a4 < 'excellent'
        expectedRows = new int[]{6, 8, 10, 16, 18, 24};
        context.setFilterString("a4c25s9dexcellento1");
        assertRowsReturned(expectedRows);

        // a4 > 'excellent'
        expectedRows = new int[]{1, 3, 5, 7, 11, 13, 15, 17, 19, 21, 23, 25};
        context.setFilterString("a4c25s9dexcellento2");
        assertRowsReturned(expectedRows);

        // a4 <= 'excellent'
        expectedRows = new int[]{2, 4, 6, 8, 9, 10, 14, 16, 18, 20, 22, 24};
        context.setFilterString("a4c25s9dexcellento3");
        assertRowsReturned(expectedRows);

        // a4 >= 'excellent'
        expectedRows = new int[]{1, 2, 3, 4, 5, 7, 9, 11, 13, 14, 15, 17, 19, 20, 21, 22, 23, 25};
        context.setFilterString("a4c25s9dexcellento4");
        assertRowsReturned(expectedRows);

        // a4 <> 'excellent'
        expectedRows = new int[]{1, 3, 5, 6, 7, 8, 10, 11, 12, 13, 15, 16, 17, 18, 19, 21, 23, 24, 25};
        context.setFilterString("a4c25s9dexcellento6");
        assertRowsReturned(expectedRows);

        // a4 IS NULL
        expectedRows = new int[]{12};
        context.setFilterString("a4o8");
        assertRowsReturned(expectedRows);

        // a4 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a4o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testVarCharPushDown() throws Exception {
        // a11 = 's_16'
        int[] expectedRows = {11, 12, 13, 14};
        context.setFilterString("a11c25s4ds_16o5");
        assertRowsReturned(expectedRows);

        // a11 < 's_10'
        expectedRows = new int[]{};
        context.setFilterString("a11c25s4ds_10o1");
        assertRowsReturned(expectedRows);

        // a11 > 's_168'
        expectedRows = new int[]{1, 2, 3, 4, 15, 25};
        context.setFilterString("a11c25s5ds_168o2");
        assertRowsReturned(expectedRows);

        // a11 <= 's_10'
        expectedRows = new int[]{5};
        context.setFilterString("a11c25s4ds_10o3");
        assertRowsReturned(expectedRows);

        // a11 >= 's_168'
        expectedRows = new int[]{1, 2, 3, 4, 15, 24, 25};
        context.setFilterString("a11c25s5ds_168o4");
        assertRowsReturned(expectedRows);

        // a11 <> 's_16'
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a11c25s4ds_16o6");
        assertRowsReturned(expectedRows);

        // a11 IS NULL
        expectedRows = new int[]{23};
        context.setFilterString("a11o8");
        assertRowsReturned(expectedRows);

        // a11 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 24, 25};
        context.setFilterString("a11o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testCharPushDown() throws Exception {
        // a12 = 'EUR'
        int[] expectedRows = {8, 12, 17, 22, 23};
        context.setFilterString("a12c1042s3dEURo5");
        assertRowsReturned(expectedRows);

        // a12 < 'USD'
        expectedRows = new int[]{8, 9, 11, 12, 14, 17, 20, 22, 23};
        context.setFilterString("a12c1042s3dUSDo1");
        assertRowsReturned(expectedRows);

        // a12 > 'EUR'
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 13, 14, 15, 16, 18, 19, 20, 21, 25};
        context.setFilterString("a12c1042s3dEURo2");
        assertRowsReturned(expectedRows);

        // a12 <= 'EUR'
        expectedRows = new int[]{8, 12, 17, 22, 23};
        context.setFilterString("a12c1042s3dEURo3");
        assertRowsReturned(expectedRows);

        // a12 >= 'USD'
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 10, 13, 15, 16, 18, 19, 21, 25};
        context.setFilterString("a12c1042s3dUSDo4");
        assertRowsReturned(expectedRows);

        // a12 <> 'USD'
        expectedRows = new int[]{8, 9, 11, 12, 14, 17, 20, 22, 23, 24};
        context.setFilterString("a12c1042s3dUSDo6");
        assertRowsReturned(expectedRows);

        // a12 IS NULL
        expectedRows = new int[]{24};
        context.setFilterString("a12o8");
        assertRowsReturned(expectedRows);

        // a12 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25};
        context.setFilterString("a12o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testCharPushDownWithWhitespaces() throws Exception {
        // a12 = 'EUR '
        int[] expectedRows = {8, 12, 17, 22, 23};
        context.setFilterString("a12c1042s4dEUR o5");
        assertRowsReturned(expectedRows);

        // a12 > 'EUR '
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 13, 14, 15, 16, 18, 19, 20, 21, 25};
        context.setFilterString("a12c1042s4dEUR o2");
        assertRowsReturned(expectedRows);

        // a12 <= 'EUR '
        expectedRows = new int[]{8, 12, 17, 22, 23};
        context.setFilterString("a12c1042s4dEUR o3");
        assertRowsReturned(expectedRows);

        // a12 >= 'USD '
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 10, 13, 15, 16, 18, 19, 21, 25};
        context.setFilterString("a12c1042s4dUSD o4");
        assertRowsReturned(expectedRows);

        // a12 <> 'USD '
        expectedRows = new int[]{8, 9, 11, 12, 14, 17, 20, 22, 23, 24};
        context.setFilterString("a12c1042s4dUSD o6");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testSmallIntPushDown() throws Exception {
        // a9 = 1000
        int[] expectedRows = {16};
        context.setFilterString("a9c23s4d1000o5");
        assertRowsReturned(expectedRows);

        // a9 < -1000
        expectedRows = new int[]{1, 2, 3, 4, 5, 6};
        context.setFilterString("a9c23s5d-1000o1");
        assertRowsReturned(expectedRows);

        // a9 > 31000
        expectedRows = new int[]{22, 23, 24, 25};
        context.setFilterString("a9c23s5d31000o2");
        assertRowsReturned(expectedRows);

        // a9 <= 0
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
        context.setFilterString("a9c23s1d0o3");
        assertRowsReturned(expectedRows);

        // a9 >= 0
        expectedRows = new int[]{13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25};
        context.setFilterString("a9c23s1d0o4");
        assertRowsReturned(expectedRows);

        // a9 <> 0
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a9c23s1d0o6");
        assertRowsReturned(expectedRows);

        // a9 IS NULL
        expectedRows = new int[]{21};
        context.setFilterString("a9o8");
        assertRowsReturned(expectedRows);

        // a9 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25};
        context.setFilterString("a9o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testDatePushDown() throws Exception {
        // a2 = '2019-12-04'
        int[] expectedRows = {4};
        context.setFilterString("a2c1082s10d2019-12-04o5");
        assertRowsReturned(expectedRows);

        // a2 < '2019-12-04'
        expectedRows = new int[]{1, 2, 3};
        context.setFilterString("a2c1082s10d2019-12-04o1");
        assertRowsReturned(expectedRows);

        // a2 > '2019-12-20'
        expectedRows = new int[]{21, 23, 24, 25};
        context.setFilterString("a2c1082s10d2019-12-20o2");
        assertRowsReturned(expectedRows);

        // a2 <= '2019-12-06'
        expectedRows = new int[]{1, 2, 3, 4, 5, 6};
        context.setFilterString("a2c1082s10d2019-12-06o3");
        assertRowsReturned(expectedRows);

        // a2 >= '2019-12-15'
        expectedRows = new int[]{15, 16, 17, 18, 19, 20, 21, 23, 24, 25};
        context.setFilterString("a2c1082s10d2019-12-15o4");
        assertRowsReturned(expectedRows);

        // a2 <> '2019-12-15'
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a2c1082s10d2019-12-15o6");
        assertRowsReturned(expectedRows);

        // a2 IS NULL
        expectedRows = new int[]{22};
        context.setFilterString("a2o8");
        assertRowsReturned(expectedRows);

        // a2 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23, 24, 25};
        context.setFilterString("a2o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testDoublePushDown() throws Exception {
        // a3 = 1200
        int[] expectedRows = {1};
        context.setFilterString("a3c701s4d1200o5");
        assertRowsReturned(expectedRows);

        // a3 < 1500
        expectedRows = new int[]{1, 2, 3};
        context.setFilterString("a3c701s4d1500o1");
        assertRowsReturned(expectedRows);

        // a3 > 2500
        expectedRows = new int[]{16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a3c701s4d2500o2");
        assertRowsReturned(expectedRows);

        // a3 <= 1500
        expectedRows = new int[]{1, 2, 3, 4};
        context.setFilterString("a3c701s4d1500o3");
        assertRowsReturned(expectedRows);

        // a3 >= 2550
        expectedRows = new int[]{16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a3c701s4d2550o4");
        assertRowsReturned(expectedRows);

        // a3 <> 1200
        expectedRows = new int[]{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a3c701s4d1200o6");
        assertRowsReturned(expectedRows);

        // a3 IS NULL
        expectedRows = new int[]{14};
        context.setFilterString("a3o8");
        assertRowsReturned(expectedRows);

        // a3 IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a3o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testByteAFilter() throws Exception {
        // bin = '1'
        int[] expectedRows = {1, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};
        context.setFilterString("a8c25s1d1o5");
        assertRowsReturned(expectedRows);

        // bin < '1'
        expectedRows = new int[]{10};
        context.setFilterString("a8c25s1d1o1");
        assertRowsReturned(expectedRows);

        // bin > '1'
        expectedRows = new int[]{2, 3, 4, 5, 6, 7, 8, 9};
        context.setFilterString("a8c25s1d1o2");
        assertRowsReturned(expectedRows);

        // bin <= '1'
        expectedRows = new int[]{1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};
        context.setFilterString("a8c25s1d1o3");
        assertRowsReturned(expectedRows);

        // bin >= '1'
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};
        context.setFilterString("a8c25s1d1o4");
        assertRowsReturned(expectedRows);

        // bin <> '1'
        expectedRows = new int[]{2, 3, 4, 5, 6, 7, 8, 9, 10, 25};
        context.setFilterString("a8c25s1d1o6");
        assertRowsReturned(expectedRows);

        // bin IS NULL
        expectedRows = new int[]{25};
        context.setFilterString("a8o8");
        assertRowsReturned(expectedRows);

        // bin IS NOT NULL
        expectedRows = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};
        context.setFilterString("a8o9");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testDateAndAmtFilter() throws Exception {
        // cdate > '2019-12-02' and cdate < '2019-12-12' and amt > 1500
        int[] expectedRows = {5, 6, 7, 8, 9, 10, 11};
        context.setFilterString("a2c1082s10d2019-12-02o2a2c1082s10d2019-12-12o1a3c701s4d1500o2l0l0");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testDateWithOrAndAmtFilter() throws Exception {
        // cdate > '2019-12-19' OR ( cdate <= '2019-12-15' and amt > 2000)
        int[] expectedRows = {10, 11, 12, 13, 15, 20, 21, 23, 24, 25};
        context.setFilterString("a2c1082s10d2019-12-19o2a2c1082s10d2019-12-15o3a3c701s4d2000o2l0l1");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testDateWithOrAndAmtFilterWithProjectedColumns() throws Exception {

        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();
        columnDescriptors.forEach(d -> d.setProjected(false));
        columnDescriptors.get(0).setProjected(true);
        columnDescriptors.get(2).setProjected(true);
        columnDescriptors.get(3).setProjected(true);
        columnDescriptors.get(5).setProjected(true);

        // cdate > '2019-12-19' OR ( cdate <= '2019-12-15' and amt > 2000)
        int[] expectedRows = {10, 11, 12, 13, 15, 20, 21, 23, 24, 25};
        context.setFilterString("a2c1082s10d2019-12-19o2a2c1082s10d2019-12-15o3a3c701s4d2000o2l0l1");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testDateOrAmtFilter() throws Exception {
        // cdate > '2019-12-20' OR amt < 1500
        int[] expectedRows = {1, 2, 3, 21, 23, 24, 25};
        context.setFilterString("a2c1082s10d2019-12-20o2a3c701s4d1500o1l1");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testUnsupportedINT96Filter() throws Exception {
        // tm = '2013-07-23 21:00:00'
        context.setFilterString("a6c1114s19d2013-07-23 21:00:00o5");
        // all rows are expected
        assertRowsReturned(ALL);
    }

    @Test
    public void testUnsupportedFixedLenByteArrayFilter() throws Exception {
        // dec2 = 0
        context.setFilterString("a14c23s1d0o5");
        // all rows are expected
        assertRowsReturned(ALL);
    }

    @Test
    public void testInOperationFilter() throws Exception {
        // a16 in (11, 12)
        int[] expectedRows = {11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
        context.setFilterString("a16m1007s2d11s2d12o10");
        // all rows are expected
        assertRowsReturned(expectedRows);
    }

    private void assertRowsReturned(int[] expectedRows) throws Exception {
        assertTrue(accessor.openForRead());

        OneRow oneRow;
        for (int expectedRow : expectedRows) {
            oneRow = accessor.readNextObject();
            assertNotNull(oneRow, "Row " + expectedRow);
            List<OneField> fieldList = resolver.getFields(oneRow);
            assertNotNull(fieldList, "Row " + expectedRow);
            assertEquals(17, fieldList.size(), "Row " + expectedRow);

            assertTypes(fieldList);
            assertValues(fieldList, expectedRow - 1);
        }
        oneRow = accessor.readNextObject();
        assertNull(oneRow, "No more rows expected");

        accessor.closeForRead();
    }

    private void assertTypes(List<OneField> fieldList) {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();

        if (columnDescriptors.get(0).isProjected()) {
            assertEquals(DataType.INTEGER.getOID(), fieldList.get(0).type);
        }
        if (columnDescriptors.get(1).isProjected()) {
            assertEquals(DataType.TEXT.getOID(), fieldList.get(1).type);
        }
        if (columnDescriptors.get(2).isProjected()) {
            assertEquals(DataType.DATE.getOID(), fieldList.get(2).type);
        }
        if (columnDescriptors.get(3).isProjected()) {
            assertEquals(DataType.FLOAT8.getOID(), fieldList.get(3).type);
        }
        if (columnDescriptors.get(4).isProjected()) {
            assertEquals(DataType.TEXT.getOID(), fieldList.get(4).type);
        }
        if (columnDescriptors.get(5).isProjected()) {
            assertEquals(DataType.BOOLEAN.getOID(), fieldList.get(5).type);
        }
        if (columnDescriptors.get(6).isProjected()) {
            assertEquals(DataType.TIMESTAMP.getOID(), fieldList.get(6).type);
        }
        if (columnDescriptors.get(7).isProjected()) {
            assertEquals(DataType.BIGINT.getOID(), fieldList.get(7).type);
        }
        if (columnDescriptors.get(8).isProjected()) {
            assertEquals(DataType.BYTEA.getOID(), fieldList.get(8).type);
        }
        if (columnDescriptors.get(9).isProjected()) {
            assertEquals(DataType.SMALLINT.getOID(), fieldList.get(9).type);
        }
        if (columnDescriptors.get(10).isProjected()) {
            assertEquals(DataType.REAL.getOID(), fieldList.get(10).type);
        }
        if (columnDescriptors.get(11).isProjected()) {
            assertEquals(DataType.TEXT.getOID(), fieldList.get(11).type);
        }
        if (columnDescriptors.get(12).isProjected()) {
            assertEquals(DataType.TEXT.getOID(), fieldList.get(12).type);
        }
        if (columnDescriptors.get(13).isProjected()) {
            assertEquals(DataType.NUMERIC.getOID(), fieldList.get(13).type);
        }
        if (columnDescriptors.get(14).isProjected()) {
            assertEquals(DataType.NUMERIC.getOID(), fieldList.get(14).type);
        }
        if (columnDescriptors.get(15).isProjected()) {
            assertEquals(DataType.NUMERIC.getOID(), fieldList.get(15).type);
        }
        if (columnDescriptors.get(16).isProjected()) {
            assertEquals(DataType.INTEGER.getOID(), fieldList.get(16).type);
        }
    }

    private void assertValues(List<OneField> fieldList, final int row) {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();

        if (columnDescriptors.get(0).isProjected()) {
            assertEquals(COL1[row], fieldList.get(0).val, "Row " + row);
        } else {
            assertNull(fieldList.get(0).val, "Row " + row);
        }

        if (columnDescriptors.get(1).isProjected()) {
            assertEquals(COL2[row], fieldList.get(1).val, "Row " + row);
        } else {
            assertNull(fieldList.get(1).val, "Row " + row);
        }
        if (columnDescriptors.get(2).isProjected() && COL3[row] != null) {
            assertEquals(Date.valueOf(COL3[row]), fieldList.get(2).val, "Row " + row);
        } else {
            assertNull(fieldList.get(2).val, "Row " + row);
        }

        if (columnDescriptors.get(3).isProjected() && COL4[row] != null) {
            assertEquals(COL4[row], fieldList.get(3).val, "Row " + row);
        } else {
            assertNull(fieldList.get(3).val, "Row " + row);
        }

        if (columnDescriptors.get(4).isProjected() && COL5[row] != null) {
            assertEquals(COL5[row], fieldList.get(4).val, "Row " + row);
        } else {
            assertNull(fieldList.get(4).val, "Row " + row);
        }

        if (columnDescriptors.get(5).isProjected() && COL6[row] != null) {
            assertEquals(COL6[row], fieldList.get(5).val, "Row " + row);
        } else {
            assertNull(fieldList.get(5).val, "Row " + row);
        }

        if (columnDescriptors.get(6).isProjected() && COL7[row] != null) {

            Instant timestamp = Instant.parse(COL7[row]); // UTC
            ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
            String localTimestampString = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            assertEquals(localTimestampString, fieldList.get(6).val, "Row " + row);
        } else {
            assertNull(fieldList.get(6).val, "Row " + row);
        }

        if (columnDescriptors.get(7).isProjected() && COL8[row] != null) {
            assertEquals(COL8[row], fieldList.get(7).val, "Row " + row);
        } else {
            assertNull(fieldList.get(7).val, "Row " + row);
        }

        if (columnDescriptors.get(8).isProjected() && COL9[row] != null) {
            assertTrue(fieldList.get(8).val instanceof byte[], "Row " + row);
            byte[] bin = (byte[]) fieldList.get(8).val;
            assertEquals(1, bin.length, "Row " + row);
            assertEquals(COL9[row].byteValue(), bin[0],
                    "Row " + row + ", actual " + String.format("%8s", Integer.toBinaryString(bin[0] & 0xFF)).replace(' ', '0'));
        } else {
            assertNull(fieldList.get(8).val, "Row " + row);
        }

        if (columnDescriptors.get(9).isProjected() && COL10[row] != null) {
            assertEquals(COL10[row], fieldList.get(9).val, "Row " + row);
        } else {
            assertNull(fieldList.get(9).val, "Row " + row);
        }

        if (columnDescriptors.get(10).isProjected() && COL11[row] != null) {
            assertEquals(COL11[row], fieldList.get(10).val, "Row " + row);
        } else {
            assertNull(fieldList.get(10).val, "Row " + row);
        }

        if (columnDescriptors.get(11).isProjected() && COL12[row] != null) {
            assertEquals(COL12[row], fieldList.get(11).val, "Row " + row);
        } else {
            assertNull(fieldList.get(11).val);
        }

        if (columnDescriptors.get(12).isProjected() && COL13[row] != null) {
            assertEquals(COL13[row], fieldList.get(12).val, "Row " + row);
        } else {
            assertNull(fieldList.get(12).val, "Row " + row);
        }

        if (columnDescriptors.get(13).isProjected() && COL14[row] != null) {
            assertBigDecimal("Row " + row, COL14[row], fieldList.get(13).val);
        } else {
            assertNull(fieldList.get(13).val, "Row " + row);
        }

        if (columnDescriptors.get(14).isProjected() && COL15[row] != null) {
            assertBigDecimal("Row " + row, COL15[row], fieldList.get(14).val);
        } else {
            assertNull(fieldList.get(14).val, "Row " + row);
        }

        if (columnDescriptors.get(15).isProjected() && COL16[row] != null) {
            assertBigDecimal("Row " + row, COL16[row], fieldList.get(15).val);
        } else {
            assertNull(fieldList.get(15).val, "Row " + row);
        }

        if (columnDescriptors.get(16).isProjected() && COL17[row] != null) {
            assertEquals(COL17[row], fieldList.get(16).val, "Row " + row);
        } else {
            assertNull(fieldList.get(16).val, "Row " + row);
        }
    }

    private void assertBigDecimal(String message, Double expectedDouble, Object actual) {
        assertTrue(actual instanceof BigDecimal, message);
        BigDecimal expected = BigDecimal.valueOf(expectedDouble);
        expected = expected.setScale(((BigDecimal) actual).scale());
        assertEquals(expected, actual, message);
    }

}
