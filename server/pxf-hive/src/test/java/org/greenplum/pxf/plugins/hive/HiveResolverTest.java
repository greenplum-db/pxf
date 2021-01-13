package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class HiveResolverTest {

    private static final String SERDE_CLASS_NAME = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    private static final String COL_NAMES_SIMPLE = "name,amt";
    private static final String COL_TYPES_SIMPLE = "string:double";

    private static final String SERDE_CLASS_NAME_STRUCT = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final String COL_NAMES_STRUCT = "address";
    private static final String COL_TYPES_STRUCT = "struct<street:string,zipcode:bigint>";
    private static final String COL_NAMES_NESTED_STRUCT = "address";
    private static final String COL_TYPES_NESTED_STRUCT = "struct<line1:struct<number:bigint,street_name:string>,line2:struct<city:string,zipcode:bigint>>";
    Configuration configuration;
    Properties properties;
    List<ColumnDescriptor> columnDescriptors;
    private HiveResolver resolver;
    RequestContext context;
    List<Integer> hiveIndexes;

    @Before
    public void setup() {
        properties = new Properties();
        configuration = new Configuration();
        columnDescriptors = new ArrayList<>();
        context = new RequestContext();
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("test-user");

        hiveIndexes = Arrays.asList(0, 1);

        resolver = new HiveResolver();
    }

    @Test
    public void testSimpleString() throws Exception {

        properties.put("serialization.lib", SERDE_CLASS_NAME);
        properties.put(serdeConstants.LIST_COLUMNS, COL_NAMES_SIMPLE);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COL_TYPES_SIMPLE);
        columnDescriptors.add(new ColumnDescriptor("name", DataType.TEXT.getOID(), 0, "text", null));
        columnDescriptors.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 1, "float8", null));

        ArrayWritable aw = new ArrayWritable(Text.class, new Writable[]{new Text("plain string"), new DoubleWritable(1000)});
        OneRow row = new OneRow(aw);

        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver.initialize(context);
        List<OneField> output = resolver.getFields(row);

        assertEquals("plain string", output.get(0).val);
        assertEquals(DataType.TEXT.getOID(), output.get(0).type);
        assertEquals(1000.0,output.get(1).val);
        assertEquals(DataType.FLOAT8.getOID(), output.get(1).type);
    }

    @Test
    public void testSpecialCharString() throws Exception {

        properties.put("serialization.lib", SERDE_CLASS_NAME);
        properties.put(serdeConstants.LIST_COLUMNS, COL_NAMES_SIMPLE);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COL_TYPES_SIMPLE);
        columnDescriptors.add(new ColumnDescriptor("name", DataType.TEXT.getOID(), 0, "text", null));
        columnDescriptors.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 1, "float8", null));

        ArrayWritable aw = new ArrayWritable(Text.class, new Writable[]{new Text("a really \"fancy\" string? *wink*"), new DoubleWritable(1000)});
        OneRow row = new OneRow(aw);

        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver.initialize(context);
        List<OneField> output = resolver.getFields(row);

        assertEquals("a really \"fancy\" string? *wink*", output.get(0).val);
        assertEquals(DataType.TEXT.getOID(), output.get(0).type);
        assertEquals(1000.0, output.get(1).val);
        assertEquals(DataType.FLOAT8.getOID(), output.get(1).type);
    }

    @Test
    public void testStructSimpleString() throws Exception {
        properties.put("serialization.lib", SERDE_CLASS_NAME_STRUCT);
        properties.put(serdeConstants.LIST_COLUMNS, COL_NAMES_STRUCT);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COL_TYPES_STRUCT);
        columnDescriptors.add(new ColumnDescriptor("address", DataType.TEXT.getOID(), 0, "struct", null));

        OneRow row = new OneRow(0, new Text("plain string\u00021001"));

        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver.initialize(context);
        List<OneField> output = resolver.getFields(row);

        assertEquals("{\"street\":\"plain string\",\"zipcode\":1001}", output.get(0).toString());
    }

    @Test
    public void testStructSpecialCharString() throws Exception {
        properties.put("serialization.lib", SERDE_CLASS_NAME_STRUCT);
        properties.put(serdeConstants.LIST_COLUMNS, COL_NAMES_STRUCT);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COL_TYPES_STRUCT);
        columnDescriptors.add(new ColumnDescriptor("address", DataType.TEXT.getOID(), 0, "struct", null));

        OneRow row = new OneRow(0, new Text("a really \"fancy\" string\u00021001"));

        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver.initialize(context);
        List<OneField> output = resolver.getFields(row);

        assertEquals("{\"street\":\"a really \\\"fancy\\\" string\",\"zipcode\":1001}", output.get(0).toString());
    }

    @Test
    public void testNestedStruct() throws Exception {
        properties.put("serialization.lib", SERDE_CLASS_NAME_STRUCT);
        properties.put(serdeConstants.LIST_COLUMNS, COL_NAMES_NESTED_STRUCT);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COL_TYPES_NESTED_STRUCT);
        columnDescriptors.add(new ColumnDescriptor("address", DataType.TEXT.getOID(), 0, "struct", null));

        OneRow row = new OneRow(0, new Text("1000\u0003a really \"fancy\" string\u0002plain string\u00031001"));

        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver.initialize(context);
        List<OneField> output = resolver.getFields(row);

        assertEquals("{\"line1\":{\"number\":1000,\"street_name\":\"a really \\\"fancy\\\" string\"},\"line2\":{\"city\":\"plain string\",\"zipcode\":1001}}", output.get(0).toString());
    }
}
