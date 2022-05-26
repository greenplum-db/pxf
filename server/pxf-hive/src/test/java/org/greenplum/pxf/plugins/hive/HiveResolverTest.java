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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class HiveResolverTest {

    @Mock
    HiveUtilities mockHiveUtilities;

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

    @BeforeEach
    public void setup() {
        properties = new Properties();
        configuration = new Configuration();
        columnDescriptors = new ArrayList<>();
        context = new RequestContext();
        // metadata usually set in accessor
        hiveIndexes = Arrays.asList(0, 1);
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

        context.setConfiguration(configuration);
        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver = new HiveResolver(mockHiveUtilities);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        List<OneField> output = resolver.getFields(row);

        assertThat(output.get(0).val).isEqualTo("plain string");
        assertThat(output.get(0).type).isEqualTo(DataType.TEXT.getOID());
        assertThat(output.get(1).val).isEqualTo(1000.0);
        assertThat(output.get(1).type).isEqualTo(DataType.FLOAT8.getOID());
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

        context.setConfiguration(configuration);
        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver = new HiveResolver(mockHiveUtilities);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        List<OneField> output = resolver.getFields(row);

        assertThat(output.get(0).val).isEqualTo("a really \"fancy\" string? *wink*");
        assertThat(output.get(0).type).isEqualTo(DataType.TEXT.getOID());
        assertThat(output.get(1).val).isEqualTo(1000.0);
        assertThat(output.get(1).type).isEqualTo(DataType.FLOAT8.getOID());
    }

    @Test
    public void testStructSimpleString() throws Exception {
        properties.put("serialization.lib", SERDE_CLASS_NAME_STRUCT);
        properties.put(serdeConstants.LIST_COLUMNS, COL_NAMES_STRUCT);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COL_TYPES_STRUCT);
        columnDescriptors.add(new ColumnDescriptor("address", DataType.TEXT.getOID(), 0, "struct", null));

        OneRow row = new OneRow(0, new Text("plain string\u00021001"));

        context.setConfiguration(configuration);
        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver = new HiveResolver(mockHiveUtilities);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        List<OneField> output = resolver.getFields(row);

        assertThat(output.get(0).toString()).isEqualTo("{\"street\":\"plain string\",\"zipcode\":1001}");
    }

    @Test
    public void testStructSpecialCharString() throws Exception {
        properties.put("serialization.lib", SERDE_CLASS_NAME_STRUCT);
        properties.put(serdeConstants.LIST_COLUMNS, COL_NAMES_STRUCT);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COL_TYPES_STRUCT);
        columnDescriptors.add(new ColumnDescriptor("address", DataType.TEXT.getOID(), 0, "struct", null));

        OneRow row = new OneRow(0, new Text("a really \"fancy\" string\u00021001"));

        context.setConfiguration(configuration);
        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver = new HiveResolver(mockHiveUtilities);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        List<OneField> output = resolver.getFields(row);

        assertThat(output.get(0).toString()).isEqualTo("{\"street\":\"a really \\\"fancy\\\" string\",\"zipcode\":1001}");
    }

    @Test
    public void testNestedStruct() throws Exception {
        properties.put("serialization.lib", SERDE_CLASS_NAME_STRUCT);
        properties.put(serdeConstants.LIST_COLUMNS, COL_NAMES_NESTED_STRUCT);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COL_TYPES_NESTED_STRUCT);
        columnDescriptors.add(new ColumnDescriptor("address", DataType.TEXT.getOID(), 0, "struct", null));

        OneRow row = new OneRow(0, new Text("1000\u0003a really \"fancy\" string\u0002plain string\u00031001"));

        context.setConfiguration(configuration);
        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver = new HiveResolver(mockHiveUtilities);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        List<OneField> output = resolver.getFields(row);

        assertThat(output.get(0).toString()).isEqualTo("{\"line1\":{\"number\":1000,\"street_name\":\"a really \\\"fancy\\\" string\"},\"line2\":{\"city\":\"plain string\",\"zipcode\":1001}}");
    }

    @Test
    public void testSetFieldsIsNotSupported() {
        resolver = new HiveResolver(mockHiveUtilities);

        Exception e = assertThrows(UnsupportedOperationException.class, () -> resolver.setFields(null));
        assertEquals("Hive resolver does not support write operation.", e.getMessage());
    }
}
