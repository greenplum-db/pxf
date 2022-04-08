package org.greenplum.pxf.plugins.hdfs.orc;

import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ORCSchemaBuilderTest {

    public static final String ALL_TYPES_SCHEMA = (new StringBuilder("struct<"))
            .append("col0:boolean,")
            .append("col1:binary,")
            .append("col2:bigint,")
            .append("col3:smallint,")
            .append("col4:int,")
            .append("col5:string,")
            .append("col6:float,")
            .append("col7:double,")
            .append("col8:char(256),")
            .append("col9:varchar(256),")
            .append("col10:date,")
            .append("col12:timestamp,")
            .append("col13:timestamp with local time zone,")
            .append("col14:decimal(38,10),")
            .append("col15:string,")
            .append("col16:array<smallint>,")
            .append("col17:array<int>,")
            .append("col18:array<bigint>,")
            .append("col19:array<boolean>,")
            .append("col20:array<string>,")
            .append("col21:array<float>,")
            .append("col22:array<double>,")
            .append("col23:array<binary>,")
            .append("col24:array<char(256)>,")
            .append("col25:array<varchar(256)>,")
            .append("col26:array<date>,")
            .append("col27:array<string>,")
            .append("col28:array<decimal(38,10)>,")
            .append("col30:array<timestamp>,")
            .append("col31:array<timestamp with local time zone>>").toString();

    private ORCSchemaBuilder builder;
    private List<ColumnDescriptor> columnDescriptors= new ArrayList<>();

    @BeforeEach
    public void setup() {
        builder = new ORCSchemaBuilder();
        columnDescriptors.clear();
    }

    @Test
    public void testNoColumnDescriptors() {
        assertNull(builder.buildSchema(null));
    }

    @Test
    public void testEmptyColumnDescriptors() {
        assertEquals("struct<>", builder.buildSchema(columnDescriptors).toString());
    }

    @Test
    public void testUnsupportedType() {
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.UNSUPPORTED_TYPE.getOID(), 0, "", null));
        Exception e = assertThrows(PxfRuntimeException.class, () -> builder.buildSchema(columnDescriptors));
        assertEquals("Unsupported Greenplum type -1 for column col0", e.getMessage());
    }

    @Test
    public void testAllSupportedTypes() {
        columnDescriptors = buildAllTypes();
        assertEquals(ALL_TYPES_SCHEMA, builder.buildSchema(columnDescriptors).toString());
    }

    @Test
    public void testBpcharMaxLength() {
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.BPCHAR.getOID(), 0, "", new Integer[]{}));
        assertEquals("struct<col0:char(256)>", builder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.BPCHAR.getOID(), 0, "", new Integer[]{3}));
        assertEquals("struct<col0:char(3)>", builder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.BPCHAR.getOID(), 0, "", new Integer[]{300}));
        assertEquals("struct<col0:char(300)>", builder.buildSchema(columnDescriptors).toString());
    }

    @Test
    public void testVarcharMaxLength() {
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.VARCHAR.getOID(), 0, "", new Integer[]{}));
        assertEquals("struct<col0:varchar(256)>", builder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.VARCHAR.getOID(), 0, "", new Integer[]{3}));
        assertEquals("struct<col0:varchar(3)>", builder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.VARCHAR.getOID(), 0, "", new Integer[]{300}));
        assertEquals("struct<col0:varchar(300)>", builder.buildSchema(columnDescriptors).toString());
    }

    @Test
    public void testNumericPrecisionAndScale() {
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{}));
        assertEquals("struct<col0:decimal(38,10)>", builder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{20,5}));
        assertEquals("struct<col0:decimal(20,5)>", builder.buildSchema(columnDescriptors).toString());

        // precision is null, scale is ignored
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{null,5}));
        assertEquals("struct<col0:decimal(38,10)>", builder.buildSchema(columnDescriptors).toString());

        // scale is missing, defaulted to 0
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{20}));
        assertEquals("struct<col0:decimal(20,0)>", builder.buildSchema(columnDescriptors).toString());

        // scale is null, defaulted to 0
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{20}));
        assertEquals("struct<col0:decimal(20,0)>", builder.buildSchema(columnDescriptors).toString());

        // precision is smaller than ORC default scale of 10, scale missing
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{8}));
        assertEquals("struct<col0:decimal(8,0)>", builder.buildSchema(columnDescriptors).toString());

        // precision is smaller than ORC default scale of 10, scale is provided
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{8,2}));
        assertEquals("struct<col0:decimal(8,2)>", builder.buildSchema(columnDescriptors).toString());

        // precision is larger than ORC max of 38
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{55}));
        Exception e = assertThrows(IllegalArgumentException.class, () -> builder.buildSchema(columnDescriptors));
        assertEquals("precision 55 is out of range 1 .. 0", e.getMessage());
        // that was rather unfortunate error message from ORC library, since ORC errors out with the same error message
        // for this complex check (precision > MAX_PRECISION || scale > precision), but we'll leave it as such and
        // have ORC perform this validation since MAX_PRECISION is not a public constant and might change in the future.
    }

    private List<ColumnDescriptor> buildAllTypes() {
        List<ColumnDescriptor> descriptors = new ArrayList<>();
        // scalar types
        descriptors.add(new ColumnDescriptor("col0", DataType.BOOLEAN.getOID(),0,"", null));
        descriptors.add(new ColumnDescriptor("col1", DataType.BYTEA.getOID(),1,"", null));
        descriptors.add(new ColumnDescriptor("col2", DataType.BIGINT.getOID(),2,"", null));
        descriptors.add(new ColumnDescriptor("col3", DataType.SMALLINT.getOID(),3,"", null));
        descriptors.add(new ColumnDescriptor("col4", DataType.INTEGER.getOID(),4,"", null));
        descriptors.add(new ColumnDescriptor("col5", DataType.TEXT.getOID(),5,"", null));
        descriptors.add(new ColumnDescriptor("col6", DataType.REAL.getOID(),6,"", null));
        descriptors.add(new ColumnDescriptor("col7", DataType.FLOAT8.getOID(),7,"", null));
        descriptors.add(new ColumnDescriptor("col8", DataType.BPCHAR.getOID(),8,"", null));
        descriptors.add(new ColumnDescriptor("col9", DataType.VARCHAR.getOID(),9,"", null));
        descriptors.add(new ColumnDescriptor("col10", DataType.DATE.getOID(),10,"", null));
        // TODO: descriptors.add(new ColumnDescriptor("col11", DataType.TIME.getOID(),11,"", null));
        descriptors.add(new ColumnDescriptor("col12", DataType.TIMESTAMP.getOID(),12,"", null));
        descriptors.add(new ColumnDescriptor("col13", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(),13,"", null));
        descriptors.add(new ColumnDescriptor("col14", DataType.NUMERIC.getOID(),14,"", null));
        descriptors.add(new ColumnDescriptor("col15", DataType.UUID.getOID(),15,"", null));
        // array types
        descriptors.add(new ColumnDescriptor("col16", DataType.INT2ARRAY.getOID(),16,"", null));
        descriptors.add(new ColumnDescriptor("col17", DataType.INT4ARRAY.getOID(),17,"", null));
        descriptors.add(new ColumnDescriptor("col18", DataType.INT8ARRAY.getOID(),18,"", null));
        descriptors.add(new ColumnDescriptor("col19", DataType.BOOLARRAY.getOID(),19,"", null));
        descriptors.add(new ColumnDescriptor("col20", DataType.TEXTARRAY.getOID(),20,"", null));
        descriptors.add(new ColumnDescriptor("col21", DataType.FLOAT4ARRAY.getOID(),21,"", null));
        descriptors.add(new ColumnDescriptor("col22", DataType.FLOAT8ARRAY.getOID(),22,"", null));
        descriptors.add(new ColumnDescriptor("col23", DataType.BYTEAARRAY.getOID(),23,"", null));
        descriptors.add(new ColumnDescriptor("col24", DataType.BPCHARARRAY.getOID(),24,"", null));
        descriptors.add(new ColumnDescriptor("col25", DataType.VARCHARARRAY.getOID(),25,"", null));
        descriptors.add(new ColumnDescriptor("col26", DataType.DATEARRAY.getOID(),26,"", null));
        descriptors.add(new ColumnDescriptor("col27", DataType.UUIDARRAY.getOID(),27,"", null));
        descriptors.add(new ColumnDescriptor("col28", DataType.NUMERICARRAY.getOID(),28,"", null));
        // TODO: descriptors.add(new ColumnDescriptor("col29", DataType.TIMEARRAY.getOID(),29,"", null));
        descriptors.add(new ColumnDescriptor("col30", DataType.TIMESTAMPARRAY.getOID(),30,"", null));
        descriptors.add(new ColumnDescriptor("col31", DataType.TIMESTAMP_WITH_TIMEZONE_ARRAY.getOID(),31,"", null));

        return descriptors;
    }
}
