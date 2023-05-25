package org.greenplum.pxf.plugins.hdfs.orc;

import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ORCSchemaBuilderTest {

    public static final String ALL_TYPES_SCHEMA = new StringJoiner(",", "struct<", ">")
            .add("col0:boolean")
            .add("col1:binary")
            .add("col2:bigint")
            .add("col3:smallint")
            .add("col4:int")
            .add("col5:string")
            .add("col6:float")
            .add("col7:double")
            .add("col8:char(12)")
            .add("col9:varchar(24)")
            .add("col10:string")
            .add("col11:date")
            .add("col12:string")
            .add("col13:timestamp")
            .add("col14:timestamp with local time zone")
            .add("col15:decimal(38,10)")
            .add("col16:string")
            .add("col17:array<smallint>")
            .add("col18:array<int>")
            .add("col19:array<bigint>")
            .add("col20:array<boolean>")
            .add("col21:array<string>")
            .add("col22:array<float>")
            .add("col23:array<double>")
            .add("col24:array<binary>")
            .add("col25:array<char(12)>")
            .add("col26:array<varchar(24)>")
            .add("col27:array<string>")
            .add("col28:array<date>")
            .add("col29:array<string>")
            .add("col30:array<decimal(38,10)>")
            .add("col31:array<string>")
            .add("col32:array<timestamp>")
            .add("col33:array<timestamp with local time zone>").toString();

    private List<ColumnDescriptor> columnDescriptors = new ArrayList<>();

    @BeforeEach
    public void setup() {
        columnDescriptors.clear();
    }

    @Test
    public void testNoColumnDescriptors() {
        assertNull(ORCSchemaBuilder.buildSchema(null));
    }

    @Test
    public void testEmptyColumnDescriptors() {
        assertEquals("struct<>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());
    }

    @Test
    public void testUnsupportedType() {
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.UNSUPPORTED_TYPE.getOID(), 0, "", null));
        Exception e = assertThrows(PxfRuntimeException.class, () -> ORCSchemaBuilder.buildSchema(columnDescriptors));
        assertEquals("Unsupported Greenplum type -1 for column col0", e.getMessage());
    }

    @Test
    public void testAllSupportedTypes() {
        columnDescriptors = buildAllTypes();
        assertEquals(ALL_TYPES_SCHEMA, ORCSchemaBuilder.buildSchema(columnDescriptors).toString());
    }

    @Test
    public void testBpcharMaxLength() {
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.BPCHAR.getOID(), 0, "", new Integer[]{}));
        Exception e = assertThrows(PxfRuntimeException.class, () -> ORCSchemaBuilder.buildSchema(columnDescriptors));
        assertEquals("Column col0 of CHAR type must have maximum size information.", e.getMessage());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.BPCHAR.getOID(), 0, "", new Integer[]{null}));
        e = assertThrows(PxfRuntimeException.class, () -> ORCSchemaBuilder.buildSchema(columnDescriptors));
        assertEquals("Column col0 of CHAR type must have maximum size information.", e.getMessage());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.BPCHAR.getOID(), 0, "", new Integer[]{3}));
        assertEquals("struct<col0:char(3)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.BPCHAR.getOID(), 0, "", new Integer[]{300}));
        assertEquals("struct<col0:char(300)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());
    }

    @Test
    public void testVarcharMaxLength() {
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.VARCHAR.getOID(), 0, "", new Integer[]{}));
        assertEquals("struct<col0:string>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.VARCHAR.getOID(), 0, "", new Integer[]{null}));
        assertEquals("struct<col0:string>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.VARCHAR.getOID(), 0, "", new Integer[]{3}));
        assertEquals("struct<col0:varchar(3)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.VARCHAR.getOID(), 0, "", new Integer[]{300}));
        assertEquals("struct<col0:varchar(300)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());
    }

    @Test
    public void testNumericPrecisionAndScale() {
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{}));
        assertEquals("struct<col0:decimal(38,10)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{20, 5}));
        assertEquals("struct<col0:decimal(20,5)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        // precision and scale are both explicit nulls, same as missing, defaults are assumed
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{null, null}));
        assertEquals("struct<col0:decimal(38,10)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        // precision is null, scale is not null, error is reported
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{null, 5}));
        Exception e = assertThrows(PxfRuntimeException.class, () -> ORCSchemaBuilder.buildSchema(columnDescriptors));
        assertEquals("Invalid modifiers: scale defined as 5 while precision is not set.", e.getMessage());

        // scale is missing, defaulted to 0
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{20}));
        assertEquals("struct<col0:decimal(20,0)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        // scale is null, defaulted to 0
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{20}));
        assertEquals("struct<col0:decimal(20,0)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        // precision is smaller than ORC default scale of 10, scale missing, and we've changed the default scale from 10 to 0
        // we will not get an error from the check (precision > 38 || scale > precision)
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{8}));
        assertEquals("struct<col0:decimal(8,0)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        // precision is smaller than ORC default scale of 10, scale is provided
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{8, 2}));
        assertEquals("struct<col0:decimal(8,2)>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());

        // precision is larger than ORC max of 38
        // we've replaced the orc's error messages with our more detailed one when failed on (precision > 38 || scale > precision)
        columnDescriptors.clear();
        columnDescriptors.add(new ColumnDescriptor("col0", DataType.NUMERIC.getOID(), 0, "", new Integer[]{55}));
        e = assertThrows(UnsupportedTypeException.class, () -> ORCSchemaBuilder.buildSchema(columnDescriptors));
        assertEquals("Column col0 is defined as NUMERIC with precision 55 which exceeds maximum supported precision 38 of ORC.", e.getMessage());
    }

    @Test
    public void testComplexColumnNames() {
        columnDescriptors.add(new ColumnDescriptor("Hello World", DataType.INTEGER.getOID(), 0, "", null));
        columnDescriptors.add(new ColumnDescriptor("привет", DataType.TEXT.getOID(), 0, "", null));
        columnDescriptors.add(new ColumnDescriptor("simple", DataType.TEXT.getOID(), 0, "", null));
        columnDescriptors.add(new ColumnDescriptor("谢谢你", DataType.TEXT.getOID(), 0, "", null));
        // ORC schema prints non-latin-alpha-num ("^[a-zA-Z0-9_]+$") column names as escaped with "`" character
        assertEquals("struct<`Hello World`:int,`привет`:string,simple:string,`谢谢你`:string>", ORCSchemaBuilder.buildSchema(columnDescriptors).toString());
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
        descriptors.add(new ColumnDescriptor("col8", DataType.BPCHAR.getOID(),8,"", new Integer[]{12}));
        descriptors.add(new ColumnDescriptor("col9", DataType.VARCHAR.getOID(),9,"", new Integer[]{24}));
        descriptors.add(new ColumnDescriptor("col10", DataType.VARCHAR.getOID(),10,"", null)); // no length provided
        descriptors.add(new ColumnDescriptor("col11", DataType.DATE.getOID(),11,"", null));
        descriptors.add(new ColumnDescriptor("col12", DataType.TIME.getOID(),12,"", null));
        descriptors.add(new ColumnDescriptor("col13", DataType.TIMESTAMP.getOID(),13,"", null));
        descriptors.add(new ColumnDescriptor("col14", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(),14,"", null));
        descriptors.add(new ColumnDescriptor("col15", DataType.NUMERIC.getOID(),15,"", null));
        descriptors.add(new ColumnDescriptor("col16", DataType.UUID.getOID(),16,"", null));
        // array types
        descriptors.add(new ColumnDescriptor("col17", DataType.INT2ARRAY.getOID(),17,"", null));
        descriptors.add(new ColumnDescriptor("col18", DataType.INT4ARRAY.getOID(),18,"", null));
        descriptors.add(new ColumnDescriptor("col19", DataType.INT8ARRAY.getOID(),19,"", null));
        descriptors.add(new ColumnDescriptor("col20", DataType.BOOLARRAY.getOID(),20,"", null));
        descriptors.add(new ColumnDescriptor("col21", DataType.TEXTARRAY.getOID(),21,"", null));
        descriptors.add(new ColumnDescriptor("col22", DataType.FLOAT4ARRAY.getOID(),22,"", null));
        descriptors.add(new ColumnDescriptor("col23", DataType.FLOAT8ARRAY.getOID(),23,"", null));
        descriptors.add(new ColumnDescriptor("col24", DataType.BYTEAARRAY.getOID(),24,"", null));
        descriptors.add(new ColumnDescriptor("col25", DataType.BPCHARARRAY.getOID(),25,"", new Integer[]{12}));
        descriptors.add(new ColumnDescriptor("col26", DataType.VARCHARARRAY.getOID(),26,"", new Integer[]{24}));
        descriptors.add(new ColumnDescriptor("col27", DataType.VARCHARARRAY.getOID(),27,"", null)); // no length provided
        descriptors.add(new ColumnDescriptor("col28", DataType.DATEARRAY.getOID(),28,"", null));
        descriptors.add(new ColumnDescriptor("col29", DataType.UUIDARRAY.getOID(),29,"", null));
        descriptors.add(new ColumnDescriptor("col30", DataType.NUMERICARRAY.getOID(),30,"", null));
        descriptors.add(new ColumnDescriptor("col31", DataType.TIMEARRAY.getOID(),31,"", null));
        descriptors.add(new ColumnDescriptor("col32", DataType.TIMESTAMPARRAY.getOID(),32,"", null));
        descriptors.add(new ColumnDescriptor("col33", DataType.TIMESTAMP_WITH_TIMEZONE_ARRAY.getOID(),33,"", null));

        return descriptors;
    }
}
