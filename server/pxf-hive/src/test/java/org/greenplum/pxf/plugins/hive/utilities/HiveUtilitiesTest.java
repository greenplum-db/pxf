package org.greenplum.pxf.plugins.hive.utilities;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.google.common.base.Joiner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Metadata;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HiveUtilitiesTest {

    FieldSchema hiveColumn;

    static String[][] typesMappings = {
            /* hive type -> gpdb type */
            {"tinyint", "int2"},
            {"smallint", "int2"},
            {"int", "int4"},
            {"bigint", "int8"},
            {"boolean", "bool"},
            {"float", "float4"},
            {"double", "float8"},
            {"string", "text"},
            {"binary", "bytea"},
            {"timestamp", "timestamp"},
            {"date", "date"},
    };

    static String[][] typesWithModifiers = {
            {"decimal(19,84)", "numeric", "19,84"},
            {"varchar(13)", "varchar", "13"},
            {"char(40)", "bpchar", "40"},
    };

    static String[][] complexTypes = {
            {"ArraY<string>", "text"},
            {"MaP<stRing, float>", "text"},
            {"Struct<street:string, city:string, state:string, zip:int>", "text"},
            {"UnionType<array<string>, string,int>", "text"}
    };
    
    private final HiveUtilities hiveUtilities = new HiveUtilities();

    @Test
    public void mapHiveTypeUnsupported() {

        hiveColumn = new FieldSchema("complex", "someTypeWeDontSupport", null);

        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> hiveUtilities.mapHiveType(hiveColumn),
                "unsupported type");

        assertEquals("Unable to map Hive's type: " + hiveColumn.getType() + " to GPDB's type", e.getMessage());
    }

    @Test
    public void mapHiveTypeSimple() {
        /*
         * tinyint -> int2
         * smallint -> int2
         * int -> int4
         * bigint -> int8
         * boolean -> bool
         * float -> float4
         * double -> float8
         * string -> text
         * binary -> bytea
         * timestamp -> timestamp
         * date -> date
         */
        for (String[] line : typesMappings) {
            String hiveType = line[0];
            String gpdbTypeName = line[1];
            hiveColumn = new FieldSchema("field" + hiveType, hiveType, null);
            Metadata.Field result = hiveUtilities.mapHiveType(hiveColumn);
            assertEquals("field" + hiveType, result.getName());
            assertEquals(gpdbTypeName, result.getType().getTypeName());
            assertNull(result.getModifiers());
        }
    }

    @Test
    public void mapHiveTypeWithModifiers() {
        /*
         * decimal -> numeric
         * varchar -> varchar
         * char -> bpchar
         */
        for (String[] line : typesWithModifiers) {
            String hiveType = line[0];
            String expectedType = line[1];
            String modifiersStr = line[2];
            String[] expectedModifiers = modifiersStr.split(",");
            hiveColumn = new FieldSchema("field" + hiveType, hiveType, null);
            Metadata.Field result = hiveUtilities.mapHiveType(hiveColumn);
            assertEquals("field" + hiveType, result.getName());
            assertEquals(expectedType, result.getType().getTypeName());
            assertArrayEquals(expectedModifiers, result.getModifiers());
        }
    }

    @Test
    public void testCompatibleHiveType() {

        String compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.BOOLEAN, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.BooleanType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.BYTEA, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.BinaryType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.BPCHAR, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.CharType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.BIGINT, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.BigintType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.SMALLINT, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.SmallintType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.INTEGER, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.IntType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.TEXT, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.StringType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.REAL, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.FloatType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.FLOAT8, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.DoubleType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.VARCHAR, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.VarcharType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.DATE, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.DateType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.TIMESTAMP, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.TimestampType.getTypeName());

        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.NUMERIC, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.DecimalType.getTypeName());

        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> hiveUtilities.toCompatibleHiveType(DataType.UNSUPPORTED_TYPE, null),
                "should fail because there is no mapped Hive type");
        String errorMsg = "Unable to find compatible Hive type for given GPDB's type: " + DataType.UNSUPPORTED_TYPE;
        assertEquals(errorMsg, e.getMessage());
    }

    @Test
    public void testCompatibleHiveTypeWithModifiers() {

        Integer[] gpdbModifiers;
        String compatibleTypeName;

        gpdbModifiers = new Integer[]{5};
        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.BPCHAR, gpdbModifiers);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.CharType.getTypeName() + "(" + Joiner.on(",").join(gpdbModifiers) + ")");

        gpdbModifiers = new Integer[]{10};
        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.VARCHAR, gpdbModifiers);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.VarcharType.getTypeName() + "(" + Joiner.on(",").join(gpdbModifiers) + ")");

        gpdbModifiers = new Integer[]{38, 18};
        compatibleTypeName = hiveUtilities.toCompatibleHiveType(DataType.NUMERIC, gpdbModifiers);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.DecimalType.getTypeName() + "(" + Joiner.on(",").join(gpdbModifiers) + ")");

        Integer[] finalGpdbModifiers = gpdbModifiers;
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> hiveUtilities.toCompatibleHiveType(DataType.UNSUPPORTED_TYPE, finalGpdbModifiers),
                "should fail because there is no mapped Hive type");
        String errorMsg = "Unable to find compatible Hive type for given GPDB's type: " + DataType.UNSUPPORTED_TYPE;
        assertEquals(errorMsg, e.getMessage());
    }

    @Test
    public void validateSchema() {
        String columnName = "abc";

        Integer[] gpdbModifiers = {};
        hiveUtilities.validateTypeCompatible(DataType.SMALLINT, gpdbModifiers, EnumHiveToGpdbType.TinyintType.getTypeName(), columnName);

        hiveUtilities.validateTypeCompatible(DataType.SMALLINT, gpdbModifiers, EnumHiveToGpdbType.SmallintType.getTypeName(), columnName);

        //Both Hive and GPDB types have the same modifiers
        gpdbModifiers = new Integer[]{38, 18};
        hiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "decimal(38,18)", columnName);

        //GPDB datatype doesn't require modifiers, they are empty, Hive has non-empty modifiers
        //Types are compatible in this case
        gpdbModifiers = new Integer[]{};
        hiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "decimal(38,18)", columnName);
        gpdbModifiers = null;
        hiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "decimal(38,18)", columnName);

        //GPDB has wider modifiers than Hive, types are compatible
        gpdbModifiers = new Integer[]{11, 3};
        hiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "decimal(10,2)", columnName);

        //GPDB has lesser modifiers than Hive, types aren't compatible
        gpdbModifiers = new Integer[]{38, 17};
        Integer[] finalGpdbModifiers = gpdbModifiers;
        UnsupportedTypeException e = assertThrows(UnsupportedTypeException.class,
                () -> hiveUtilities.validateTypeCompatible(DataType.NUMERIC, finalGpdbModifiers, "decimal(38,18)", columnName),
                "should fail with incompatible modifiers message");
        String errorMsg = "Invalid definition for column " + columnName
                + ": modifiers are not compatible, "
                + Arrays.toString(new String[]{"38", "18"}) + ", "
                + Arrays.toString(new String[]{"38", "17"});
        assertEquals(errorMsg, e.getMessage());

        //Different types, which are not mapped to each other
        Integer[] finalGpdbModifiers1 = new Integer[]{};
        e = assertThrows(UnsupportedTypeException.class,
                () -> hiveUtilities.validateTypeCompatible(DataType.NUMERIC, finalGpdbModifiers1, "boolean", columnName),
                "should fail with incompatible types message");
        errorMsg = "Invalid definition for column " + columnName
                + ": expected GPDB type " + DataType.BOOLEAN
                + ", actual GPDB type " + DataType.NUMERIC;
        assertEquals(errorMsg, e.getMessage());
    }

    @Test
    public void extractModifiers() {
        Integer[] mods = EnumHiveToGpdbType.extractModifiers("decimal(10,2)");
        assertArrayEquals(mods, new Integer[]{10, 2});
    }

    @Test
    public void mapHiveTypeWithModifiersNegative() {

        String badHiveType = "decimal(2)";
        hiveColumn = new FieldSchema("badNumeric", badHiveType, null);
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> hiveUtilities.mapHiveType(hiveColumn),
                "should fail with bad numeric type error");
        String errorMsg = "GPDB does not support type " + badHiveType + " (Field badNumeric), " +
                "expected number of modifiers: 2, actual number of modifiers: 1";
        assertEquals(errorMsg, e.getMessage());

        badHiveType = "char(1,2,3)";
        hiveColumn = new FieldSchema("badChar", badHiveType, null);
        e = assertThrows(UnsupportedTypeException.class,
                () -> hiveUtilities.mapHiveType(hiveColumn),
                "should fail with bad char type error");
        errorMsg = "GPDB does not support type " + badHiveType + " (Field badChar), " +
                "expected number of modifiers: 1, actual number of modifiers: 3";
        assertEquals(errorMsg, e.getMessage());

        badHiveType = "char(acter)";
        hiveColumn = new FieldSchema("badModifier", badHiveType, null);
        e = assertThrows(UnsupportedTypeException.class,
                () -> hiveUtilities.mapHiveType(hiveColumn),
                "should fail with bad modifier error");
        errorMsg = "GPDB does not support type " + badHiveType + " (Field badModifier), " +
                "modifiers should be integers";
        assertEquals(errorMsg, e.getMessage());
    }

    @Test
    public void mapHiveTypeInvalidModifiers() {
        String badHiveType = "decimal(abc, xyz)";
        hiveColumn = new FieldSchema("numericColumn", badHiveType, null);
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> hiveUtilities.mapHiveType(hiveColumn),
                "should fail with bad modifiers error");
        String errorMsg = "GPDB does not support type " + badHiveType + " (Field numericColumn), modifiers should be integers";
        assertEquals(errorMsg, e.getMessage());
    }

    @Test
    public void mapHiveTypeComplex() {
        /*
         * array<dataType> -> text
         * map<keyDataType, valueDataType> -> text
         * struct<fieldName1:dataType, ..., fieldNameN:dataType> -> text
         * uniontype<...> -> text
         */
        for (String[] line : complexTypes) {
            String hiveType = line[0];
            String expectedType = line[1];
            hiveColumn = new FieldSchema("field" + hiveType, hiveType, null);
            Metadata.Field result = hiveUtilities.mapHiveType(hiveColumn);
            assertEquals("field" + hiveType, result.getName());
            assertEquals(expectedType, result.getType().getTypeName());
            assertNull(result.getModifiers());
        }
    }
}
