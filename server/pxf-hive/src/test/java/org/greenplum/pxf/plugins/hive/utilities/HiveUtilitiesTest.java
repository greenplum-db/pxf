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


import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;

import com.google.common.base.Joiner;

import org.greenplum.pxf.api.io.DataType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.Test;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.UnsupportedTypeException;

public class HiveUtilitiesTest {

    FieldSchema hiveColumn;
    Metadata.Item tblDesc;

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

    @Test
    public void mapHiveTypeUnsupported() throws Exception {

        hiveColumn = new FieldSchema("complex", "someTypeWeDontSupport", null);

        try {
            HiveUtilities.mapHiveType(hiveColumn);
            fail("unsupported type");
        } catch (UnsupportedTypeException e) {
            assertEquals("Unable to map Hive's type: " + hiveColumn.getType() + " to GPDB's type", e.getMessage());
        }
    }

    @Test
    public void mapHiveTypeSimple() throws Exception {
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
        for (String[] line: typesMappings) {
            String hiveType = line[0];
            String gpdbTypeName = line[1];
            hiveColumn = new FieldSchema("field" + hiveType, hiveType, null);
            Metadata.Field result = HiveUtilities.mapHiveType(hiveColumn);
            assertEquals("field" + hiveType, result.getName());
            assertEquals(gpdbTypeName, result.getType().getTypeName());
            assertNull(result.getModifiers());
        }
    }

    @Test
    public void mapHiveTypeWithModifiers() throws Exception {
        /*
         * decimal -> numeric
         * varchar -> varchar
         * char -> bpchar
         */
        for (String[] line: typesWithModifiers) {
            String hiveType = line[0];
            String expectedType = line[1];
            String modifiersStr = line[2];
            String[] expectedModifiers = modifiersStr.split(",");
            hiveColumn = new FieldSchema("field" + hiveType, hiveType, null);
            Metadata.Field result = HiveUtilities.mapHiveType(hiveColumn);
            assertEquals("field" + hiveType, result.getName());
            assertEquals(expectedType, result.getType().getTypeName());
            assertArrayEquals(expectedModifiers, result.getModifiers());
        }
    }

    @Test
    public void testCompatibleHiveType() {

        String compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.BOOLEAN, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.BooleanType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.BYTEA, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.BinaryType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.BPCHAR, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.CharType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.BIGINT, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.BigintType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.SMALLINT, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.SmallintType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.INTEGER, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.IntType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.TEXT, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.StringType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.REAL, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.FloatType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.FLOAT8, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.DoubleType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.VARCHAR, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.VarcharType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.DATE, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.DateType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.TIMESTAMP, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.TimestampType.getTypeName());

        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.NUMERIC, null);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.DecimalType.getTypeName());

        try {
            compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.UNSUPPORTED_TYPE, null);
            fail("should fail because there is no mapped Hive type");
        }
        catch (UnsupportedTypeException e) {
            String errorMsg = "Unable to find compatible Hive type for given GPDB's type: " + DataType.UNSUPPORTED_TYPE;
            assertEquals(errorMsg, e.getMessage());
        }


    }

    @Test
    public void testCompatibleHiveTypeWithModifiers() {

        Integer[] gpdbModifiers;
        String compatibleTypeName;

        gpdbModifiers = new Integer[]{5};
        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.BPCHAR, gpdbModifiers);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.CharType.getTypeName() + "(" + Joiner.on(",").join(gpdbModifiers) + ")");

        gpdbModifiers = new Integer[]{10};
        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.VARCHAR, gpdbModifiers);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.VarcharType.getTypeName() + "(" + Joiner.on(",").join(gpdbModifiers) + ")");

        gpdbModifiers = new Integer[]{38, 18};
        compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.NUMERIC, gpdbModifiers);
        assertEquals(compatibleTypeName, EnumHiveToGpdbType.DecimalType.getTypeName() + "(" + Joiner.on(",").join(gpdbModifiers) + ")");

        try {
            compatibleTypeName = HiveUtilities.toCompatibleHiveType(DataType.UNSUPPORTED_TYPE, gpdbModifiers);
            fail("should fail because there is no mapped Hive type");
        }
        catch (UnsupportedTypeException e) {
            String errorMsg = "Unable to find compatible Hive type for given GPDB's type: " + DataType.UNSUPPORTED_TYPE;
            assertEquals(errorMsg, e.getMessage());
        }


    }

    @Test
    public void validateSchema() throws Exception {
        String columnName = "abc";

        Integer[] gpdbModifiers = {};
        HiveUtilities.validateTypeCompatible(DataType.SMALLINT, gpdbModifiers, EnumHiveToGpdbType.TinyintType.getTypeName(), columnName);

        HiveUtilities.validateTypeCompatible(DataType.SMALLINT, gpdbModifiers, EnumHiveToGpdbType.SmallintType.getTypeName(), columnName);

        //Both Hive and GPDB types have the same modifiers
        gpdbModifiers = new Integer[]{38, 18};
        HiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "decimal(38,18)", columnName);

        //GPDB datatype doesn't require modifiers, they are empty, Hive has non-empty modifiers
        //Types are compatible in this case
        gpdbModifiers = new Integer[]{};
        HiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "decimal(38,18)", columnName);
        gpdbModifiers = null;
        HiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "decimal(38,18)", columnName);

        //GPDB has wider modifiers than Hive, types are compatible
        gpdbModifiers = new Integer[]{11, 3};
        HiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "decimal(10,2)", columnName);


        //GPDB has lesser modifiers than Hive, types aren't compatible
        try {
            gpdbModifiers = new Integer[]{38, 17};
            HiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "decimal(38,18)", columnName);
            fail("should fail with incompatible modifiers message");
        }
        catch (UnsupportedTypeException e) {
            String errorMsg = "Invalid definition for column " + columnName
                    +  ": modifiers are not compatible, "
                    + Arrays.toString(new String[]{"38", "18"}) + ", "
                    + Arrays.toString(new String[]{"38", "17"});
            assertEquals(errorMsg, e.getMessage());
        }


        //Different types, which are not mapped to each other
        try {
            gpdbModifiers = new Integer[]{};
            HiveUtilities.validateTypeCompatible(DataType.NUMERIC, gpdbModifiers, "boolean", columnName);
            fail("should fail with incompatible types message");
        }
        catch (UnsupportedTypeException e) {
            String errorMsg = "Invalid definition for column " + columnName
                    + ": expected GPDB type " + DataType.BOOLEAN
                    + ", actual GPDB type " + DataType.NUMERIC;
            assertEquals(errorMsg, e.getMessage());
        }
    }

    @Test
    public void extractModifiers() throws Exception {
        Integer[] mods = EnumHiveToGpdbType.extractModifiers("decimal(10,2)");
        assertArrayEquals(mods, new Integer[]{10, 2});
    }

    @Test
    public void mapHiveTypeWithModifiersNegative() throws Exception {

        String badHiveType = "decimal(2)";
        hiveColumn = new FieldSchema("badNumeric", badHiveType, null);
        try {
            HiveUtilities.mapHiveType(hiveColumn);
            fail("should fail with bad numeric type error");
        } catch (UnsupportedTypeException e) {
            String errorMsg = "GPDB does not support type " + badHiveType + " (Field badNumeric), " +
                "expected number of modifiers: 2, actual number of modifiers: 1";
            assertEquals(errorMsg, e.getMessage());
        }

        badHiveType = "char(1,2,3)";
        hiveColumn = new FieldSchema("badChar", badHiveType, null);
        try {
            HiveUtilities.mapHiveType(hiveColumn);
            fail("should fail with bad char type error");
        } catch (UnsupportedTypeException e) {
            String errorMsg = "GPDB does not support type " + badHiveType + " (Field badChar), " +
                    "expected number of modifiers: 1, actual number of modifiers: 3";
            assertEquals(errorMsg, e.getMessage());
        }

        badHiveType = "char(acter)";
        hiveColumn = new FieldSchema("badModifier", badHiveType, null);
        try {
            HiveUtilities.mapHiveType(hiveColumn);
            fail("should fail with bad modifier error");
        } catch (UnsupportedTypeException e) {
            String errorMsg = "GPDB does not support type " + badHiveType + " (Field badModifier), " +
                "modifiers should be integers";
            assertEquals(errorMsg, e.getMessage());
        }
    }

    @Test
    public void mapHiveTypeInvalidModifiers() throws Exception {
        String badHiveType = "decimal(abc, xyz)";
        hiveColumn = new FieldSchema("numericColumn", badHiveType, null);
        try {
            HiveUtilities.mapHiveType(hiveColumn);
            fail("should fail with bad modifiers error");
        } catch (UnsupportedTypeException e) {
            String errorMsg = "GPDB does not support type " + badHiveType + " (Field numericColumn), modifiers should be integers";
            assertEquals(errorMsg, e.getMessage());
        }
    }

    @Test
    public void mapHiveTypeComplex() throws Exception {
        /*
         * array<dataType> -> text
         * map<keyDataType, valueDataType> -> text
         * struct<fieldName1:dataType, ..., fieldNameN:dataType> -> text
         * uniontype<...> -> text
         */
        for (String[] line: complexTypes) {
            String hiveType = line[0];
            String expectedType = line[1];
            hiveColumn = new FieldSchema("field" + hiveType, hiveType, null);
            Metadata.Field result = HiveUtilities.mapHiveType(hiveColumn);
            assertEquals("field" + hiveType, result.getName());
            assertEquals(expectedType, result.getType().getTypeName());
            assertNull(result.getModifiers());
        }
    }

    @Test
    public void parseTableQualifiedNameNoDbName() throws Exception {
        String name = "orphan";
        tblDesc = HiveUtilities.extractTableFromName(name);

        assertEquals("default", tblDesc.getPath());
        assertEquals(name, tblDesc.getName());
    }

    @Test
    public void parseTableQualifiedName() throws Exception {
        String name = "not.orphan";
        tblDesc = HiveUtilities.extractTableFromName(name);

        assertEquals("not", tblDesc.getPath());
        assertEquals("orphan", tblDesc.getName());
    }

    @Test
    public void parseTableQualifiedNameTooManyQualifiers() throws Exception {
        String name = "too.many.parents";
        String errorMsg = surroundByQuotes(name) + " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";

        parseTableQualifiedNameNegative(name, errorMsg, "too many qualifiers");
    }

    @Test
    public void parseTableQualifiedNameEmpty() throws Exception {
        String name = "";
        String errorMsg = "empty string is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";

        parseTableQualifiedNameNegative(name, errorMsg, "empty string");

        name = null;
        parseTableQualifiedNameNegative(name, errorMsg, "null string");

        name = ".";
        errorMsg = surroundByQuotes(name) + " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";
        parseTableQualifiedNameNegative(name, errorMsg, "empty db and table names");

        name = " . ";
        errorMsg = surroundByQuotes(name) + " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";
        parseTableQualifiedNameNegative(name, errorMsg, "only white spaces in string");
    }

    private String surroundByQuotes(String str) {
        return "\"" + str + "\"";
    }

    private void parseTableQualifiedNameNegative(String name, String errorMsg, String reason) throws Exception {
        try {
            tblDesc = HiveUtilities.extractTableFromName(name);
            fail("test should fail because of " + reason);
        } catch (IllegalArgumentException e) {
            assertEquals(errorMsg, e.getMessage());
        }
    }

    @Test
    public void getDelimiterCode() {

        //Default delimiter code should be 44(comma)
        Integer delimiterCode = HiveUtilities.getDelimiterCode(null);
        char defaultDelim = ',';
        assertTrue(delimiterCode == (int) defaultDelim);

        //Some serdes use FIELD_DELIM key
        char expectedDelim = '%';
        StorageDescriptor sd = new StorageDescriptor();
        SerDeInfo si = new SerDeInfo();
        si.setParameters(Collections.singletonMap(serdeConstants.FIELD_DELIM, String.valueOf(expectedDelim)));
        sd.setSerdeInfo(si);
        delimiterCode = HiveUtilities.getDelimiterCode(sd);
        assertTrue(delimiterCode == (int) expectedDelim);

        //Some serdes use SERIALIZATION_FORMAT key
        sd = new StorageDescriptor();
        si = new SerDeInfo();
        si.setParameters(Collections.singletonMap(serdeConstants.SERIALIZATION_FORMAT, String.valueOf((int)expectedDelim)));
        sd.setSerdeInfo(si);
        delimiterCode = HiveUtilities.getDelimiterCode(sd);
        assertTrue(delimiterCode == (int) expectedDelim);
    }
}
