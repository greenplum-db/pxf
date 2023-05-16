package org.greenplum.pxf.api.io;

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


import java.util.EnumSet;

/**
 * Supported Data Types and OIDs (GPDB Data Type identifiers).
 * There's a one-to-one match between a Data Type and it's corresponding OID.
 */
public enum DataType {
    BOOLEAN(16, false),
    BYTEA(17, true),
    BIGINT(20, false),
    SMALLINT(21, false),
    INTEGER(23, false),
    TEXT(25, true),
    REAL(700, false),
    FLOAT8(701, false),
    /**
     * char(length), blank-padded string, fixed storage length
     */
    BPCHAR(1042, true),
    /**
     * varchar(length), non-blank-padded string, variable storage length
     */
    VARCHAR(1043, true),
    DATE(1082, false),
    TIME(1083, true),
    TIMESTAMP(1114, true),
    TIMESTAMP_WITH_TIME_ZONE(1184, true),
    NUMERIC(1700, false),
    UUID(2950, false),

    INT2ARRAY(1005),
    INT4ARRAY(1007),
    INT8ARRAY(1016),
    BOOLARRAY(1000),
    TEXTARRAY(1009),
    FLOAT4ARRAY(1021),
    FLOAT8ARRAY(1022),
    BYTEAARRAY(1001),
    BPCHARARRAY(1014),
    VARCHARARRAY(1015),
    DATEARRAY(1182),
    UUIDARRAY(2951),
    NUMERICARRAY(1231),
    TIMEARRAY(1183),
    TIMESTAMPARRAY(1115),
    TIMESTAMP_WITH_TIMEZONE_ARRAY(1185),

    UNSUPPORTED_TYPE(-1);

    private static final int[] OID_ARRAY;
    private static final DataType[] DATA_TYPES;
    private static final int[] NOT_TEXT = {BIGINT.OID, BOOLEAN.OID, BYTEA.OID,
            FLOAT8.OID, INTEGER.OID, REAL.OID, SMALLINT.OID};

    // Set of types that preserve the type information when their value is deserialized,
    // this is similar to NOT_TEXT above, but used explicitly in the deserialization case of PXF Write Flow
    private static EnumSet<DataType> SELF_DESER_TYPES = EnumSet.of(BOOLEAN, SMALLINT, INTEGER, BIGINT, REAL, FLOAT8, BYTEA);

    static {
        INT2ARRAY.typeElem = SMALLINT;
        INT4ARRAY.typeElem = INTEGER;
        INT8ARRAY.typeElem = BIGINT;
        BOOLARRAY.typeElem = BOOLEAN;
        TEXTARRAY.typeElem = TEXT;
        FLOAT4ARRAY.typeElem = REAL;
        FLOAT8ARRAY.typeElem = FLOAT8;
        BYTEAARRAY.typeElem = BYTEA;
        BPCHARARRAY.typeElem = BPCHAR;
        VARCHARARRAY.typeElem = VARCHAR;
        DATEARRAY.typeElem = DATE;
        UUIDARRAY.typeElem = UUID;
        NUMERICARRAY.typeElem = NUMERIC;
        TIMEARRAY.typeElem = TIME;
        TIMESTAMPARRAY.typeElem = TIMESTAMP;
        TIMESTAMP_WITH_TIMEZONE_ARRAY.typeElem = TIMESTAMP_WITH_TIME_ZONE;

        SMALLINT.typeArray = INT2ARRAY;
        INTEGER.typeArray = INT4ARRAY;
        BIGINT.typeArray = INT8ARRAY;
        BOOLEAN.typeArray = BOOLARRAY;
        TEXT.typeArray = TEXTARRAY;
        REAL.typeArray = FLOAT4ARRAY;
        FLOAT8.typeArray = FLOAT8ARRAY;
        BYTEA.typeArray = BYTEAARRAY;
        BPCHAR.typeArray = BPCHARARRAY;
        VARCHAR.typeArray = VARCHARARRAY;
        DATE.typeArray = DATEARRAY;
        UUID.typeArray = UUIDARRAY;
        NUMERIC.typeArray = NUMERICARRAY;
        TIME.typeArray = TIMEARRAY;
        TIMESTAMP.typeArray = TIMESTAMPARRAY;
        TIMESTAMP_WITH_TIME_ZONE.typeArray = TIMESTAMP_WITH_TIMEZONE_ARRAY;

        DataType[] allTypes = DataType.values();
        OID_ARRAY = new int[allTypes.length];
        DATA_TYPES = new DataType[allTypes.length];

        int index = 0;
        for (DataType type : allTypes) {
            OID_ARRAY[index] = type.OID;
            DATA_TYPES[index] = type;
            index++;
        }
    }

    private final int OID;
    private final boolean needsEscapingInArray;
    private DataType typeElem;
    private DataType typeArray;

    DataType(int OID, boolean needsEscapingInArray) {
        this.OID = OID;
        this.needsEscapingInArray = needsEscapingInArray;
    }

    DataType(int OID) {
        this.OID = OID;
        this.needsEscapingInArray = true;
    }

    /**
     * Utility method for converting an {@link #OID} to a {@link #DataType}.
     *
     * @param OID the oid to be converted
     * @return the corresponding DataType if exists, else returns {@link #UNSUPPORTED_TYPE}
     */
    public static DataType get(int OID) {
        // Previously, this lookup was based on a HashMap, but during profiling
        // we noticed that the Hashmap.get call was a hot spot. A for loop is
        // more performant when the number of elements is low (usually less
        // than 100). We built a small benchmark based on JMH to compare the
        // two implementations and here are the results we obtained at that
        // time:
        //
        // Throughput Benchmark (Higher score is better)
        // Benchmark                               (iterations)   Mode  Cnt    Score    Error   Units
        // DemoApplication.benchmarkGetForLoop            10000  thrpt   40  477.072 ± 11.663  ops/us
        // DemoApplication.benchmarkHashMapLookup         10000  thrpt   40    0.009 ±  0.001  ops/us
        //
        // Average Time Benchmark (Lower score is better)
        // Benchmark                               (iterations)  Mode  Cnt    Score    Error  Units
        // DemoApplication.benchmarkGetForLoop            10000  avgt   40    0.002 ±  0.001  us/op
        // DemoApplication.benchmarkHashMapLookup         10000  avgt   40  110.740 ±  5.670  us/op
        for (int i = 0; i < OID_ARRAY.length; i++) {
            if (OID == OID_ARRAY[i]) {
                return DATA_TYPES[i];
            }
        }
        return UNSUPPORTED_TYPE;
    }

    public boolean isArrayType() {
        return typeElem != null;
    }

    public static boolean isTextForm(int OID) {
        for (int value : NOT_TEXT) {
            if (OID == value) return false;
        }
        return true;
    }

    public int getOID() {
        return OID;
    }

    public DataType getTypeElem() {
        return typeElem;
    }

    public DataType getTypeArray() {
        return typeArray;
    }

    public boolean getNeedsEscapingInArray() {
        return needsEscapingInArray;
    }

    /**
     * Returns the type that deserialization logic needs to report for backward compatibility with GPDBWritable,
     * where only boolean/short/int/long/float/double/bytea are represented by their actual types
     * and the rest of data types are represented as TEXT by the deserialization logic.
     * @return the corresponding DataType when deserializing a value of a given type
     */
    public DataType getDeserializationType() {
        if (SELF_DESER_TYPES.contains(this)) {
            return this;          // return itself as the type that should be reported
        } else {
            return DataType.TEXT; // everything else is reported as TEXT once deserialized
        }
    }
}
