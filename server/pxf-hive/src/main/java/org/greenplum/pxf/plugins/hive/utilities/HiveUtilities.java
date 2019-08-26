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


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.EnumGpdbType;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hive.HiveUserData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


/**
 * Class containing helper functions connecting
 * and interacting with Hive.
 */
public class HiveUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(HiveUtilities.class);

    /**
     * Checks if hive type is supported, and if so return its matching GPDB
     * type. Unsupported types will result in an exception. <br>
     * The supported mappings are:
     * <ul>
     * <li>{@code tinyint -> int2}</li>
     * <li>{@code smallint -> int2}</li>
     * <li>{@code int -> int4}</li>
     * <li>{@code bigint -> int8}</li>
     * <li>{@code boolean -> bool}</li>
     * <li>{@code float -> float4}</li>
     * <li>{@code double -> float8}</li>
     * <li>{@code string -> text}</li>
     * <li>{@code binary -> bytea}</li>
     * <li>{@code timestamp -> timestamp}</li>
     * <li>{@code date -> date}</li>
     * <li>{@code decimal(precision, scale) -> numeric(precision, scale)}</li>
     * <li>{@code varchar(size) -> varchar(size)}</li>
     * <li>{@code char(size) -> bpchar(size)}</li>
     * <li>{@code array<dataType> -> text}</li>
     * <li>{@code map<keyDataType, valueDataType> -> text}</li>
     * <li>{@code struct<field1:dataType,...,fieldN:dataType> -> text}</li>
     * <li>{@code uniontype<...> -> text}</li>
     * </ul>
     *
     * @param hiveColumn hive column schema
     * @return field with mapped GPDB type and modifiers
     * @throws UnsupportedTypeException if the column type is not supported
     * @see EnumHiveToGpdbType
     */
    public static Metadata.Field mapHiveType(FieldSchema hiveColumn) throws UnsupportedTypeException {
        String fieldName = hiveColumn.getName();
        String hiveType = hiveColumn.getType(); // Type name and modifiers if any
        String hiveTypeName; // Type name
        String[] modifiers = null; // Modifiers
        EnumHiveToGpdbType hiveToGpdbType = EnumHiveToGpdbType.getHiveToGpdbType(hiveType);
        EnumGpdbType gpdbType = hiveToGpdbType.getGpdbType();

        if (hiveToGpdbType.getSplitExpression() != null) {
            String[] tokens = hiveType.split(hiveToGpdbType.getSplitExpression());
            hiveTypeName = tokens[0];
            if (gpdbType.getModifiersNum() > 0) {
                modifiers = Arrays.copyOfRange(tokens, 1, tokens.length);
                if (modifiers.length != gpdbType.getModifiersNum()) {
                    throw new UnsupportedTypeException(
                            "GPDB does not support type " + hiveType
                                    + " (Field " + fieldName + "), "
                                    + "expected number of modifiers: "
                                    + gpdbType.getModifiersNum()
                                    + ", actual number of modifiers: "
                                    + modifiers.length);
                }
                if (!verifyIntegerModifiers(modifiers)) {
                    throw new UnsupportedTypeException("GPDB does not support type " + hiveType + " (Field " + fieldName + "), modifiers should be integers");
                }
            }
        } else
            hiveTypeName = hiveType;

        return new Metadata.Field(fieldName, gpdbType, hiveToGpdbType.isComplexType(), hiveTypeName, modifiers);
    }

    /**
     * Verifies modifiers are null or integers.
     * Modifier is a value assigned to a type,
     * e.g. size of a varchar - varchar(size).
     *
     * @param modifiers type modifiers to be verified
     * @return whether modifiers are null or integers
     */
    private static boolean verifyIntegerModifiers(String[] modifiers) {
        if (modifiers == null) {
            return true;
        }
        for (String modifier : modifiers) {
            if (StringUtils.isBlank(modifier) || !StringUtils.isNumeric(modifier)) {
                return false;
            }
        }
        return true;
    }


    /**
     * Converts GPDB type to hive type.
     *
     * @param type      GPDB data type
     * @param modifiers Integer array of modifier info
     * @return Hive type
     * @throws UnsupportedTypeException if type is not supported
     * @see EnumHiveToGpdbType For supported mappings
     */
    public static String toCompatibleHiveType(DataType type, Integer[] modifiers) {

        EnumHiveToGpdbType hiveToGpdbType = EnumHiveToGpdbType.getCompatibleHiveToGpdbType(type);
        return EnumHiveToGpdbType.getFullHiveTypeName(hiveToGpdbType, modifiers);
    }


    /**
     * Validates whether given GPDB and Hive data types are compatible.
     * If data type could have modifiers, GPDB data type is valid if it hasn't modifiers at all
     * or GPDB's modifiers are greater or equal to Hive's modifiers.
     * <p>
     * For example:
     * <p>
     * Hive type - varchar(20), GPDB type varchar - valid.
     * <p>
     * Hive type - varchar(20), GPDB type varchar(20) - valid.
     * <p>
     * Hive type - varchar(20), GPDB type varchar(25) - valid.
     * <p>
     * Hive type - varchar(20), GPDB type varchar(15) - invalid.
     *
     * @param gpdbDataType   GPDB data type
     * @param gpdbTypeMods   GPDB type modifiers
     * @param hiveType       full Hive type, i.e. decimal(10,2)
     * @param gpdbColumnName Hive column name
     * @throws UnsupportedTypeException if types are incompatible
     */
    public static void validateTypeCompatible(DataType gpdbDataType, Integer[] gpdbTypeMods, String hiveType, String gpdbColumnName) {

        EnumHiveToGpdbType hiveToGpdbType = EnumHiveToGpdbType.getHiveToGpdbType(hiveType);
        EnumGpdbType expectedGpdbType = hiveToGpdbType.getGpdbType();

        if (!expectedGpdbType.getDataType().equals(gpdbDataType)) {
            throw new UnsupportedTypeException("Invalid definition for column " + gpdbColumnName
                    + ": expected GPDB type " + expectedGpdbType.getDataType() +
                    ", actual GPDB type " + gpdbDataType);
        }

        switch (gpdbDataType) {
            case NUMERIC:
            case VARCHAR:
            case BPCHAR:
                if (gpdbTypeMods != null && gpdbTypeMods.length > 0) {
                    Integer[] hiveTypeModifiers = EnumHiveToGpdbType
                            .extractModifiers(hiveType);
                    for (int i = 0; i < hiveTypeModifiers.length; i++) {
                        if (gpdbTypeMods[i] < hiveTypeModifiers[i])
                            throw new UnsupportedTypeException(
                                    "Invalid definition for column " + gpdbColumnName
                                            + ": modifiers are not compatible, "
                                            + Arrays.toString(hiveTypeModifiers) + ", "
                                            + Arrays.toString(gpdbTypeMods));
                    }
                }
                break;
        }
    }

    /**
     * The method parses raw user data into HiveUserData class
     *
     * @param context input data
     * @return instance of HiveUserData class
     * @throws IllegalArgumentException when incorrect number of tokens in Hive user data received
     */
    public static HiveUserData parseHiveUserData(RequestContext context) throws IllegalArgumentException {
        String userData = new String(context.getFragmentUserData());
        String[] toks = userData.split(HiveUserData.HIVE_UD_DELIM, HiveUserData.getNumOfTokens());

        if (toks.length != (HiveUserData.getNumOfTokens())) {
            throw new IllegalArgumentException("HiveInputFormatFragmenter expected "
                    + HiveUserData.getNumOfTokens() + " tokens, but got " + toks.length);
        }

        HiveUserData hiveUserData = new HiveUserData(toks[0], toks[1], toks[2], toks[3], Boolean.valueOf(toks[4]), toks[5], toks[6], Integer.parseInt(toks[7]));

        return hiveUserData;
    }

    /**
     * Creates an instance of a given serde type
     *
     * @param serdeClassName the name of the serde class
     * @return instance of a given serde
     * @throws Exception if an error occurs during the creation of SerDe instance
     */
    public static Deserializer createDeserializer(String serdeClassName) throws Exception {
        Deserializer deserializer = (Deserializer) Utilities.createAnyInstance(serdeClassName);
        return deserializer;
    }

    /**
     * Creates ORC file reader.
     *
     * @param requestContext input data with given data source
     * @return ORC file reader
     */
    public static Reader getOrcReader(Configuration configuration, RequestContext requestContext) {
        try {
            Path path = new Path(requestContext.getDataSource());
            return OrcFile.createReader(path.getFileSystem(configuration), path);
        } catch (Exception e) {
            throw new RuntimeException("Exception while getting orc reader", e);
        }
    }
}
