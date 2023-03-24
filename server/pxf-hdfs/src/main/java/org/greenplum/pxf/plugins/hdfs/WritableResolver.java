package org.greenplum.pxf.plugins.hdfs;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.utilities.DataSchemaException;
import org.greenplum.pxf.plugins.hdfs.utilities.RecordkeyAdapter;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

/**
 * WritableResolver handles serialization and deserialization of records
 * that were serialized using Hadoop's Writable serialization framework.
 * <p>
 * A field named 'recordkey' is treated as a key of the given row, and not as
 * part of the data schema. See {@link RecordkeyAdapter}.
 */
public class WritableResolver extends BasePlugin implements Resolver {
    private static final int RECORDKEY_UNDEFINED = -1;
    private static final Log LOG = LogFactory.getLog(WritableResolver.class);
    private RecordkeyAdapter recordkeyAdapter = new RecordkeyAdapter();
    private int recordkeyIndex;
    // reflection fields
    private Object userObject;
    private Field[] fields;

    /**
     * Initialize the plugin for the incoming request
     */
    @Override
    public void afterPropertiesSet() {
        // DATA-SCHEMA option is deprecated in favor of DATA_SCHEMA to support FDW foreign table definitions
        String schemaName = StringUtils.defaultString(context.getOption("DATA_SCHEMA"), context.getOption("DATA-SCHEMA"));

        /* Testing that the schema name was supplied by the user - schema is an optional property. */
        if (schemaName == null) {
            throw new DataSchemaException(DataSchemaException.MessageFmt.SCHEMA_NOT_INDICATED, this.getClass().getName());
        }

        /* Testing that the schema resource exists. */
        if (!isSchemaOnClasspath(schemaName)) {
            throw new DataSchemaException(DataSchemaException.MessageFmt.SCHEMA_NOT_ON_CLASSPATH, schemaName);
        }

        try {
            userObject = Utilities.createAnyInstance(schemaName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create an instance of " + schemaName, e);
        }
        fields = userObject.getClass().getDeclaredFields();
        recordkeyIndex = (context.getRecordkeyColumn() == null)
                ? RECORDKEY_UNDEFINED
                : context.getRecordkeyColumn().columnIndex();

        // fields details:
        if (LOG.isDebugEnabled()) {
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                String javaType = field.getType().getName();
                boolean isPrivate = Modifier.isPrivate(field.getModifiers());

                LOG.debug("Field #" + i + ", name: " + field.getName() +
                        " type: " + javaType + ", " +
                        (isArray(javaType) ? "Array" : "Primitive") + ", " +
                        (isPrivate ? "Private" : "accessible") + " field");
            }
        }
    }

    private boolean isArray(String javaType) {
        return (javaType.startsWith("[") && !"[B".equals(javaType));
    }

    @Override
    public List<OneField> getFields(OneRow onerow) throws Exception {
        userObject = onerow.getData();
        List<OneField> record = new LinkedList<>();

        int currentIdx = 0;
        for (Field field : fields) {
            if (currentIdx == recordkeyIndex) {
                currentIdx += recordkeyAdapter.appendRecordkeyField(record, context, onerow);
            }

            if (Modifier.isPrivate(field.getModifiers())) {
                continue;
            }

            currentIdx += populateRecord(record, field);
        }

        return record;
    }

    private int setArrayField(List<OneField> record, int dataType, Field reflectedField) throws IllegalAccessException {
        Object array = reflectedField.get(userObject);
        int length = Array.getLength(array);
        for (int j = 0; j < length; j++) {
            record.add(new OneField(dataType, Array.get(array, j)));
        }
        return length;
    }

    /*
     * Given a java Object type, convert it to the corresponding output field
     * type.
     */
    private DataType convertJavaToGPDBType(String type) {
        if ("boolean".equals(type) || "[Z".equals(type)) {
            return DataType.BOOLEAN;
        }
        if ("int".equals(type) || "[I".equals(type)) {
            return DataType.INTEGER;
        }
        if ("double".equals(type) || "[D".equals(type)) {
            return DataType.FLOAT8;
        }
        if ("java.lang.String".equals(type) || "[Ljava.lang.String;".equals(type)) {
            return DataType.TEXT;
        }
        if ("float".equals(type) || "[F".equals(type)) {
            return DataType.REAL;
        }
        if ("long".equals(type) || "[J".equals(type)) {
            return DataType.BIGINT;
        }
        if ("[B".equals(type)) {
            return DataType.BYTEA;
        }
        if ("short".equals(type) || "[S".equals(type)) {
            return DataType.SMALLINT;
        }
        throw new UnsupportedTypeException("Type " + type + " is not supported by GPDBWritable");
    }

    private int populateRecord(List<OneField> record, Field field) throws BadRecordException {
        String javaType = field.getType().getName();
        try {
            DataType dataType = convertJavaToGPDBType(javaType);
            if (isArray(javaType)) {
                return setArrayField(record, dataType.getOID(), field);
            }
            record.add(new OneField(dataType.getOID(), field.get(userObject)));
            return 1;
        } catch (IllegalAccessException ex) {
            throw new BadRecordException(ex);
        }
    }

    /**
     * Sets customWritable fields and creates a OneRow object.
     */
    @Override
    public OneRow setFields(List<OneField> record) throws Exception {
        Writable key = null;

        int colIdx = 0;
        for (Field field : fields) {
            /*
             * extract recordkey based on the column descriptor type
             * and add to OneRow.key
             */
            if (colIdx == recordkeyIndex) {
                key = recordkeyAdapter.convertKeyValue(record.get(colIdx).val);
                colIdx++;
            }

            if (Modifier.isPrivate(field.getModifiers())) {
                continue;
            }

            String javaType = field.getType().getName();
            convertJavaToGPDBType(javaType);
            if (isArray(javaType)) {
                Object value = field.get(userObject);
                int length = Array.getLength(value);
                for (int j = 0; j < length; j++, colIdx++) {
                    Array.set(value, j, record.get(colIdx).val);
                }
            } else {
                field.set(userObject, record.get(colIdx).val);
                colIdx++;
            }
        }

        return new OneRow(key, userObject);
    }

    /*
     * Tests for the case schema resource is a file like avro_schema.avsc
     * or for the case schema resource is a Java class. in which case we try to reflect the class name.
     */
    private boolean isSchemaOnClasspath(String resource) {
        if (this.getClass().getClassLoader().getResource("/" + resource) != null) {
            return true;
        }

        try {
            Class.forName(resource);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
