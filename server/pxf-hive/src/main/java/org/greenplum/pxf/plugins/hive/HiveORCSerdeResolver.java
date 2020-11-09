package org.greenplum.pxf.plugins.hive;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.Properties;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;

/**
 * Specialized HiveResolver for a Hive table stored as RC file.
 * Use together with HiveInputFormatFragmenter/HiveRCFileAccessor.
 */
public class HiveORCSerdeResolver extends HiveResolver {
    private static final Log LOG = LogFactory.getLog(HiveORCSerdeResolver.class);

    @Override
    protected Properties getSerdeProperties() {
        int numberOfDataColumns = context.getColumns() - getNumberOfPartitions();

        LOG.debug("Serde number of columns is " + numberOfDataColumns);

        Properties properties = super.getSerdeProperties();
        StringBuilder columnNames = new StringBuilder(numberOfDataColumns * 2); // column + delimiter
        StringBuilder columnTypes = new StringBuilder(numberOfDataColumns * 2); // column + delimiter
        String[] cols = properties.getProperty(META_TABLE_COLUMN_TYPES).split(":");
        String[] hiveColTypes = new String[cols.length];
        parseColTypes(cols, hiveColTypes);

        String delim = ",";
        for (int j = 0; j < context.getTupleDescription().size(); j++) {
            ColumnDescriptor column = context.getColumn(j);
            Integer i = hiveIndexes.get(j);
            if (i == null) continue;

            String columnName = column.columnName();
            String columnType = hiveUtilities.toCompatibleHiveType(column.getDataType(), column.columnTypeModifiers());
            //Complex Types will have a mismatch between Hive and Gpdb type
            if (!columnType.equals(hiveColTypes[i])) {
                columnType = hiveColTypes[i];
            }
            if (columnNames.length() > 0) {
                columnNames.append(delim);
                columnTypes.append(delim);
            }
            columnNames.append(columnName);
            columnTypes.append(columnType);
        }
        properties.put(serdeConstants.LIST_COLUMNS, columnNames.toString());
        properties.put(serdeConstants.LIST_COLUMN_TYPES, columnTypes.toString());
        return properties;
    }

    private void parseColTypes(String[] cols, String[] output) {
        int i = 0;
        StringBuilder structTypeBuilder = new StringBuilder();
        boolean inStruct = false;
        for (String str : cols) {
            if (str.contains("struct")) {
                structTypeBuilder = new StringBuilder();
                inStruct = true;
                structTypeBuilder.append(str);
            } else if (inStruct) {
                structTypeBuilder.append(':');
                structTypeBuilder.append(str);
                if (str.contains(">")) {
                    inStruct = false;
                    output[i++] = structTypeBuilder.toString();
                }
            } else {
                output[i++] = str;
            }
        }
    }
}
