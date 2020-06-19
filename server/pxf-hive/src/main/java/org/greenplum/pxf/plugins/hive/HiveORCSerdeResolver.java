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
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.Properties;

/**
 * Specialized HiveResolver for a Hive table stored as RC file.
 * Use together with HiveInputFormatFragmenter/HiveRCFileAccessor.
 */
@Component("HiveORCSerdeResolver")
@RequestScope
public class HiveORCSerdeResolver extends HiveResolver {
    private static final Log LOG = LogFactory.getLog(HiveORCSerdeResolver.class);
    private String serdeType;
    private String typesString;

    /* read the data supplied by the fragmenter: inputformat name, serde name, partition keys */
    @Override
    void parseUserData(RequestContext input) {
        HiveFragmentMetadata metadata = context.getFragmentMetadata();
        serdeType = metadata.getSerdeClassName();
        partitionKeys = metadata.getPartitionKeys();
        typesString = metadata.getColTypes();
        collectionDelim = input.getOption("COLLECTION_DELIM") == null ? COLLECTION_DELIM
                : input.getOption("COLLECTION_DELIM");
        mapkeyDelim = input.getOption("MAPKEY_DELIM") == null ? MAPKEY_DELIM
                : input.getOption("MAPKEY_DELIM");
        hiveIndexes = metadata.getHiveIndexes();
    }

    /*
     * Get and init the deserializer for the records of this Hive data fragment.
     * Suppress Warnings added because deserializer.initialize is an abstract function that is deprecated
     * but its implementations (ColumnarSerDe, LazyBinaryColumnarSerDe) still use the deprecated interface.
     */
    @SuppressWarnings("deprecation")
    @Override
    void initSerde(RequestContext input) throws Exception {
        Properties serdeProperties = new Properties();
        int numberOfDataColumns = input.getColumns() - getNumberOfPartitions();

        LOG.debug("Serde number of columns is " + numberOfDataColumns);

        StringBuilder columnNames = new StringBuilder(numberOfDataColumns * 2); // column + delimiter
        StringBuilder columnTypes = new StringBuilder(numberOfDataColumns * 2); // column + delimiter
        String[] cols = typesString.split(":");
        String[] hiveColTypes = new String[cols.length];
        parseColTypes(cols, hiveColTypes);

        String delim = ",";
        for (int j = 0; j < input.getTupleDescription().size(); j++) {
            ColumnDescriptor column = input.getColumn(j);
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
        serdeProperties.put(serdeConstants.LIST_COLUMNS, columnNames.toString());
        serdeProperties.put(serdeConstants.LIST_COLUMN_TYPES, columnTypes.toString());

        deserializer = hiveUtilities.createDeserializer(serdeType);
        deserializer.initialize(new JobConf(configuration, HiveORCSerdeResolver.class), serdeProperties);
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
