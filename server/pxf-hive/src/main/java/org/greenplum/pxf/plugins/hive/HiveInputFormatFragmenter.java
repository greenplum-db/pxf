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


import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

/**
 * Specialized Hive fragmenter for RC and Text files tables. Unlike the
 * {@link HiveDataFragmenter}, this class does not send the serde properties to
 * the accessor/resolvers. This is done to avoid memory explosion in Gpdb. For
 * RC use together with {@link HiveRCFileAccessor}/
 * {@link HiveColumnarSerdeResolver}. For Text use together with
 * {@link HiveLineBreakAccessor}/{@link HiveStringPassResolver}. <br>
 * Given a Hive table and its partitions, divide the data into fragments (here a
 * data fragment is actually a HDFS file block) and return a list of them. Each
 * data fragment will contain the following information:
 * <ol>
 * <li>sourceName: full HDFS path to the data file that this data fragment is
 * part of</li>
 * <li>hosts: a list of the datanode machines that hold a replica of this block</li>
 * <li>userData: inputformat name, serde names and partition keys</li>
 * </ol>
 */
@Component("HiveInputFormatFragmenter")
@RequestScope
public class HiveInputFormatFragmenter extends HiveDataFragmenter {
    private static final Logger LOG = LoggerFactory.getLogger(HiveInputFormatFragmenter.class);

    /**
     * Defines the Hive input formats currently supported in pxf
     */
    public enum PXF_HIVE_INPUT_FORMATS {
        RC_FILE_INPUT_FORMAT,
        TEXT_FILE_INPUT_FORMAT,
        ORC_FILE_INPUT_FORMAT
    }

    /**
     * Checks that hive fields and partitions match the Greenplum schema.
     * Throws an exception if:
     * - A Greenplum column does not match any columns or partitions on the
     * Hive table definition
     * - The hive fields types do not match the Greenplum fields.
     * Then return a list of indexes corresponding to the matching columns in
     * Greenplum, ordered by the Greenplum schema order. It excludes any
     * partition column
     *
     * @param tbl the hive table
     * @return a list of indexes
     */
    @Override
    List<Integer> verifySchema(Table tbl) {

        List<Integer> indexes = new ArrayList<>();
        List<FieldSchema> hiveColumns = tbl.getSd().getCols();
        List<FieldSchema> hivePartitions = tbl.getPartitionKeys();

        Map<String, FieldSchema> columnNameToFieldSchema =
                Stream.concat(hiveColumns.stream(), hivePartitions.stream())
                        .collect(Collectors.toMap(FieldSchema::getName, fieldSchema -> fieldSchema));

        Map<String, Integer> columnNameToColsIndexMap =
                IntStream.range(0, hiveColumns.size())
                        .boxed()
                        .collect(Collectors.toMap(i -> hiveColumns.get(i).getName(), i -> i));

        FieldSchema fieldSchema;
        for (ColumnDescriptor cd : context.getTupleDescription()) {
            if ((fieldSchema = columnNameToFieldSchema.get(cd.columnName())) == null &&
                    (fieldSchema = columnNameToFieldSchema.get(cd.columnName().toLowerCase())) == null) {
                throw new PxfRuntimeException(
                        String.format("column '%s' does not exist in the Hive schema or Hive Partition",
                                cd.columnName()),
                        "Ensure the column or partition exists and check the name spelling and case."
                );
            }

            hiveUtilities.validateTypeCompatible(
                    cd.getDataType(),
                    cd.columnTypeModifiers(),
                    fieldSchema.getType(),
                    cd.columnName());

            // The index of the column on the Hive schema
            Integer index =
                    defaultIfNull(columnNameToColsIndexMap.get(cd.columnName()),
                            columnNameToColsIndexMap.get(cd.columnName().toLowerCase()));
            indexes.add(index);
        }
        return indexes;
    }
}
