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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;

import java.util.List;

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
public class HiveInputFormatFragmenter extends HiveDataFragmenter {
    private static final Log LOG = LogFactory.getLog(HiveInputFormatFragmenter.class);

    /** Defines the Hive input formats currently supported in pxf */
    public enum PXF_HIVE_INPUT_FORMATS {
        RC_FILE_INPUT_FORMAT,
        TEXT_FILE_INPUT_FORMAT,
        ORC_FILE_INPUT_FORMAT
    }

    /**
     * Constructs a HiveInputFormatFragmenter.
     *
     * @param requestContext all input parameters coming from the client
     */
    public HiveInputFormatFragmenter(RequestContext requestContext) {
        super(requestContext, HiveInputFormatFragmenter.class);
    }

    /*
     * Checks that hive fields and partitions match the GPDB schema. Throws an
     * exception if: - the number of fields (+ partitions) do not match the GPDB
     * table definition. - the hive fields types do not match the GPDB fields.
     */
    @Override
    void verifySchema(Table tbl) throws Exception {

        int columnsSize = requestContext.getColumns();
        int hiveColumnsSize = tbl.getSd().getColsSize();
        int hivePartitionsSize = tbl.getPartitionKeysSize();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Hive table: " + hiveColumnsSize + " fields, "
                    + hivePartitionsSize + " partitions. " + "GPDB table: "
                    + columnsSize + " fields.");
        }

        // check schema size
        if (columnsSize != (hiveColumnsSize + hivePartitionsSize)) {
            throw new IllegalArgumentException("Hive table schema ("
                    + hiveColumnsSize + " fields, " + hivePartitionsSize
                    + " partitions) " + "doesn't match PXF table ("
                    + columnsSize + " fields)");
        }

        int index = 0;
        // check hive fields
        List<FieldSchema> hiveColumns = tbl.getSd().getCols();
        for (FieldSchema hiveCol : hiveColumns) {
            ColumnDescriptor colDesc = requestContext.getColumn(index++);
            DataType colType = DataType.get(colDesc.columnTypeCode());
            HiveUtilities.validateTypeCompatible(colType, colDesc.columnTypeModifiers(), hiveCol.getType(), colDesc.columnName());
        }
        // check partition fields
        List<FieldSchema> hivePartitions = tbl.getPartitionKeys();
        for (FieldSchema hivePart : hivePartitions) {
            ColumnDescriptor colDesc = requestContext.getColumn(index++);
            DataType colType = DataType.get(colDesc.columnTypeCode());
            HiveUtilities.validateTypeCompatible(colType, colDesc.columnTypeModifiers(), hivePart.getType(), colDesc.columnName());
        }

    }
}
