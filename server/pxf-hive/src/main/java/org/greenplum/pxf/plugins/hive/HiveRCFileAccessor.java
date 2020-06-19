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


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.*;

/**
 * Specialization of HiveAccessor for a Hive table that stores only RC files.
 * This class replaces the generic HiveAccessor for a case where a table is stored entirely as RC files.
 * Use together with {@link HiveInputFormatFragmenter}/{@link HiveColumnarSerdeResolver}
 */
@Component("HiveRCFileAccessor")
@RequestScope
public class HiveRCFileAccessor extends HiveAccessor {

    /**
     * Constructs a HiveRCFileAccessor.
     */
    public HiveRCFileAccessor() {
        super(new RCFileInputFormat());
    }

    @Override
    public boolean openForRead() throws Exception {
        addColumns();
        return super.openForRead();
    }

    /**
     * Adds the table tuple description to JobConf object
     * so only these columns will be returned.
     */
    private void addColumns() {

        List<Integer> colIds = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        for (int i = 0; i < tupleDescription.size(); i++) {
            ColumnDescriptor col = tupleDescription.get(i);
            if (col.isProjected() && hiveIndexes.get(i) != null) {
                colIds.add(hiveIndexes.get(i));
                colNames.add(col.columnName());
            }
        }
        jobConf.set(READ_ALL_COLUMNS, "false");
        jobConf.set(READ_COLUMN_IDS_CONF_STR, StringUtils.join(colIds, ","));
        jobConf.set(READ_COLUMN_NAMES_CONF_STR, StringUtils.join(colNames, ","));
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split) throws IOException {
        return new RCFileRecordReader(jobConf, (FileSplit) split);
    }
}
