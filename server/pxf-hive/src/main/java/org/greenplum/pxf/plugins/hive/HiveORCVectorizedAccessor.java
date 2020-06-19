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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.io.IOException;

/**
 * Accessor class which reads data in batches.
 * One batch is 1024 rows of all projected columns
 */
@Component("HiveORCVectorizedAccessor")
@RequestScope
public class HiveORCVectorizedAccessor extends HiveORCAccessor {

    private RecordReader vrr;
    private int batchIndex;
    private VectorizedRowBatch batch;

    @Override
    public boolean openForRead() throws Exception {
        Reader.Options options = new Reader.Options();
        addColumns(options);
        addFragments(options);
        orcReader = hiveUtilities.getOrcReader(context);
        vrr = orcReader.rowsOptions(options);
        batch = orcReader.getSchema().createRowBatch();
        return vrr.hasNext();
    }

    /**
     * File might have multiple splits, so this method restricts
     * reader to one split.
     *
     * @param options reader options to modify
     */
    private void addFragments(Reader.Options options) {
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);
        options.range(fileSplit.getStart(), fileSplit.getLength());
    }

    /**
     * Reads next batch for current fragment.
     *
     * @return next batch in OneRow format, key is a batch number, data is a batch
     */
    @Override
    public OneRow readNextObject() throws IOException {
        if (vrr.hasNext()) {
            vrr.nextBatch(batch);
            batchIndex++;
            return new OneRow(new LongWritable(batchIndex), batch);
        } else {
            //All batches are exhausted
            return null;
        }
    }

    /**
     * This method updated reader options to include projected columns only.
     *
     * @param options reader options to modify
     */
    private void addColumns(Reader.Options options) {
        boolean[] includeColumns = new boolean[context.getColumns() + 1];
        for (ColumnDescriptor col : context.getTupleDescription()) {
            if (col.isProjected()) {
                includeColumns[col.columnIndex() + 1] = true;
            }
        }
        options.include(includeColumns);
    }

    @Override
    public void closeForRead() throws Exception {
        if (vrr != null) {
            vrr.close();
        }
    }
}
