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

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StatsAccessor;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.EnumAggregationType;
import org.greenplum.pxf.api.utilities.SerializationService;
import org.greenplum.pxf.api.utilities.SpringContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;

import java.util.EnumSet;

/**
 * Specialization of HiveAccessor for a Hive table that stores only ORC files.
 * This class replaces the generic HiveAccessor for a case where a table is stored entirely as ORC files.
 * Use together with {@link HiveInputFormatFragmenter}/{@link HiveColumnarSerdeResolver}
 */
public class HiveORCAccessor extends HiveAccessor implements StatsAccessor {

    Reader orcReader;

    private boolean useStats;
    private long count;
    private long objectsEmitted;
    private OneRow rowToEmitCount;

    private boolean statsInitialized;

    /**
     * Constructs a HiveORCFileAccessor.
     */
    public HiveORCAccessor() {
        this(SpringContext.getBean(HiveUtilities.class),
                SpringContext.getBean(SerializationService.class));
    }

    public HiveORCAccessor(HiveUtilities hiveUtilities, SerializationService serializationService) {
        super(new OrcInputFormat(), hiveUtilities, serializationService);
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        useStats = Utilities.aggregateOptimizationsSupported(context);
    }

    @Override
    public boolean openForRead() throws Exception {
        if (useStats) {
            orcReader = hiveUtilities.getOrcReader(context);
            if (orcReader == null) {
                return false;
            }
            objectsEmitted = 0;
        }
        return super.openForRead();
    }

    @Override
    protected boolean shouldAddProjectionsAndFilters() {
        return !useStats;
    }

    /**
     * Fetches file-level statistics from an ORC file.
     */
    @Override
    public void retrieveStats() throws Exception {
        if (!this.useStats) {
            throw new IllegalStateException("Accessor is not using statistics in current context.");
        }
        /*
         * We are using file-level stats therefore if file has multiple splits,
         * it's enough to return count for a first split in file.
         * In case file has multiple splits - we don't want to duplicate counts.
         */
        if (context.getFragmentIndex() == 0) {
            this.count = this.orcReader.getNumberOfRows();
            rowToEmitCount = readNextObject();
        }
        statsInitialized = true;
    }

    /**
     * Emits tuple without reading from disk, currently supports COUNT
     */
    @Override
    public OneRow emitAggObject() {
        if (!statsInitialized) {
            throw new IllegalStateException("retrieveStats() should be called before calling emitAggObject()");
        }
        OneRow row = null;
        if (context.getAggType() == null)
            throw new UnsupportedOperationException("Aggregate operation is required");
        if (context.getAggType() != EnumAggregationType.COUNT)
            throw new UnsupportedOperationException("Aggregation operation is not supported.");

        if (objectsEmitted < count) {
            objectsEmitted++;
            row = rowToEmitCount;
        }
        return row;
    }

    @Override
    protected EnumSet<Operator> getSupportedOperatorsForPushdown() {
        return ORC_SUPPORTED_OPERATORS;
    }

    @Override
    protected EnumSet<DataType> getSupportedDatatypesForPushdown() {
        return ORC_SUPPORTED_DATATYPES;
    }

    /**
     * @return ORC file reader
     */
    protected Reader getOrcReader() {
        return hiveUtilities.getOrcReader(context);
    }

}
