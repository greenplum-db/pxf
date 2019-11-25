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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StatsAccessor;
import org.greenplum.pxf.api.filter.BaseTreePruner;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.TreePruner;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.EnumAggregationType;
import org.greenplum.pxf.api.utilities.Utilities;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR;

/**
 * Specialization of HiveAccessor for a Hive table that stores only ORC files.
 * This class replaces the generic HiveAccessor for a case where a table is stored entirely as ORC files.
 * Use together with {@link HiveInputFormatFragmenter}/{@link HiveColumnarSerdeResolver}
 */
public class HiveORCAccessor extends HiveAccessor implements StatsAccessor {

    private static final int KRYO_BUFFER_SIZE = 4 * 1024;
    private static final int KRYO_MAX_BUFFER_SIZE = 10 * 1024 * 1024;

    private static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    Operator.IS_NULL,
                    Operator.IS_NOT_NULL,
                    Operator.IN,
                    Operator.OR,
                    Operator.AND,
                    Operator.NOT
            );
    private static final TreePruner TREE_PRUNER = new BaseTreePruner(SUPPORTED_OPERATORS);

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
        super(new OrcInputFormat());
    }

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        useStats = Utilities.aggregateOptimizationsSupported(context);
    }

    @Override
    public boolean openForRead() throws Exception {
        if (useStats) {
            orcReader = getOrcReader();
            if (orcReader == null) {
                return false;
            }
            objectsEmitted = 0;
        } else {
            addColumns();
            addFilters();
        }
        return super.openForRead();
    }

    /**
     * Adds the table tuple description to JobConf ojbect
     * so only these columns will be returned.
     */
    private void addColumns() {

        List<Integer> colIds = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        for (ColumnDescriptor col : context.getTupleDescription()) {
            if (col.isProjected()) {
                colIds.add(col.columnIndex());
                colNames.add(col.columnName());
            }
        }
        jobConf.set(READ_ALL_COLUMNS, "false");
        jobConf.set(READ_COLUMN_IDS_CONF_STR, StringUtils.join(colIds, ","));
        jobConf.set(READ_COLUMN_NAMES_CONF_STR, StringUtils.join(colNames, ","));
    }

    /**
     * Uses {@link HiveTreeVisitor} to translate a filter string into a
     * Hive {@link SearchArgument} object. The result is added as a filter to
     * JobConf object
     */
    private void addFilters() throws Exception {
        if (!context.hasFilter()) {
            return;
        }

        /* Predicate push-down configuration */
        String filterStr = context.getFilterString();

        Node root = new FilterParser().parse(filterStr.getBytes());
        root = TREE_PRUNER.prune(root);

        HiveORCTreeVisitor treeVisitor = new HiveORCTreeVisitor(context.getTupleDescription(), configuration);
        new TreeTraverser().inOrderTraversal(root, treeVisitor);

        SearchArgument.Builder filterBuilder = treeVisitor.getFilterBuilder();
        SearchArgument searchArgument = filterBuilder.build();
        jobConf.set(ConvertAstToSearchArg.SARG_PUSHDOWN, toKryo(searchArgument));
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
        if (context.getAggType() == EnumAggregationType.COUNT) {
            if (objectsEmitted < count) {
                objectsEmitted++;
                row = rowToEmitCount;
            }
        } else {
            throw new UnsupportedOperationException("Aggregation operation is not supported.");
        }
        return row;
    }

    /**
     * Package private for unit testing
     *
     * @return the jobConf
     */
    JobConf getJobConf() {
        return jobConf;
    }

    private String toKryo(SearchArgument sarg) {
        Output out = new Output(KRYO_BUFFER_SIZE, KRYO_MAX_BUFFER_SIZE);
        new Kryo().writeObject(out, sarg);
        out.close();
        return Base64.encodeBase64String(out.toBytes());
    }

}
