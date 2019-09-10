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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.BasicFilter;
import org.greenplum.pxf.api.LogicalFilter;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StatsAccessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.*;

/**
 * Specialization of HiveAccessor for a Hive table that stores only ORC files.
 * This class replaces the generic HiveAccessor for a case where a table is stored entirely as ORC files.
 * Use together with {@link HiveInputFormatFragmenter}/{@link HiveColumnarSerdeResolver}
 */
public class HiveORCAccessor extends HiveAccessor implements StatsAccessor {

    private static final Log LOG = LogFactory.getLog(HiveORCAccessor.class);
    private static final int KRYO_BUFFER_SIZE = 4 * 1024;
    private static final int KRYO_MAX_BUFFER_SIZE = 10 * 1024 * 1024;

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
     * Uses {@link HiveFilterBuilder} to translate a filter string into a
     * Hive {@link SearchArgument} object. The result is added as a filter to
     * JobConf object
     */
    private void addFilters() throws Exception {
        if (!context.hasFilter()) {
            return;
        }

        /* Predicate pushdown configuration */
        String filterStr = context.getFilterString();
        HiveFilterBuilder eval = new HiveFilterBuilder();
        Object filter = eval.getFilterObject(filterStr);
        SearchArgument.Builder filterBuilder = SearchArgumentFactory.newBuilder(configuration);

        /*
         * If there is only a single filter it will be of type Basic Filter
         * need special case logic to make sure to still wrap the filter in a
         * startAnd() & end() block
         */
        if (filter instanceof LogicalFilter) {
            if (!buildExpression(filterBuilder, Arrays.asList(filter))) {
                return;
            }
        } else {
            filterBuilder.startAnd();
            if (!buildArgument(filterBuilder, filter)) {
                return;
            }
            filterBuilder.end();
        }
        SearchArgument sarg = filterBuilder.build();
        jobConf.set(ConvertAstToSearchArg.SARG_PUSHDOWN, toKryo(sarg));
    }

    private boolean buildExpression(SearchArgument.Builder builder, List<Object> filterList) {
        for (Object f : filterList) {
            if (f instanceof LogicalFilter) {
                switch (((LogicalFilter) f).getOperator()) {
                    case HDOP_OR:
                        builder.startOr();
                        break;
                    case HDOP_AND:
                        builder.startAnd();
                        break;
                    case HDOP_NOT:
                        builder.startNot();
                        break;
                }
                if (buildExpression(builder, ((LogicalFilter) f).getFilterList())) {
                    builder.end();
                } else {
                    return false;
                }
            } else {
                if (!buildArgument(builder, f)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean buildArgument(SearchArgument.Builder builder, Object filterObj) {
        /* The below functions will not be compatible and requires update  with Hive 2.0 APIs */
        BasicFilter filter = (BasicFilter) filterObj;
        int filterColumnIndex = filter.getColumn().index();
        // filter value might be null for unary operations
        Object filterValue = filter.getConstant() == null ? null : filter.getConstant().constant();
        ColumnDescriptor filterColumn = context.getColumn(filterColumnIndex);
        String filterColumnName = filterColumn.columnName();

        // In Hive 1, boxing of values happened inside the builder
        // For Hive 2 libraries, we need to do it before passing values to
        // Hive jars
        if (filterValue instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> l = (List<Object>) filterValue;
            filterValue = l.stream().map(HiveORCAccessor::boxLiteral).collect(Collectors.toList());
        } else if (filterValue != null) {
            filterValue = boxLiteral(filterValue);
        }

        PredicateLeaf.Type predicateLeafType = PredicateLeaf.Type.STRING;

        if (filterValue != null) {
            predicateLeafType = getType(filterValue);
        }

        switch (filter.getOperation()) {
            case HDOP_LT:
                builder.lessThan(filterColumnName, predicateLeafType, filterValue);
                break;
            case HDOP_GT:
                builder.startNot().lessThanEquals(filterColumnName, predicateLeafType, filterValue).end();
                break;
            case HDOP_LE:
                builder.lessThanEquals(filterColumnName, predicateLeafType, filterValue);
                break;
            case HDOP_GE:
                builder.startNot().lessThan(filterColumnName, predicateLeafType, filterValue).end();
                break;
            case HDOP_EQ:
                builder.equals(filterColumnName, predicateLeafType, filterValue);
                break;
            case HDOP_NE:
                builder.startNot().equals(filterColumnName, predicateLeafType, filterValue).end();
                break;
            case HDOP_IS_NULL:
                builder.isNull(filterColumnName, predicateLeafType);
                break;
            case HDOP_IS_NOT_NULL:
                builder.startNot().isNull(filterColumnName, predicateLeafType).end();
                break;
            case HDOP_IN:
                if (filterValue instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> l = (List<Object>) filterValue;
                    builder.in(filterColumnName, predicateLeafType, l.toArray());
                } else {
                    throw new IllegalArgumentException("filterValue should be instance of List for HDOP_IN operation");
                }
                break;
            default: {
                LOG.debug("Filter push-down is not supported for " + filter.getOperation() + "operation.");
                return false;
            }
        }
        return true;
    }

    /**
     * Get the type of the given expression node.
     *
     * @param literal the object
     * @return int, string, or float or null if we don't know the type
     */
    private PredicateLeaf.Type getType(Object literal) {
        if (literal instanceof Byte ||
                literal instanceof Short ||
                literal instanceof Integer ||
                literal instanceof Long) {
            return PredicateLeaf.Type.LONG;
        } else if (literal instanceof HiveChar ||
                literal instanceof HiveVarchar ||
                literal instanceof String) {
            return PredicateLeaf.Type.STRING;
        } else if (literal instanceof Float ||
                literal instanceof Double) {
            return PredicateLeaf.Type.FLOAT;
        } else if (literal instanceof Date) {
            return PredicateLeaf.Type.DATE;
        } else if (literal instanceof Timestamp) {
            return PredicateLeaf.Type.TIMESTAMP;
        } else if (literal instanceof HiveDecimal ||
                literal instanceof BigDecimal) {
            return PredicateLeaf.Type.DECIMAL;
        } else if (literal instanceof Boolean) {
            return PredicateLeaf.Type.BOOLEAN;
        } else if (literal instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> l = (List<Object>) literal;
            if (l.size() > 0)
                return getType(l.get(0));
        }
        throw new IllegalArgumentException(String.format("Unknown type for literal %s", literal));
    }

    private static Object boxLiteral(Object literal) {
        if (literal instanceof String ||
                literal instanceof Long ||
                literal instanceof Double ||
                literal instanceof Date ||
                literal instanceof Timestamp ||
                literal instanceof HiveDecimal ||
                literal instanceof BigDecimal ||
                literal instanceof Boolean) {
            return literal;
        } else if (literal instanceof HiveChar ||
                literal instanceof HiveVarchar) {
            return StringUtils.stripEnd(literal.toString(), null);
        } else if (literal instanceof Byte ||
                literal instanceof Short ||
                literal instanceof Integer) {
            return ((Number) literal).longValue();
        } else if (literal instanceof Float) {
            // to avoid change in precision when upcasting float to double
            // we convert the literal to string and parse it as double. (HIVE-8460)
            return Double.parseDouble(literal.toString());
        } else {
            throw new IllegalArgumentException("Unknown type for literal " +
                    literal);
        }
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
        switch (context.getAggType()) {
            case COUNT:
                if (objectsEmitted < count) {
                    objectsEmitted++;
                    row = rowToEmitCount;
                }
                break;
            default: {
                throw new UnsupportedOperationException("Aggregation operation is not supported.");
            }
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
