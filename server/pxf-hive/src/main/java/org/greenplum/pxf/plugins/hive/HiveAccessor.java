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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.SupportedDataTypePruner;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.ToStringTreeVisitor;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SerializationService;
import org.greenplum.pxf.api.utilities.SpringContext;
import org.greenplum.pxf.plugins.hdfs.HdfsSplittableDataAccessor;
import org.greenplum.pxf.plugins.hdfs.filter.SearchArgumentBuilder;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.HEADER_COUNT;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR;
import static org.greenplum.pxf.plugins.hive.HiveDataFragmenter.HIVE_PARTITIONS_DELIM;
import static org.greenplum.pxf.plugins.hive.HiveDataFragmenter.PXF_META_TABLE_PARTITION_COLUMN_VALUES;

/**
 * Accessor for Hive tables. The accessor will open and read a split belonging
 * to a Hive table. Opening a split means creating the corresponding
 * InputFormat and RecordReader required to access the split's data. The actual
 * record reading is done in the base class -
 * {@link HdfsSplittableDataAccessor}. <br>
 * HiveAccessor will also enforce Hive partition filtering by filtering-out a
 * split which does not belong to a partition filter. Naturally, the partition
 * filtering will be done only for Hive tables that are partitioned.
 */
public class HiveAccessor extends HdfsSplittableDataAccessor {

    private static final Logger LOG = LoggerFactory.getLogger(HiveAccessor.class);
    private static final String PXF_PPD_HIVE = "pxf.ppd.hive";
    private static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";
    private static final String UNSUPPORTED_ERR_MESSAGE = "Hive accessor does not support write operation.";

    private List<HivePartition> partitions;
    private int skipHeaderCount;
    private final SerializationService serializationService;
    private String hiveColumnsString;
    private String hiveColumnTypesString;
    private boolean isPredicatePushdownAllowed;

    protected HiveUtilities hiveUtilities;
    protected List<Integer> hiveIndexes;

    // ----- members for predicate pushdown handling -----
    static final EnumSet<Operator> PARQUET_SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.NOOP,
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    //Operator.IS_NULL,
                    //Operator.IS_NOT_NULL,
                    Operator.IN,
                    Operator.OR,
                    Operator.AND,
                    Operator.NOT
            );

    static final EnumSet<Operator> ORC_SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.NOOP,
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

    static final EnumSet<DataType> PARQUET_SUPPORTED_DATATYPES =
            EnumSet.of(
                    DataType.BIGINT,
                    DataType.INTEGER,
                    DataType.SMALLINT,
                    DataType.REAL,
                    //DataType.NUMERIC,
                    DataType.FLOAT8,
                    DataType.TEXT,
                    DataType.VARCHAR,
                    DataType.BPCHAR,
                    DataType.BOOLEAN,
                    //DataType.DATE,
                    //DataType.TIMESTAMP,
                    DataType.TIME,
                    DataType.BYTEA
            );

    static final EnumSet<DataType> ORC_SUPPORTED_DATATYPES =
            EnumSet.of(
                    DataType.BIGINT,
                    DataType.INTEGER,
                    DataType.SMALLINT,
                    DataType.REAL,
                    DataType.NUMERIC,
                    DataType.FLOAT8,
                    DataType.TEXT,
                    DataType.VARCHAR,
                    DataType.BPCHAR,
                    DataType.BOOLEAN,
                    DataType.DATE,
                    DataType.TIMESTAMP,
                    DataType.TIME,
                    DataType.BYTEA
            );

    private static final TreeTraverser TRAVERSER = new TreeTraverser();

    /**
     * Constructs a HiveAccessor
     */
    public HiveAccessor() {
        this(null, SpringContext.getBean(HiveUtilities.class),
                SpringContext.getBean(SerializationService.class));
    }

    /**
     * Creates an instance of HiveAccessor using specified input format and hive utilities
     *
     * @param inputFormat          input format
     * @param hiveUtilities        the hive utilities
     * @param serializationService the service that provides kryo objects
     */
    HiveAccessor(InputFormat<?, ?> inputFormat, HiveUtilities hiveUtilities, SerializationService serializationService) {
        super(inputFormat);
        this.hiveUtilities = hiveUtilities;
        this.serializationService = serializationService;
    }

    /**
     * Initializes a HiveAccessor and creates an InputFormat (derived from
     * {@link org.apache.hadoop.mapred.InputFormat}) and the Hive partition
     * fields
     *
     * @throws RuntimeException if failed to create input format
     */
    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();

        // determine if predicate pushdown is allowed by configuration
        isPredicatePushdownAllowed = configuration.get(PXF_PPD_HIVE, "true").equalsIgnoreCase("true");
        if (isPredicatePushdownAllowed) {
            LOG.debug("Predicate pushdown for Hive is enabled");
            /*
             Set the property if not explicitly specified by the user to "false" to work around the issue fixed
             in HIVE-21407 but not yet available for distribution, where in ParquetRecordReaderWrapper
             code "conf = new JobConf(oldJobConf)" should be "conf = new JobConf(jobConf)" otherwise predicate pushdown
             information for Parquet file read is lost and is not used in the further processing by ParquetRecordReaderWrapper

             We assume our preference is to not skip the timestamp conversion, as we assume parquet file is properly storing
             the timestamp that was converted from local timezone to UTC. If conversion is not desired and should be skipped,
             users can set the property value to "true" in their hive-site.xml
            */
            jobConf.set(HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION.varname,
                    jobConf.get(HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION.varname, "false"));
        } else {
            LOG.debug("Predicate pushdown for Hive has been disabled in configuration");
        }

        Properties properties;
        try {
            HiveFragmentMetadata metadata = context.getFragmentMetadata();
            if(metadata == null && context.getRequestType().equals(RequestContext.RequestType.WRITE_BRIDGE))
                throw new UnsupportedOperationException();
            // clone properties from the fragment metadata as they are shared across fragments and
            // properties for the current fragment will be modified by Hive Resolvers and SerDe classes
            properties = (Properties) metadata.getProperties().clone();
            if (inputFormat == null) {
                String inputFormatClassName = properties.getProperty(FILE_INPUT_FORMAT);
                this.inputFormat = hiveUtilities.makeInputFormat(inputFormatClassName, jobConf);
            }
        } catch(UnsupportedOperationException e) {
            throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize HiveAccessor", e);
        }

        initPartitionFields(properties);
        skipHeaderCount = Integer.parseInt(properties.getProperty(HEADER_COUNT, "0"));
        hiveIndexes = buildHiveIndexes(properties);
        hiveColumnsString = properties.getProperty(META_TABLE_COLUMNS);
        hiveColumnTypesString = properties.getProperty(META_TABLE_COLUMN_TYPES);

        context.setMetadata(new HiveMetadata(properties, partitions, hiveIndexes));
    }

    /**
     * Opens Hive data split for read. Enables Hive partition filtering.
     * Adds column projection and predicate pushdown information to JobConf to be used by the reader, if supported.
     *
     * @return true if there are no partitions or there is no partition filter
     * or partition filter is set and the file currently opened by the
     * accessor belongs to the partition.
     * @throws Exception if filter could not be built, connection to Hive failed
     *                   or resource failed to open
     */
    @Override
    public boolean openForRead() throws Exception {
        // Make sure lines aren't skipped outside of the first fragment
        if (context.getFragmentIndex() != 0) {
            skipHeaderCount = 0;
        }
        if (!shouldDataBeReturnedFromFilteredPartition()) {
            return false;
        }
        // add Hive schema information for the RecordReader to return data in the Hive schema column order
        if (StringUtils.isNotBlank(hiveColumnsString)) {
            jobConf.set(IOConstants.COLUMNS, hiveColumnsString);
        }
        if (StringUtils.isNotBlank(hiveColumnTypesString)) {
            jobConf.set(IOConstants.COLUMNS_TYPES, hiveColumnTypesString);
        }
        // add column projection and predicate pushdown filters to JobConf to make them available to RecordReaders
        if (shouldAddProjectionsAndFilters()) {
            addColumns();
            addFilters();
        }
        return super.openForRead();
    }

    /**
     * Fetches one record from the file. The record is returned as a Java object.
     * We will skip skipHeaderCount # of lines within the first fragment.
     */
    @Override
    public OneRow readNextObject() throws IOException {
        while (skipHeaderCount > 0) {
            super.readNextObject();
            skipHeaderCount--;
        }
        return super.readNextObject();
    }

    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     */
    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     */
    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    /**
     * Closes the resource for write.
     */
    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    /**
     * Creates the RecordReader suitable for this given split.
     *
     * @param jobConf configuration data for the Hadoop framework
     * @param split   the split that was allocated for reading to this accessor
     * @return record reader
     * @throws IOException if failed to create record reader
     */
    @Override
    protected Object getReader(JobConf jobConf, InputSplit split)
            throws IOException {
        return inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
    }

    /**
     * Specifies whether column projection and predicate pushdown information should be added to
     * the JobConfig so that it is accessible to the RecordReader.
     *
     * @return true if CP / PPD information should be added, false if not
     */
    protected boolean shouldAddProjectionsAndFilters() {
        return true;
    }

    /*
     * The partition fields are initialized one time base on userData provided
     * by the fragmenter
     */
    void initPartitionFields(Properties properties) {
        partitions = new LinkedList<>();

        String partitionColumns = properties.getProperty(META_TABLE_PARTITION_COLUMNS);
        String partitionColumnTypes = properties.getProperty(META_TABLE_PARTITION_COLUMN_TYPES);
        String partitionColumnValue = properties.getProperty(PXF_META_TABLE_PARTITION_COLUMN_VALUES);
        if (StringUtils.isBlank(partitionColumns) || StringUtils.isBlank(partitionColumnTypes)) {
            // no partition column information
            return;
        }

        String[] partKeys = partitionColumns.trim().split("/");
        String[] partKeyTypes = partitionColumnTypes.trim().split(":");
        String[] partKeyValues = partitionColumnValue.trim().split(HIVE_PARTITIONS_DELIM);

        if (partKeys.length != partKeyTypes.length ||
                partKeys.length != partKeyValues.length) {
            throw new IllegalArgumentException(String.format("The partition keys and partition key types length does not match. partKeys.length=%d partKeyTypes.length=%d",
                    partKeys.length, partKeyTypes.length));
        }

        for (int i = 0; i < partKeys.length; i++) {
            partitions.add(new HivePartition(partKeys[i], partKeyTypes[i], partKeyValues[i]));
        }
    }

    /**
     * Builds a list of indexes corresponding to the matching columns in
     * Greenplum, ordered by the Greenplum schema order.
     *
     * @param properties the metadata properties
     * @return the hive indexes
     */
    private List<Integer> buildHiveIndexes(Properties properties) {
        List<Integer> indexes = new ArrayList<>();

        String delimiter = properties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER, ",");
        String columns = Objects.requireNonNull(properties.getProperty(META_TABLE_COLUMNS), "The \"columns\" property cannot be null");
        List<String> hiveColumns = Arrays.asList(columns.trim().split(delimiter));

        Map<String, Integer> columnNameToColsIndexMap =
                IntStream.range(0, hiveColumns.size())
                        .boxed()
                        .collect(Collectors.toMap(hiveColumns::get, i -> i));

        for (ColumnDescriptor cd : context.getTupleDescription()) {
            // The index of the column on the Hive schema
            Integer index = defaultIfNull(columnNameToColsIndexMap.get(cd.columnName()),
                    columnNameToColsIndexMap.get(cd.columnName().toLowerCase()));
            indexes.add(index);
        }
        return indexes;
    }

    private boolean shouldDataBeReturnedFromFilteredPartition() throws Exception {
        if (!context.hasFilter()) {
            return true;
        }

        String filterStr = context.getFilterString();
        Node root = new FilterParser().parse(filterStr);
        boolean returnData = isFiltered(partitions, root);

        if (LOG.isDebugEnabled()) {
            LOG.debug("{}-{}: {}--{} returnData: {}", context.getTransactionId(),
                    context.getSegmentId(), context.getDataSource(), filterStr, returnData);

            ToStringTreeVisitor toStringTreeVisitor = new ToStringTreeVisitor();
            new TreeTraverser().traverse(root, toStringTreeVisitor);

            LOG.debug("Filter string after pruning {}", toStringTreeVisitor.toString());
        }

        return returnData;
    }

    private boolean isFiltered(List<HivePartition> partitionFields, Node root) {
        return testOneFilter(partitionFields, root);
    }

    private boolean testForUnsupportedOperators(Node node) {
        boolean nonAndOp = true;
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            if (operator.isLogical()) {
                if (operator != Operator.AND) {
                    return false;
                }

                if (operatorNode.getLeft() != null) {
                    nonAndOp = testForUnsupportedOperators(operatorNode.getLeft());
                }

                if (operatorNode.getRight() != null) {
                    nonAndOp = nonAndOp && testForUnsupportedOperators(operatorNode.getRight());
                }
            }
        }
        return nonAndOp;
    }

    private boolean testForPartitionEquality(List<HivePartition> partitionFields, Node node) {
        if (!(node instanceof OperatorNode)) return true;
        boolean partitionAllowed = true;
        OperatorNode operatorNode = (OperatorNode) node;
        Operator operator = operatorNode.getOperator();

        if (operator.isLogical()) {
            if (operatorNode.getLeft() != null) {
                partitionAllowed = testForPartitionEquality(partitionFields, operatorNode.getLeft());
            }

            if (operatorNode.getRight() != null) {
                partitionAllowed = partitionAllowed && testForPartitionEquality(partitionFields, operatorNode.getRight());
            }
        } else {
            if (operator != Operator.EQUALS) {
                /*
                 * in case this is not an "equality node"
                 * we ignore it here - in partition
                 * filtering
                 */
                return true;
            }

            ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
            OperandNode valueOperandNode = operatorNode.getValueOperand();

            if (valueOperandNode == null) {
                throw new IllegalArgumentException(String.format(
                        "OperatorNode %s does not contain a scalar operand", operator));
            }

            String filterValue = valueOperandNode.toString();
            ColumnDescriptor filterColumn = context.getColumn(columnIndexOperand.index());
            String filterColumnName = filterColumn.columnName();

            for (HivePartition partition : partitionFields) {
                if (filterColumnName.equals(partition.getName())) {

                    /*
                     * the node field matches a partition field, but the values do
                     * not match
                     */
                    boolean keepPartition = filterValue.equals(partition.getValue());

                    /*
                     * If the string comparison fails then we should check the comparison of
                     * the two operands as typed values
                     * If the partition value equals HIVE_DEFAULT_PARTITION just skip
                     */
                    if (!keepPartition && !partition.getValue().equals(HIVE_DEFAULT_PARTITION)) {
                        keepPartition = testFilterByType(filterValue, partition);
                    }
                    return keepPartition;
                }
            }

            /*
             * node field did not match any partition field, so we ignore this
             * node and hence return true
             */
        }
        return partitionAllowed;
    }

    /*
     * Given two values in String form and their type, convert each to the same type do an equality check
     */
    private boolean testFilterByType(String filterValue, HivePartition partition) {
        boolean result;
        switch (partition.getType()) {
            case serdeConstants.BOOLEAN_TYPE_NAME:
                result = Boolean.valueOf(filterValue).equals(Boolean.valueOf(partition.getValue()));
                break;
            case serdeConstants.TINYINT_TYPE_NAME:
            case serdeConstants.SMALLINT_TYPE_NAME:
                result = (Short.parseShort(filterValue) == Short.parseShort(partition.getValue()));
                break;
            case serdeConstants.INT_TYPE_NAME:
                result = (Integer.parseInt(filterValue) == Integer.parseInt(partition.getValue()));
                break;
            case serdeConstants.BIGINT_TYPE_NAME:
                result = (Long.parseLong(filterValue) == Long.parseLong(partition.getValue()));
                break;
            case serdeConstants.FLOAT_TYPE_NAME:
                result = (Float.parseFloat(filterValue) == Float.parseFloat(partition.getValue()));
                break;
            case serdeConstants.DOUBLE_TYPE_NAME:
                result = (Double.parseDouble(filterValue) == Double.parseDouble(partition.getValue()));
                break;
            case serdeConstants.TIMESTAMP_TYPE_NAME:
                result = Timestamp.valueOf(filterValue).equals(Timestamp.valueOf(partition.getValue()));
                break;
            case serdeConstants.DATE_TYPE_NAME:
                result = Date.valueOf(filterValue).equals(Date.valueOf(partition.getValue()));
                break;
            case serdeConstants.DECIMAL_TYPE_NAME:
                result = HiveDecimal.create(filterValue).bigDecimalValue().equals(HiveDecimal.create(partition.getValue()).bigDecimalValue());
                break;
            case serdeConstants.BINARY_TYPE_NAME:
                result = Arrays.equals(filterValue.getBytes(), partition.getValue().getBytes());
                break;
            case serdeConstants.STRING_TYPE_NAME:
            case serdeConstants.VARCHAR_TYPE_NAME:
            case serdeConstants.CHAR_TYPE_NAME:
            default:
                result = false;
        }

        return result;
    }

    /*
     * We are testing one filter against all the partition fields. The filter
     * has the form "fieldA = valueA". The partitions have the form
     * partitionOne=valueOne/partitionTwo=ValueTwo/partitionThree=valueThree 1.
     * For a filter to match one of the partitions, lets say partitionA for
     * example, we need: fieldA = partitionOne and valueA = valueOne. If this
     * condition occurs, we return true. 2. If fieldA does not match any one of
     * the partition fields we also return true, it means we ignore this filter
     * because it is not on a partition field. 3. If fieldA = partitionOne and
     * valueA != valueOne, then we return false.
     */
    private boolean testOneFilter(List<HivePartition> partitionFields, Node root) {
        // Let's look first at the filter and escape if there are any OR or NOT ops
        if (!testForUnsupportedOperators(root))
            return true;

        return testForPartitionEquality(partitionFields, root);
    }

    /**
     * Adds the table tuple description to JobConf object
     * so only these columns will be returned.
     */
    protected void addColumns() {
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

    /**
     * Uses {@link SearchArgumentBuilder} to translate a filter string into a
     * Hive {@link SearchArgument} object. The result is added as a filter to
     * JobConf object
     */
    private void addFilters() throws Exception {
        if (!isPredicatePushdownAllowed || !context.hasFilter()) {
            return;
        }

        /* Predicate push-down configuration */
        SearchArgumentBuilder searchArgumentBuilder =
                new SearchArgumentBuilder(context.getTupleDescription(), configuration);

        // Parse the filter string into a expression tree Node
        String filterStr = context.getFilterString();
        Node root = new FilterParser().parse(filterStr);

        // Prune the parsed tree with valid supported datatypes and operators and then
        // traverse the pruned tree with the searchArgumentBuilder to produce a SearchArgument
        TRAVERSER.traverse(
                root,
                new SupportedDataTypePruner(context.getTupleDescription(), getSupportedDatatypesForPushdown()),
                new SupportedOperatorPruner(getSupportedOperatorsForPushdown()),
                searchArgumentBuilder);

        String kryoString = toKryoString(searchArgumentBuilder.getFilterBuilder().build());
        jobConf.set(ConvertAstToSearchArg.SARG_PUSHDOWN, kryoString);
        LOG.debug("Added SARG={}", kryoString);
    }

    protected EnumSet<DataType> getSupportedDatatypesForPushdown() {
        return PARQUET_SUPPORTED_DATATYPES;
    }

    protected EnumSet<Operator> getSupportedOperatorsForPushdown() {
        return PARQUET_SUPPORTED_OPERATORS;
    }

    /**
     * Package private for unit testing
     *
     * @return the jobConf
     */
    JobConf getJobConf() {
        return jobConf;
    }


    /**
     * Serializes an object into a Base64 encoded String using Kryo serialization
     *
     * @param object the object to serialize
     * @return the serialized object as a String
     */
    private String toKryoString(Object object) {
        Output out = new Output(4 * 1024, 10 * 1024 * 1024);

        Kryo kryo = serializationService.borrowKryo();
        try {
            kryo.writeObject(out, object);
        } finally {
            serializationService.releaseKryo(kryo);
        }
        out.close();
        return Base64.encodeBase64String(out.toBytes());
    }
}
