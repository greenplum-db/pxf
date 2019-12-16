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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.BaseConfigurationFactory;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.greenplum.pxf.plugins.hive.utilities.ProfileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Fragmenter class for HIVE tables. <br>
 * Given a Hive table and its partitions divide the data into fragments (here a
 * data fragment is actually a HDFS file block) and return a list of them. Each
 * data fragment will contain the following information:
 * <ol>
 * <li>sourceName: full HDFS path to the data file that this data fragment is
 * part of</li>
 * <li>hosts: a list of the datanode machines that hold a replica of this block</li>
 * <li>userData:
 * file_input_format_name_DELIM_serde_name_DELIM_serialization_properties</li>
 * </ol>
 */
public class HiveDataFragmenter extends HdfsDataFragmenter {
    private static final Logger LOG = LoggerFactory.getLogger(HiveDataFragmenter.class);
    private static final short ALL_PARTS = -1;

    public static final String HIVE_1_PART_DELIM = "!H1PD!";
    public static final String HIVE_PARTITIONS_DELIM = "!HPAD!";
    public static final String HIVE_NO_PART_TBL = "!HNPT!";

    static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.EQUALS,
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.NOT_EQUALS,
                    Operator.AND,
                    Operator.OR
            );

    private IMetaStoreClient client;
    private HiveClientWrapper hiveClientWrapper;

    private boolean filterInFragmenter = false;

    // Data structure to hold hive partition names if exist, to be used by
    // partition filtering
    private Set<String> setPartitions = new TreeSet<>(
            String.CASE_INSENSITIVE_ORDER);
    private Map<String, String> partitionKeyTypes = new HashMap<>();

    public HiveDataFragmenter() {
        this(BaseConfigurationFactory.getInstance(), HiveClientWrapper.getInstance());
    }

    HiveDataFragmenter(ConfigurationFactory configurationFactory, HiveClientWrapper hiveClientWrapper) {
        this.configurationFactory = configurationFactory;
        this.hiveClientWrapper = hiveClientWrapper;
    }

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
        client = hiveClientWrapper.initHiveClient(this.context, configuration);
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        Metadata.Item tblDesc = hiveClientWrapper.extractTableFromName(context.getDataSource());

        fetchTableMetaData(tblDesc);

        return fragments;
    }

    /**
     * Creates the partition InputFormat.
     *
     * @param inputFormatName input format class name
     * @param jobConf         configuration data for the Hadoop framework
     * @return a {@link org.apache.hadoop.mapred.InputFormat} derived object
     * @throws Exception if failed to create input format
     */
    public static InputFormat<?, ?> makeInputFormat(String inputFormatName,
                                                    JobConf jobConf)
            throws Exception {
        Class<?> c = Class.forName(inputFormatName, true,
                JavaUtils.getClassLoader());
        InputFormat<?, ?> inputFormat = (InputFormat<?, ?>) c.newInstance();

        if ("org.apache.hadoop.mapred.TextInputFormat".equals(inputFormatName)) {
            // TextInputFormat needs a special configuration
            ((TextInputFormat) inputFormat).configure(jobConf);
        }

        return inputFormat;
    }

    /*
     * Goes over the table partitions metadata and extracts the splits and the
     * InputFormat and Serde per split.
     */
    private void fetchTableMetaData(Metadata.Item tblDesc) throws Exception {

        Table tbl = hiveClientWrapper.getHiveTable(client, tblDesc);

        Metadata metadata = new Metadata(tblDesc);
        hiveClientWrapper.getSchema(tbl, metadata);
        boolean hasComplexTypes = hiveClientWrapper.hasComplexTypes(metadata);

        verifySchema(tbl);

        List<Partition> partitions;
        String filterStringForHive = "";

        // If query has filter and hive table has partitions, prepare the filter
        // string for hive metastore and retrieve only the matched partitions
        if (context.hasFilter() && tbl.getPartitionKeysSize() > 0) {

            // Save all hive partition names in a set for later filter match
            for (FieldSchema fs : tbl.getPartitionKeys()) {
                setPartitions.add(fs.getName());
                partitionKeyTypes.put(fs.getName(), fs.getType());
            }

            LOG.debug("setPartitions: {}", setPartitions);

            // canPushDownIntegral represents hive.metastore.integral.jdo.pushdown property in hive-site.xml
            boolean canPushDownIntegral = configuration
                    .getBoolean(HiveConf.ConfVars.METASTORE_INTEGER_JDO_PUSHDOWN.varname, false);

            List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();

            HiveTreeVisitor hiveTreeVisitor = new HiveTreeVisitor(columnDescriptors);
            TreeVisitor hivePartitionPruner = new HivePartitionPruner(SUPPORTED_OPERATORS,
                    canPushDownIntegral, partitionKeyTypes, columnDescriptors);

            Node root = new FilterParser().parse(context.getFilterString().getBytes());
            root = hivePartitionPruner.visit(root);
            new TreeTraverser().inOrderTraversal(root, hiveTreeVisitor);

            // Generate filter string for retrieve match pxf filter/hive partition name
            filterStringForHive = hiveTreeVisitor.toString();
        }

        if (StringUtils.isNotBlank(filterStringForHive)) {

            LOG.debug("Filter String for Hive partition retrieval : {}",
                    filterStringForHive);

            filterInFragmenter = true;

            // API call to Hive MetaStore, will return a List of all the
            // partitions for this table, that matches the partition filters
            // Defined in filterStringForHive.
            partitions = client.listPartitionsByFilter(tblDesc.getPath(),
                    tblDesc.getName(), filterStringForHive, ALL_PARTS);

            // No matched partitions for the filter, no fragments to return.
            if (partitions == null || partitions.isEmpty()) {

                LOG.debug("Table - {}.{} has no matched partitions for the filter : {}",
                        tblDesc.getPath(), tblDesc.getName(), filterStringForHive);
                return;
            }

            LOG.debug("Table - {}.{} matched partitions list size: {}",
                    tblDesc.getPath(), tblDesc.getName(), partitions.size());

        } else {
            // API call to Hive MetaStore, will return a List of all the
            // partitions for this table (no filtering)
            partitions = client.listPartitions(tblDesc.getPath(),
                    tblDesc.getName(), ALL_PARTS);
        }

        StorageDescriptor descTable = tbl.getSd();
        Properties props;

        if (partitions.isEmpty()) {
            props = getSchema(tbl);
            fetchMetaDataForSimpleTable(descTable, props, hasComplexTypes);
        } else {
            List<FieldSchema> partitionKeys = tbl.getPartitionKeys();

            for (Partition partition : partitions) {
                StorageDescriptor descPartition = partition.getSd();
                props = MetaStoreUtils.getSchema(descPartition, descTable,
                        null,
                        tblDesc.getPath(), tblDesc.getName(),
                        partitionKeys);
                fetchMetaDataForPartitionedTable(descPartition, props,
                        partition, partitionKeys, tblDesc.getName(), hasComplexTypes);
            }
        }
    }

    void verifySchema(Table tbl) throws Exception {
        /* nothing to verify here */
    }

    private static Properties getSchema(Table table) {
        return MetaStoreUtils.getSchema(table.getSd(), table.getSd(),
                table.getParameters(), table.getDbName(), table.getTableName(),
                table.getPartitionKeys());
    }

    private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc,
                                             Properties props, boolean hasComplexTypes) throws Exception {
        fetchMetaDataForSimpleTable(stdsc, props, null, hasComplexTypes);
    }

    private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc,
                                             Properties props, String tableName, boolean hasComplexTypes)
            throws Exception {
        fetchMetaData(new HiveTablePartition(stdsc, props, null, null,
                tableName), hasComplexTypes);
    }

    private void fetchMetaDataForPartitionedTable(StorageDescriptor stdsc,
                                                  Properties props,
                                                  Partition partition,
                                                  List<FieldSchema> partitionKeys,
                                                  String tableName,
                                                  boolean hasComplexTypes)
            throws Exception {
        fetchMetaData(new HiveTablePartition(stdsc, props, partition,
                partitionKeys, tableName), hasComplexTypes);
    }

    /* Fills a table partition */
    private void fetchMetaData(HiveTablePartition tablePartition, boolean hasComplexTypes)
            throws Exception {
        InputFormat<?, ?> fformat = makeInputFormat(
                tablePartition.storageDesc.getInputFormat(), jobConf);
        String profile = null;
        String userProfile = context.getProfile();
        if (userProfile != null) {
            // evaluate optimal profile based on file format if profile was explicitly specified in url
            // if user passed accessor+fragmenter+resolver - use them
            profile = ProfileFactory.get(fformat, hasComplexTypes, userProfile);
        }
        String fragmenterForProfile;
        if (profile != null) {
            fragmenterForProfile = context.getPluginConf().getPlugins(profile).get("FRAGMENTER");
        } else {
            fragmenterForProfile = context.getFragmenter();
        }

        FileInputFormat.setInputPaths(jobConf, new Path(
                tablePartition.storageDesc.getLocation()));

        InputSplit[] splits;
        try {
            splits = fformat.getSplits(jobConf, 1);
        } catch (org.apache.hadoop.mapred.InvalidInputException e) {
            LOG.debug("getSplits failed on " + e.getMessage());
            return;
        }

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String[] hosts = fsp.getLocations();
            String filepath = fsp.getPath().toString();

            byte[] locationInfo = HdfsUtilities.prepareFragmentMetadata(fsp);
            Fragment fragment = new Fragment(filepath, hosts, locationInfo,
                    hiveClientWrapper.makeUserData(fragmenterForProfile, tablePartition, filterInFragmenter), profile);
            fragments.add(fragment);
        }
    }

    /**
     * Returns statistics for Hive table. Currently it's not implemented.
     */
    @Override
    public FragmentStats getFragmentStats() throws Exception {
        Metadata.Item tblDesc = hiveClientWrapper.extractTableFromName(context.getDataSource());
        Table tbl = hiveClientWrapper.getHiveTable(client, tblDesc);
        Metadata metadata = new Metadata(tblDesc);
        hiveClientWrapper.getSchema(tbl, metadata);

        long split_count = Long.parseLong(tbl.getParameters().get("numFiles"));
        long totalSize = Long.parseLong(tbl.getParameters().get("totalSize"));
        long firstFragmentSize = totalSize / split_count;
        return new FragmentStats(split_count, firstFragmentSize, totalSize);
    }

}
