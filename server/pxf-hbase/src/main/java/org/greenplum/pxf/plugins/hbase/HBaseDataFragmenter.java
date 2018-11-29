package org.greenplum.pxf.plugins.hbase;

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


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseLookupTable;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseUtilities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

/**
 * Fragmenter class for HBase data resources.
 *
 * Extends the {@link BaseFragmenter} abstract class, with the purpose of transforming
 * an input data path (an HBase table name in this case) into a list of regions
 * that belong to this table.
 *
 * This class also puts HBase lookup table information for the given
 * table (if exists) in each fragment's user data field.
 */
public class HBaseDataFragmenter extends BaseFragmenter {

    private Connection connection;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        configuration = HBaseConfiguration.create(configuration);
        configuration.set("hbase.client.retries.number", "3");
    }

    /**
     * Returns statistics for HBase table. Currently it's not implemented.
     */
    @Override
    public FragmentStats getFragmentStats() throws Exception {
        throw new UnsupportedOperationException("ANALYZE for HBase plugin is not supported");
    }

    /**
     * Returns list of fragments containing all of the
     * HBase's table data.
     * Lookup table information with mapping between
     * field names in GPDB table and HBase table will be
     * returned as user data.
     *
     * @return a list of fragments
     */
    @Override
    public List<Fragment> getFragments() throws Exception {

        // check that Zookeeper and HBase master are available
        HBaseAdmin.checkHBaseAvailable(configuration);
        connection = ConnectionFactory.createConnection(configuration);
        Admin hbaseAdmin = connection.getAdmin();
        if (!HBaseUtilities.isTableAvailable(hbaseAdmin, context.getDataSource())) {
            HBaseUtilities.closeConnection(hbaseAdmin, connection);
            throw new TableNotFoundException(context.getDataSource());
        }

        byte[] userData = prepareUserData();
        addTableFragments(userData);

        HBaseUtilities.closeConnection(hbaseAdmin, connection);

        return fragments;
    }

    /**
     * Serializes lookup table mapping into byte array.
     *
     * @return serialized lookup table mapping
     * @throws IOException when connection to lookup table fails
     * or serialization fails
     */
    private byte[] prepareUserData() throws Exception {
        HBaseLookupTable lookupTable = new HBaseLookupTable(configuration);
        Map<String, byte[]> mappings = lookupTable.getMappings(context.getDataSource());
        lookupTable.close();

        if (mappings != null) {
            return serializeMap(mappings);
        }

        return null;
    }

    /**
     * Serializes fragment metadata information
     * (region start and end keys) into byte array.
     *
     * @param region region to be serialized
     * @return serialized metadata information
     * @throws IOException when serialization fails
     */
    private byte[] prepareFragmentMetadata(HRegionInfo region) throws IOException {

        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
        objectStream.writeObject(region.getStartKey());
        objectStream.writeObject(region.getEndKey());

        return byteArrayStream.toByteArray();
    }

    private void addTableFragments(byte[] userData) throws IOException {
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(context.getDataSource()));
        List <HRegionLocation> locations = regionLocator.getAllRegionLocations();

        for (HRegionLocation location : locations) {
            addFragment(location, userData);
        }

        regionLocator.close();
    }

    private void addFragment(HRegionLocation location,
            byte[] userData) throws IOException {
        ServerName serverInfo = location.getServerName();
        String[] hosts = new String[] {serverInfo.getHostname()};
        HRegionInfo region = location.getRegionInfo();
        byte[] fragmentMetadata = prepareFragmentMetadata(region);
        Fragment fragment = new Fragment(context.getDataSource(), hosts, fragmentMetadata, userData);
        fragments.add(fragment);
    }

    private byte[] serializeMap(Map<String, byte[]> tableMappings) throws IOException {
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
        objectStream.writeObject(tableMappings);

        return byteArrayStream.toByteArray();
    }
}
