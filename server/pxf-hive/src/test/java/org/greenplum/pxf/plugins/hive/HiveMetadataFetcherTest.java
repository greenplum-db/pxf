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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.model.PluginConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveMetadataFetcher.class}) // Enables mocking 'new' calls
@SuppressStaticInitializationFor({"org.apache.hadoop.hive.metastore.api.MetaException",
        "org.greenplum.pxf.plugins.hive.utilities.HiveUtilities"}) // Prevents static inits
public class HiveMetadataFetcherTest {
    private RequestContext requestContext;
    private Log LOG;
    private HiveConf hiveConfiguration;
    private HiveMetaStoreClient hiveClient;
    private HiveMetadataFetcher fetcher;
    private List<Metadata> metadataList;
    private ConfigurationFactory mockConfigurationFactory;

    @Before
    public void setupCompressionFactory() throws Exception {
        LOG = mock(Log.class);
        Whitebox.setInternalState(HiveUtilities.class, LOG);

        @SuppressWarnings("unchecked")
        Map<String, String> mockProfileMap = mock(Map.class);
        PluginConf mockPluginConf = mock(PluginConf.class);

        requestContext = mock(RequestContext.class);
        when(requestContext.getPluginConf()).thenReturn(mockPluginConf);
        when(mockPluginConf.getPlugins("HiveText")).thenReturn(mockProfileMap);
        when(mockProfileMap.get("OUTPUTFORMAT")).thenReturn("org.greenplum.pxf.service.io.Text");

        Configuration hadoopConfiguration = mock(Configuration.class);
        PowerMockito.whenNew(Configuration.class).withNoArguments().thenReturn(hadoopConfiguration);

        JobConf jobConf = mock(JobConf.class);
        PowerMockito.whenNew(JobConf.class).withArguments(hadoopConfiguration).thenReturn(jobConf);

        hiveConfiguration = mock(HiveConf.class);
        PowerMockito.whenNew(HiveConf.class).withArguments(hadoopConfiguration, HiveConf.class).thenReturn(hiveConfiguration);

        hiveClient = mock(HiveMetaStoreClient.class);
        PowerMockito.whenNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration).thenReturn(hiveClient);

        when(requestContext.getServerName()).thenReturn("default");
        when(requestContext.getOptions()).thenReturn(null);
        mockConfigurationFactory = mock(ConfigurationFactory.class);
        when(mockConfigurationFactory.initConfiguration("default", null)).thenReturn(hadoopConfiguration);
    }

    @Test
    public void construction() throws Exception {
        fetcher = new HiveMetadataFetcher(requestContext, mockConfigurationFactory);
        PowerMockito.verifyNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration);
    }

    @Test
    public void constructorCantAccessMetaStore() throws Exception {
        PowerMockito.whenNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration).thenThrow(new MetaException("which way to albuquerque"));

        try {
            fetcher = new HiveMetadataFetcher(requestContext, mockConfigurationFactory);
            fail("Expected a RuntimeException");
        } catch (RuntimeException ex) {
            assertEquals("Failed connecting to Hive MetaStore service: which way to albuquerque", ex.getMessage());
        }
    }

    @Test
    public void getTableMetadataInvalidTableName() throws Exception {
        fetcher = new HiveMetadataFetcher(requestContext, mockConfigurationFactory);
        String tableName = "t.r.o.u.b.l.e.m.a.k.e.r";

        try {
            fetcher.getMetadata(tableName);
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertEquals("\"t.r.o.u.b.l.e.m.a.k.e.r\" is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>", ex.getMessage());
        }
    }

    @Test
    public void getTableMetadataView() throws Exception {

        fetcher = new HiveMetadataFetcher(requestContext, mockConfigurationFactory);
        String tableName = "cause";

        // mock hive table returned from hive client
        Table hiveTable = new Table();
        hiveTable.setTableType("VIRTUAL_VIEW");
        when(hiveClient.getTable("default", tableName)).thenReturn(hiveTable);

        try {
            metadataList = fetcher.getMetadata(tableName);
            fail("Expected an UnsupportedOperationException because PXF doesn't support views");
        } catch (UnsupportedOperationException e) {
            assertEquals("Hive views are not supported by GPDB", e.getMessage());
        }
    }

    @Test
    public void getTableMetadata() throws Exception {

        fetcher = new HiveMetadataFetcher(requestContext, mockConfigurationFactory);
        String tableName = "cause";

        // mock hive table returned from hive client
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("field1", "string", null));
        fields.add(new FieldSchema("field2", "int", null));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        Table hiveTable = new Table();
        hiveTable.setTableType("MANAGED_TABLE");
        hiveTable.setSd(sd);
        hiveTable.setPartitionKeys(new ArrayList<FieldSchema>());
        when(hiveClient.getTable("default", tableName)).thenReturn(hiveTable);

        // Get metadata
        metadataList = fetcher.getMetadata(tableName);
        Metadata metadata = metadataList.get(0);

        assertEquals("default.cause", metadata.getItem().toString());

        List<Metadata.Field> resultFields = metadata.getFields();
        assertNotNull(resultFields);
        assertEquals(2, resultFields.size());
        Metadata.Field field = resultFields.get(0);
        assertEquals("field1", field.getName());
        assertEquals("text", field.getType().getTypeName()); // converted type
        field = resultFields.get(1);
        assertEquals("field2", field.getName());
        assertEquals("int4", field.getType().getTypeName());
    }

    @Test
    public void getTableMetadataWithMultipleTables() throws Exception {

        fetcher = new HiveMetadataFetcher(requestContext, mockConfigurationFactory);

        String tablepattern = "*";
        String dbpattern = "*";
        String dbname = "default";
        String tablenamebase = "regulartable";
        String pattern = dbpattern + "." + tablepattern;

        List<String> dbNames = new ArrayList<String>(Arrays.asList(dbname));
        List<String> tableNames = new ArrayList<String>();

        // Prepare for tables
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("field1", "string", null));
        fields.add(new FieldSchema("field2", "int", null));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");

        // Mock hive tables returned from hive client
        for (int index = 1; index <= 2; index++) {
            String tableName = tablenamebase + index;
            tableNames.add(tableName);
            ;
            Table hiveTable = new Table();
            hiveTable.setTableType("MANAGED_TABLE");
            hiveTable.setSd(sd);
            hiveTable.setPartitionKeys(new ArrayList<FieldSchema>());
            when(hiveClient.getTable(dbname, tableName)).thenReturn(hiveTable);
        }

        // Mock database and table names return from hive client
        when(hiveClient.getDatabases(dbpattern)).thenReturn(dbNames);
        when(hiveClient.getTables(dbname, tablepattern)).thenReturn(tableNames);

        // Get metadata
        metadataList = fetcher.getMetadata(pattern);
        assertEquals(2, metadataList.size());

        for (int index = 1; index <= 2; index++) {
            Metadata metadata = metadataList.get(index - 1);
            assertEquals(dbname + "." + tablenamebase + index, metadata.getItem().toString());
            List<Metadata.Field> resultFields = metadata.getFields();
            assertNotNull(resultFields);
            assertEquals(2, resultFields.size());
            Metadata.Field field = resultFields.get(0);
            assertEquals("field1", field.getName());
            assertEquals("text", field.getType().getTypeName()); // converted type
            field = resultFields.get(1);
            assertEquals("field2", field.getName());
            assertEquals("int4", field.getType().getTypeName());
        }
    }

    @Test
    public void getTableMetadataWithIncompatibleTables() throws Exception {

        fetcher = new HiveMetadataFetcher(requestContext, mockConfigurationFactory);

        String tablepattern = "*";
        String dbpattern = "*";
        String dbname = "default";
        String pattern = dbpattern + "." + tablepattern;

        String tableName1 = "viewtable";
        // mock hive table returned from hive client
        Table hiveTable1 = new Table();
        hiveTable1.setTableType("VIRTUAL_VIEW");
        when(hiveClient.getTable(dbname, tableName1)).thenReturn(hiveTable1);

        String tableName2 = "regulartable";
        // mock hive table returned from hive client
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("field1", "string", null));
        fields.add(new FieldSchema("field2", "int", null));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        Table hiveTable2 = new Table();
        hiveTable2.setTableType("MANAGED_TABLE");
        hiveTable2.setSd(sd);
        hiveTable2.setPartitionKeys(new ArrayList<FieldSchema>());
        when(hiveClient.getTable(dbname, tableName2)).thenReturn(hiveTable2);

        // Mock get databases and tables return from hive client
        List<String> tableNames = new ArrayList<String>(Arrays.asList(tableName1, tableName2));
        List<String> dbNames = new ArrayList<String>(Arrays.asList(dbname));
        when(hiveClient.getDatabases(dbpattern)).thenReturn(dbNames);
        when(hiveClient.getTables(dbname, tablepattern)).thenReturn(tableNames);

        // Get metadata
        metadataList = fetcher.getMetadata(pattern);
        assertEquals(1, metadataList.size());
        Metadata metadata = metadataList.get(0);
        assertEquals(dbname + "." + tableName2, metadata.getItem().toString());

        List<Metadata.Field> resultFields = metadata.getFields();
        assertNotNull(resultFields);
        assertEquals(2, resultFields.size());
        Metadata.Field field = resultFields.get(0);
        assertEquals("field1", field.getName());
        assertEquals("text", field.getType().getTypeName()); // converted type
        field = resultFields.get(1);
        assertEquals("field2", field.getName());
        assertEquals("int4", field.getType().getTypeName());
    }
}
