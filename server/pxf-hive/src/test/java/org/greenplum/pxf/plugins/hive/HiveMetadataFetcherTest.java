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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.model.PluginConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HiveMetadataFetcherTest {

    private RequestContext context;
    private HiveMetaStoreClient mockHiveClient;
    private HiveMetadataFetcher fetcher;
    private List<Metadata> metadataList;
    private HiveClientWrapper fakeHiveClientWrapper;
    private HiveClientWrapper.HiveClientFactory mockClientFactory;
    private final HiveUtilities hiveUtilities = new HiveUtilities();

    @BeforeEach
    public void setupCompressionFactory() throws MetaException {

        @SuppressWarnings("unchecked")
        Map<String, String> mockProfileMap = mock(Map.class);
        PluginConf mockPluginConf = mock(PluginConf.class);

        Configuration configuration = new Configuration();

        context = new RequestContext();
        context.setPluginConf(mockPluginConf);
        when(mockPluginConf.getPlugins("HiveText")).thenReturn(mockProfileMap);
        when(mockProfileMap.get("OUTPUTFORMAT")).thenReturn("org.greenplum.pxf.api.io.Text");

        mockHiveClient = mock(HiveMetaStoreClient.class);

        context.setConfig("default");
        context.setServerName("default");
        context.setUser("dummy");
        context.setAdditionalConfigProps(null);
        context.setConfiguration(configuration);

        mockClientFactory = mock(HiveClientWrapper.HiveClientFactory.class);

        fakeHiveClientWrapper = new HiveClientWrapper();
        fakeHiveClientWrapper.setHiveClientFactory(mockClientFactory);
        fakeHiveClientWrapper.setHiveUtilities(hiveUtilities);

        when(mockClientFactory.initHiveClient(any())).thenReturn(mockHiveClient);
    }

    @Test
    public void construction() {
        fetcher = new HiveMetadataFetcher(fakeHiveClientWrapper);
        fetcher.setRequestContext(context);
        fetcher.setHiveUtilities(hiveUtilities);
        fetcher.afterPropertiesSet();
        assertNotNull(fetcher);
    }

    @Test
    public void constructorCantAccessMetaStore() throws MetaException {
        when(mockClientFactory.initHiveClient(any())).thenThrow(new MetaException("which way to albuquerque"));

        fetcher = new HiveMetadataFetcher(fakeHiveClientWrapper);
        fetcher.setRequestContext(context);
        Exception e = assertThrows(RuntimeException.class, fetcher::afterPropertiesSet);
        assertEquals("Failed connecting to Hive MetaStore service: which way to albuquerque", e.getMessage());
    }

    @Test
    public void getTableMetadataView() throws Exception {

        String tableName = "cause";
        fetcher = new HiveMetadataFetcher(fakeHiveClientWrapper);
        fetcher.setRequestContext(context);
        fetcher.afterPropertiesSet();

        // mock hive table returned from hive client
        Table hiveTable = new Table();
        hiveTable.setTableType("VIRTUAL_VIEW");
        when(mockHiveClient.getTable("default", tableName)).thenReturn(hiveTable);

        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> fetcher.getMetadata(tableName));
        assertEquals("Hive views are not supported by PXF", e.getMessage());
    }

    @Test
    public void getTableMetadata() throws Exception {

        fetcher = new HiveMetadataFetcher(fakeHiveClientWrapper);
        fetcher.setRequestContext(context);
        fetcher.setHiveUtilities(hiveUtilities);
        fetcher.afterPropertiesSet();
        String tableName = "cause";

        // mock hive table returned from hive client
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("field1", "string", null));
        fields.add(new FieldSchema("field2", "int", null));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        Table hiveTable = new Table();
        hiveTable.setTableType("MANAGED_TABLE");
        hiveTable.setSd(sd);
        hiveTable.setPartitionKeys(new ArrayList<>());
        when(mockHiveClient.getTable("default", tableName)).thenReturn(hiveTable);

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

        fetcher = new HiveMetadataFetcher(fakeHiveClientWrapper);
        fetcher.setRequestContext(context);
        fetcher.setHiveUtilities(hiveUtilities);
        fetcher.afterPropertiesSet();

        String tablePattern = "*";
        String dbPattern = "*";
        String dbName = "default";
        String tableNameBase = "regulartable";
        String pattern = dbPattern + "." + tablePattern;

        List<String> dbNames = new ArrayList<>(Collections.singletonList(dbName));
        List<String> tableNames = new ArrayList<>();

        // Prepare for tables
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("field1", "string", null));
        fields.add(new FieldSchema("field2", "int", null));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");

        // Mock hive tables returned from hive client
        for (int index = 1; index <= 2; index++) {
            String tableName = tableNameBase + index;
            tableNames.add(tableName);
            Table hiveTable = new Table();
            hiveTable.setTableType("MANAGED_TABLE");
            hiveTable.setSd(sd);
            hiveTable.setPartitionKeys(new ArrayList<>());
            when(mockHiveClient.getTable(dbName, tableName)).thenReturn(hiveTable);
        }

        // Mock database and table names return from hive client
        when(mockHiveClient.getDatabases(dbPattern)).thenReturn(dbNames);
        when(mockHiveClient.getTables(dbName, tablePattern)).thenReturn(tableNames);

        // Get metadata
        metadataList = fetcher.getMetadata(pattern);
        assertEquals(2, metadataList.size());

        for (int index = 1; index <= 2; index++) {
            Metadata metadata = metadataList.get(index - 1);
            assertEquals(dbName + "." + tableNameBase + index, metadata.getItem().toString());
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

        fetcher = new HiveMetadataFetcher(fakeHiveClientWrapper);
        fetcher.setRequestContext(context);
        fetcher.setHiveUtilities(hiveUtilities);
        fetcher.afterPropertiesSet();

        String tablePattern = "*";
        String dbPattern = "*";
        String dbName = "default";
        String pattern = dbPattern + "." + tablePattern;

        String tableName1 = "viewtable";
        // mock hive table returned from hive client
        Table hiveTable1 = new Table();
        hiveTable1.setTableType("VIRTUAL_VIEW");
        when(mockHiveClient.getTable(dbName, tableName1)).thenReturn(hiveTable1);

        String tableName2 = "regulartable";
        // mock hive table returned from hive client
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("field1", "string", null));
        fields.add(new FieldSchema("field2", "int", null));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        Table hiveTable2 = new Table();
        hiveTable2.setTableType("MANAGED_TABLE");
        hiveTable2.setSd(sd);
        hiveTable2.setPartitionKeys(new ArrayList<>());
        when(mockHiveClient.getTable(dbName, tableName2)).thenReturn(hiveTable2);

        // Mock get databases and tables return from hive client
        List<String> tableNames = new ArrayList<>(Arrays.asList(tableName1, tableName2));
        List<String> dbNames = new ArrayList<>(Collections.singletonList(dbName));
        when(mockHiveClient.getDatabases(dbPattern)).thenReturn(dbNames);
        when(mockHiveClient.getTables(dbName, tablePattern)).thenReturn(tableNames);

        // Get metadata
        metadataList = fetcher.getMetadata(pattern);
        assertEquals(1, metadataList.size());
        Metadata metadata = metadataList.get(0);
        assertEquals(dbName + "." + tableName2, metadata.getItem().toString());

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
