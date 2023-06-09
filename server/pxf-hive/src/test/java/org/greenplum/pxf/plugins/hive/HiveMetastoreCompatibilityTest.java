package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClientCompatibility1xx;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.greenplum.pxf.api.model.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockConstructionWithAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class HiveMetastoreCompatibilityTest {

    private HiveMetaStoreClientCompatibility1xx hiveCompatiblityClient;
    private HiveClientWrapper hiveClientWrapper;
    private HiveClientWrapper.HiveClientFactory hiveClientFactory;
    private ThriftHiveMetastore.Client mockThriftClient;
    private Map<String, String> hiveTableParameters;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() throws MetaException {
        mockThriftClient = mock(ThriftHiveMetastore.Client.class);
        hiveClientFactory = new HiveClientWrapper.HiveClientFactory();
        hiveClientWrapper = new HiveClientWrapper();
        hiveClientWrapper.setHiveClientFactory(hiveClientFactory);
        hiveTableParameters = new HashMap<>();
    }

    @Test
    public void getTableMetaException() throws Exception {

        String name = "orphan";

        try (MockedStatic<HiveMetaStore> hiveMetaStoreMockedStatic = mockStatic(HiveMetaStore.class)) {
            hiveMetaStoreMockedStatic.when(() -> HiveMetaStore.newRetryingHMSHandler(any(String.class), any(), any(Boolean.class)))
                    .thenReturn(mockThriftClient);

            when(mockThriftClient.get_table_req(any())).thenThrow(new MetaException("some meta failure"));

            Configuration configuration = new Configuration();
            hiveCompatiblityClient = new HiveMetaStoreClientCompatibility1xx(new HiveConf(configuration, HiveConf.class));
            Exception e = assertThrows(MetaException.class,
                    () -> hiveCompatiblityClient.getTable("default", name));
            assertEquals("some meta failure", e.getMessage());
        }
    }

    @Test
    public void getTableNoSuchObjectException() throws Exception {

        String name = "orphan";

        try (MockedStatic<HiveMetaStore> hiveMetaStoreMockedStatic = mockStatic(HiveMetaStore.class)) {
            hiveMetaStoreMockedStatic.when(() -> HiveMetaStore.newRetryingHMSHandler(any(String.class), any(), any(Boolean.class)))
                    .thenReturn(mockThriftClient);

            when(mockThriftClient.get_table_req(any())).thenThrow(new NoSuchObjectException("where's my table"));

            Configuration configuration = new Configuration();
            hiveCompatiblityClient = new HiveMetaStoreClientCompatibility1xx(new HiveConf(configuration, HiveConf.class));
            Exception e = assertThrows(NoSuchObjectException.class,
                    () -> hiveCompatiblityClient.getTable("default", name));
            assertEquals("where's my table", e.getMessage());
        }
    }

    @Test
    public void getTableFallback() throws Exception {

        String name = "orphan";
        Table hiveTable = new Table();
        hiveTable.setTableName(name);
        hiveTable.setTableType("MANAGED_TABLE");

        try (MockedStatic<HiveMetaStore> hiveMetaStoreMockedStatic = mockStatic(HiveMetaStore.class)) {
            hiveMetaStoreMockedStatic.when(() -> HiveMetaStore.newRetryingHMSHandler(any(String.class), any(), any(Boolean.class)))
                    .thenReturn(mockThriftClient);

            when(mockThriftClient.get_table_req(any())).thenThrow(new TApplicationException("fallback"));
            when(mockThriftClient.get_table("default", name)).thenReturn(hiveTable);

            Configuration configuration = new Configuration();
            hiveCompatiblityClient = new HiveMetaStoreClientCompatibility1xx(new HiveConf(configuration, HiveConf.class));
            Table resultTable = hiveCompatiblityClient.getTable("default", name);
            assertEquals(name, resultTable.getTableName());
        }
    }

    @Test
    public void getTableFailedToConnectToMetastoreFallback() throws Exception {

        String name = "orphan";

        try (MockedStatic<HiveMetaStore> hiveMetaStoreMockedStatic = mockStatic(HiveMetaStore.class)) {
            hiveMetaStoreMockedStatic.when(() -> HiveMetaStore.newRetryingHMSHandler(any(String.class), any(), any(Boolean.class)))
                    .thenReturn(mockThriftClient);

            when(mockThriftClient.get_table_req(any())).thenThrow(new TApplicationException("fallback"));
            when(mockThriftClient.get_table("default", name)).thenThrow(new TTransportException("oops. where's the metastore?"));

            Configuration configuration = new Configuration();
            hiveCompatiblityClient = new HiveMetaStoreClientCompatibility1xx(new HiveConf(configuration, HiveConf.class));
            Exception e = assertThrows(TTransportException.class,
                    () -> hiveCompatiblityClient.getTable("default", name));
            assertEquals("oops. where's the metastore?", e.getMessage());
        }
    }

    @Test
    public void getTableFailedToConnectToMetastore() throws Exception {

        String name = "orphan";

        try (MockedStatic<HiveMetaStore> hiveMetaStoreMockedStatic = mockStatic(HiveMetaStore.class)) {
            hiveMetaStoreMockedStatic.when(() -> HiveMetaStore.newRetryingHMSHandler(any(String.class), any(), any(Boolean.class)))
                    .thenReturn(mockThriftClient);

            when(mockThriftClient.get_table_req(any())).thenThrow(new TTransportException("oops. where's the metastore?"));

            Configuration configuration = new Configuration();
            hiveCompatiblityClient = new HiveMetaStoreClientCompatibility1xx(new HiveConf(configuration, HiveConf.class));
            Exception e = assertThrows(TTransportException.class,
                    () -> hiveCompatiblityClient.getTable("default", name));
            assertEquals("oops. where's the metastore?", e.getMessage());
        }
    }

    @Test
    public void getTableFailedToConnectToMetastoreNoRetries() throws Exception {

        String name = "orphan";
        Table hiveTable = new Table();
        hiveTable.setTableName(name);
        hiveTable.setTableType("MANAGED_TABLE");

        try (MockedStatic<HiveMetaStore> hiveMetaStoreMockedStatic = mockStatic(HiveMetaStore.class)) {
            hiveMetaStoreMockedStatic.when(() -> HiveMetaStore.newRetryingHMSHandler(any(String.class), any(), any(Boolean.class)))
                    .thenReturn(mockThriftClient);

            when(mockThriftClient.get_table_req(any())).thenThrow(new TApplicationException("fallback"));
            when(mockThriftClient.get_table("default", name))
                    .thenThrow(new TTransportException("oops. where's the metastore? 1"))
                    .thenThrow(new TTransportException("oops. where's the metastore? 2"))
                    .thenThrow(new TTransportException("oops. where's the metastore? 3"))
                    .thenReturn(hiveTable);

            Configuration configuration = new Configuration();
            HiveConf hiveConf = new HiveConf(configuration, HiveConf.class);
            hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, 0);

            IMetaStoreClient client = hiveClientFactory.initHiveClient(hiveConf).getClient();

            Exception e = assertThrows(TTransportException.class,
                    () -> hiveClientWrapper.getHiveTable(client, new Metadata.Item("default", name)));
            assertEquals("oops. where's the metastore? 1", e.getMessage());
        }
    }

    @Test
    public void getTableFailedToConnectToMetastoreFiveFailedRetries() throws Exception {

        String name = "orphan";
        Table hiveTable = new Table();
        hiveTable.setTableName(name);
        hiveTable.setTableType("MANAGED_TABLE");

        try (MockedConstruction<TSocket> tSocketMockedConstruction = mockConstruction(TSocket.class, (mockSocket, context) ->
                when(mockSocket.isOpen()).thenReturn(true));
             MockedConstruction<ThriftHiveMetastore.Client> thriftHiveMetastoreClientMockedConstruction = mockConstruction(ThriftHiveMetastore.Client.class,
                (mockClient, context) -> {
                    when(mockClient.get_table_req(any())).thenThrow(new TApplicationException("fallback"));
                    when(mockClient.get_table("default", name))
                            .thenThrow(new TTransportException("oops. where's the metastore?"));
                }
        )) {
            Configuration configuration = new Configuration();
            HiveConf hiveConf = new HiveConf(configuration, HiveConf.class);
            hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, 5);
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "test://test:1234");

            IMetaStoreClient client = hiveClientFactory.initHiveClient(hiveConf).getClient();

            Exception e = assertThrows(TTransportException.class,
                    () -> hiveClientWrapper.getHiveTable(client, new Metadata.Item("default", name)));
            assertEquals("oops. where's the metastore?", e.getMessage());
            assertEquals(6, thriftHiveMetastoreClientMockedConstruction.constructed().size());
            assertEquals(6, tSocketMockedConstruction.constructed().size());
        }
    }

    @Test
    public void getTableFailedToConnectToMetastoreNoFallback1Retry2ndSuccess() throws Exception {

        String name = "orphan";
        Table hiveTable = new Table();
        hiveTable.setTableName(name);
        hiveTable.setTableType("MANAGED_TABLE");
        hiveTable.setParameters(hiveTableParameters);

        try (MockedConstruction<TSocket> tSocketMockedConstruction = mockConstruction(TSocket.class, (mockSocket, context) ->
                when(mockSocket.isOpen()).thenReturn(true));
             MockedConstruction<ThriftHiveMetastore.Client> thriftHiveMetastoreClientMockedConstruction = mockConstructionWithAnswer(ThriftHiveMetastore.Client.class,
                     // first run through
                     invocation ->  {
                         if (invocation.getMethod().getName().equals("get_table_req")) {
                             throw new TTransportException("oops. where's the metastore? 1");
                         }
                         return null;
                     },
                     // second run through (retry 1 = success)
                     invocation ->  {
                         if (invocation.getMethod().getName().equals("get_table_req")) {
                             GetTableResult tempRes = new GetTableResult(hiveTable);
                             return tempRes;
                         }
                         return null;
                     }

             )) {
            Configuration configuration = new Configuration();
            HiveConf hiveConf = new HiveConf(configuration, HiveConf.class);
            hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, 1);
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "test://test:1234");

            IMetaStoreClient client = hiveClientFactory.initHiveClient(hiveConf).getClient();

            Table resultTable = hiveClientWrapper.getHiveTable(client, new Metadata.Item("default", name));
            assertEquals(hiveTable.getTableName(), resultTable.getTableName());
            assertEquals(2, thriftHiveMetastoreClientMockedConstruction.constructed().size());
            assertEquals(2, tSocketMockedConstruction.constructed().size());
        }
    }

    @Test
    public void getTableFailedToConnectToMetastoreFiveRetries3rdSuccess() throws Exception {

        String name = "orphan";
        Table hiveTable = new Table();
        hiveTable.setTableName(name);
        hiveTable.setTableType("MANAGED_TABLE");
        hiveTable.setParameters(hiveTableParameters);

        try (MockedConstruction<TSocket> tSocketMockedConstruction = mockConstruction(TSocket.class, (mockSocket, context) ->
                when(mockSocket.isOpen()).thenReturn(true));
             MockedConstruction<ThriftHiveMetastore.Client> thriftHiveMetastoreClientMockedConstruction = mockConstructionWithAnswer(ThriftHiveMetastore.Client.class,
                     // first run through
                     invocation -> {
                         if (invocation.getMethod().getName().equals("get_table_req")) {
                             throw new TApplicationException("fallback 1");
                         } else if (invocation.getMethod().getName().equals("get_table")) {
                             throw new TTransportException("oops. where's the metastore? 1");
                         }
                         return null;
                     },
                     // second run through (retry 1)
                     invocation -> {
                         if (invocation.getMethod().getName().equals("get_table_req")) {
                             throw new TApplicationException("fallback 2");
                         } else if (invocation.getMethod().getName().equals("get_table")) {
                             throw new TTransportException("oops. where's the metastore? 2");
                         }
                         return null;
                     },
                     // third run through (retry 2)
                     invocation -> {
                         if (invocation.getMethod().getName().equals("get_table_req")) {
                             throw new TApplicationException("fallback 3");
                         } else if (invocation.getMethod().getName().equals("get_table")) {
                             throw new TTransportException("oops. where's the metastore? 3");
                         }
                         return null;
                     },
                     // placebo run through
                     // the second to last invocation keeps getting skipped so place this here as a placebo
                     invocation -> {
                         if (invocation.getMethod().getName().equals("get_table_req")) {
                             throw new TApplicationException("fallback ???");
                         } else if (invocation.getMethod().getName().equals("get_table")) {
                             throw new TTransportException("oops. where's the metastore? ???");
                         }
                         return null;
                     },
                     // final run through (retry 3 = success)
                     invocation -> {
                         if (invocation.getMethod().getName().equals("get_table_req")) {
                             throw new TApplicationException("fallback");
                         } else if (invocation.getMethod().getName().equals("get_table")) {
                             return hiveTable;
                         }
                         return null;
                     }
             )) {
            Configuration configuration = new Configuration();
            HiveConf hiveConf = new HiveConf(configuration, HiveConf.class);
            hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, 5);
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "test://test:1234");

            IMetaStoreClient client = hiveClientFactory.initHiveClient(hiveConf).getClient();

            Table resultTable = hiveClientWrapper.getHiveTable(client, new Metadata.Item("default", name));
            assertEquals(hiveTable.getTableName(), resultTable.getTableName());
            assertEquals(5, thriftHiveMetastoreClientMockedConstruction.constructed().size());
            assertEquals(5, tSocketMockedConstruction.constructed().size());
        }
    }
}
