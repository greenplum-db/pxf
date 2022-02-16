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
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HiveDataFragmenterTest {

    private RequestContext context;
    private Configuration configuration;
    private HiveClientWrapper.MetaStoreClientHolder holder;

    @Mock
    private HiveClientWrapper hiveClientWrapper;
    @Mock
    private HiveUtilities hiveUtilities;
    @Mock
    private Metadata.Item mockItem;
    @Mock
    private IMetaStoreClient mockHiveClient;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs:///");

        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("sometable");
        context.setServerName("default");
        context.setUser("dummy");
        context.setConfiguration(configuration);

        holder = new HiveClientWrapper.MetaStoreClientHolder(mockHiveClient);
    }

    @Test
    public void failsToInitHiveClient() {
        when(hiveClientWrapper.initHiveClient(context, configuration)).thenThrow(new RuntimeException("test"));

        HiveDataFragmenter fragmenter = new HiveDataFragmenter(hiveUtilities, hiveClientWrapper);
        fragmenter.setRequestContext(context);
        fragmenter.afterPropertiesSet();
        Exception e = assertThrows(RuntimeException.class, fragmenter::getFragments);
        assertEquals("test", e.getMessage());
    }

    @Test
    public void failsToGetTableInfo_ClosesHiveClient() throws Exception {
        when(hiveClientWrapper.extractTableFromName(context.getDataSource())).thenReturn(mockItem);
        when(hiveClientWrapper.initHiveClient(context, configuration)).thenReturn(holder);
        when(hiveClientWrapper.getHiveTable(mockHiveClient, mockItem)).thenThrow(new RuntimeException("test"));

        HiveDataFragmenter fragmenter = new HiveDataFragmenter(hiveUtilities, hiveClientWrapper);
        fragmenter.setRequestContext(context);
        fragmenter.afterPropertiesSet();
        Exception e = assertThrows(RuntimeException.class, fragmenter::getFragments);
        assertEquals("test", e.getMessage());

        verify(mockHiveClient).close();
    }

    @Test
    public void failsToGetTableInfo_FailsToCloseHiveClient() throws Exception {
        when(hiveClientWrapper.extractTableFromName(context.getDataSource())).thenReturn(mockItem);
        when(hiveClientWrapper.initHiveClient(context, configuration)).thenReturn(holder);
        when(hiveClientWrapper.getHiveTable(mockHiveClient, mockItem)).thenThrow(new RuntimeException("test"));
        doThrow(new RuntimeException("ignored")).when(mockHiveClient).close();

        HiveDataFragmenter fragmenter = new HiveDataFragmenter(hiveUtilities, hiveClientWrapper);
        fragmenter.setRequestContext(context);
        fragmenter.afterPropertiesSet();
        Exception e = assertThrows(RuntimeException.class, fragmenter::getFragments);
        assertEquals("test", e.getMessage());

        verify(mockHiveClient).close();
    }

}
