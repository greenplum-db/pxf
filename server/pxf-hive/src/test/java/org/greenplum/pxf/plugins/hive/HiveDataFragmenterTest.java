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
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HiveDataFragmenterTest {

    private RequestContext context;
    private Configuration configuration;
    private HiveClientWrapper hiveClientWrapper;

    @BeforeEach
    public void setup() {

        hiveClientWrapper = mock(HiveClientWrapper.class);

        configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs:///");

        context = new RequestContext();
        context.setConfig("default");
        context.setServerName("default");
        context.setUser("dummy");
        context.setConfiguration(configuration);
    }

    @Test
    public void constructorCantAccessMetaStore() {
        when(hiveClientWrapper.initHiveClient(context, configuration)).thenThrow(new RuntimeException("Failed connecting to Hive MetaStore service: which way to albuquerque"));

        HiveDataFragmenter fragmenter = new HiveDataFragmenter();
        fragmenter.setHiveClientWrapper(hiveClientWrapper);
        fragmenter.setRequestContext(context);
        Exception e = assertThrows(RuntimeException.class, fragmenter::afterPropertiesSet);
        assertEquals("Failed connecting to Hive MetaStore service: which way to albuquerque", e.getMessage());
    }
}
