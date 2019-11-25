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
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HiveDataFragmenterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private RequestContext context;
    private Configuration configuration;
    private ConfigurationFactory configurationFactory;
    private HiveClientWrapper hiveClientWrapper;

    @Before
    public void setup() {

        hiveClientWrapper = mock(HiveClientWrapper.class);
        configurationFactory = mock(ConfigurationFactory.class);

        context = new RequestContext();
        context.setConfig("default");
        context.setServerName("default");
        context.setUser("dummy");

        configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs:///");

        when(configurationFactory
                .initConfiguration("default", "default", "dummy", context.getAdditionalConfigProps()))
                .thenReturn(configuration);
    }

    @Test
    public void constructorCantAccessMetaStore() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Failed connecting to Hive MetaStore service: which way to albuquerque");

        when(hiveClientWrapper.initHiveClient(context, configuration)).thenThrow(new RuntimeException("Failed connecting to Hive MetaStore service: which way to albuquerque"));

        HiveDataFragmenter fragmenter = new HiveDataFragmenter(configurationFactory, hiveClientWrapper);
        fragmenter.initialize(context);
    }
}
