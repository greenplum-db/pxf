package org.greenplum.pxf.plugins.jdbc;

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

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class JdbcPartitionFragmenterTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private RequestContext context;

    @Before
    public void setUp() {
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("table");
    }

    @Test
    public void testNoPartition() {

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter();
        fragment.initialize(context);
        List<Fragment> fragments = fragment.getFragments();

        assertEquals(1, fragments.size());
    }

    @Test
    public void testPartitionByTypeInvalid() {
        thrown.expect(IllegalArgumentException.class);

        context.addOption("PARTITION_BY", "level:float");
        new JdbcPartitionFragmenter().initialize(context);
    }

    @Test
    public void testPartitionByFormatInvalid() {
        thrown.expect(IllegalArgumentException.class);

        context.addOption("PARTITION_BY", "level-enum");
        new JdbcPartitionFragmenter().initialize(context);
    }
}
