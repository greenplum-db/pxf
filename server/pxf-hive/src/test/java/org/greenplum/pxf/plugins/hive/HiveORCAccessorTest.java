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

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory.SARG_PUSHDOWN;
import static org.junit.Assert.assertEquals;

public class HiveORCAccessorTest {

    private RequestContext context;
    private HiveORCAccessor accessor;

    @Before
    public void setup() throws Exception {
        HiveUserData userData = new HiveUserData("", "", null, HiveDataFragmenter.HIVE_NO_PART_TBL, true, "1", "", 0);
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("foo");
        context.setFragmentMetadata(HdfsUtilities.prepareFragmentMetadata(0, 0, new String[]{"localhost"}));
        context.setFragmentUserData(userData.toString().getBytes());
        context.getTupleDescription().add(new ColumnDescriptor("col1", 1, 1, "TEXT", null));
        context.getTupleDescription().add(new ColumnDescriptor("FOO", 1, 1, "TEXT", null));
        context.setAccessor(HiveORCAccessor.class.getName());

        accessor = new HiveORCAccessor();
        accessor.initialize(context);
    }

    @Test
    public void parseFilterWithISNULL() {
        context.setFilterString("a1o8");
        try {
            accessor.openForRead();
        } catch (Exception e) {
            // Ignore exception thrown by openForRead complaining about file foo not found
        }

        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd().isNull("FOO").end().build();
        assertEquals(sarg.toKryo(), accessor.getJobConf().get(SARG_PUSHDOWN));
    }

    @Test
    public void parseFilterWithISNOTNULL() {

        context.setFilterString("a1o9");
        try {
            accessor.openForRead();
        } catch (Exception e) {
            // Ignore exception thrown by openForRead complaining about file foo not found
        }

        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd().startNot().isNull("FOO").end().end().build();
        assertEquals(sarg.toKryo(), accessor.getJobConf().get(SARG_PUSHDOWN));
    }

    @Test
    public void parseFilterWithIn() {

        context.setFilterString("a1m1007s1d1s1d2s1d3o10");
        try {
            accessor.openForRead();
        } catch (Exception e) {
            // Ignore exception thrown by openForRead complaining about file foo not found
        }

        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd().in("FOO", 1, 2, 3).end().build();
        assertEquals(sarg.toKryo(), accessor.getJobConf().get(SARG_PUSHDOWN));
    }

    @Test(expected = IllegalStateException.class)
    public void emitAggObjectCountStatsNotInitialized() {
        accessor.emitAggObject();
    }
}
