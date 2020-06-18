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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg.SARG_PUSHDOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HiveORCAccessorTest {

    private RequestContext context;
    private HiveORCAccessor accessor;

    @BeforeEach
    public void setup() {
        HiveFragmentMetadata.Builder builder = HiveFragmentMetadata.Builder
                .aHiveFragmentMetadata()
                .withInputFormatName("")
                .withSerdeClassName("")
                .withPartitionKeys(HiveDataFragmenter.HIVE_NO_PART_TBL)
                .withFilterInFragmenter(true)
                .withDelimiter("1")
                .withColTypes("")
                .withSkipHeader(0)
                .withHiveIndexes(Arrays.asList(0, 1))
                .withAllColumnNames("col1,FOO")
                .withAllColumnTypes("string, string")
                .withStart(0)
                .withLength(0);
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setDataSource("foo");
        context.setFragmentMetadata(builder.build());
        context.getTupleDescription().add(new ColumnDescriptor("col1", 1, 1, "TEXT", null));
        context.getTupleDescription().add(new ColumnDescriptor("FOO", 1, 1, "TEXT", null));
        context.setAccessor(HiveORCAccessor.class.getName());
        context.setConfiguration(new Configuration());

        accessor = new HiveORCAccessor();
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
    }

    @Test
    public void parseFilterWithISNULL() {
        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd().isNull("FOO", PredicateLeaf.Type.STRING).end().build();
        String expected = toKryo(sarg);

        context.setFilterString("a1o8");
        try {
            accessor.openForRead();
        } catch (Exception e) {
            // Ignore exception thrown by openForRead complaining about file foo not found
        }

        assertEquals(expected, accessor.getJobConf().get(SARG_PUSHDOWN));
    }

    @Test
    public void parseFilterWithISNOTNULL() {

        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd().startNot().isNull("FOO", PredicateLeaf.Type.STRING).end().end().build();
        String expected = toKryo(sarg);

        context.setFilterString("a1o9");
        try {
            accessor.openForRead();
        } catch (Exception e) {
            // Ignore exception thrown by openForRead complaining about file foo not found
        }

        assertEquals(expected, accessor.getJobConf().get(SARG_PUSHDOWN));
    }

    @Test
    public void parseFilterWithIn() {

        SearchArgument sarg = SearchArgumentFactory.
                newBuilder().
                startAnd().
                in("FOO", PredicateLeaf.Type.LONG, 1L, 2L, 3L).
                end().
                build();
        String expected = toKryo(sarg);

        // _1_ IN (1,2,3)
        context.setFilterString("a1m1007s1d1s1d2s1d3o10");
        try {
            accessor.openForRead();
        } catch (Exception e) {
            // Ignore exception thrown by openForRead complaining about file foo not found
        }

        assertEquals(expected, accessor.getJobConf().get(SARG_PUSHDOWN));
    }

    @Test
    public void emitAggObjectCountStatsNotInitialized() {
        assertThrows(IllegalStateException.class, accessor::emitAggObject);
    }

    private String toKryo(SearchArgument sarg) {
        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        new Kryo().writeObject(out, sarg);
        out.close();
        return Base64.encodeBase64String(out.toBytes());
    }
}
