package org.greenplum.pxf.api;

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


import org.greenplum.pxf.api.examples.DemoAccessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DemoAccessorTest {

    private RequestContext context;
    private DemoAccessor accessor;

    @BeforeEach
    public void setup() {
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        accessor = new DemoAccessor();
        accessor.initialize(context);
    }

    @Test
    public void testRowsWithSingleColumn() throws Exception {
        context.setDataFragment(0);
        context.setFragmentMetadata("fragment1".getBytes());

        int numRows = 2;
        for (int i = 0; i < numRows; i++) {
            OneRow row = accessor.readNextObject();
            assertEquals(String.format("OneRow:0.%d->fragment1 row%d", i, i + 1), row.toString());
        }
        assertNull(accessor.readNextObject());
    }

    @Test
    public void testRowsWithMultipleColumns() throws Exception {
        context.setDataFragment(0);
        context.setFragmentMetadata("fragment1".getBytes());//, "fragment1".getBytes());
        context.getTupleDescription().add(new ColumnDescriptor("col1", 1, 1, "TEXT", null));
        context.getTupleDescription().add(new ColumnDescriptor("col2", 1, 1, "TEXT", null));
        context.getTupleDescription().add(new ColumnDescriptor("col3", 1, 1, "TEXT", null));

        int numRows = 2;
        for (int i = 0; i < numRows; i++) {
            OneRow row = accessor.readNextObject();
            assertEquals(String.format("OneRow:0.%d->fragment1 row%d,value1,value2", i, i + 1), row.toString());
        }
        assertNull(accessor.readNextObject());
    }
}
