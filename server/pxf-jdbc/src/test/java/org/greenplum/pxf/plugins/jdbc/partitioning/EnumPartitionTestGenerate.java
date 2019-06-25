package org.greenplum.pxf.plugins.jdbc.partitioning;

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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

import java.util.List;

public class EnumPartitionTestGenerate {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testPartionByEnum() throws Exception {
        String COLUMN = "col";
        String RANGE = "excellent:good:general:bad";

        List<EnumPartition> parts = EnumPartition.generate(COLUMN, RANGE, null);

        assertEquals(5, parts.size());
        assertEnumPartitionEquals(parts.get(0), "excellent");
        assertEnumPartitionEquals(parts.get(1), "good");
        assertEnumPartitionEquals(parts.get(3), "bad");
        assertEnumPartitionEquals(parts.get(4), new String[]{"excellent", "good", "general", "bad"});
    }

    @Test
    public void testPartitionByEnumSingleValue() throws Exception {
        String COLUMN = "col";
        String RANGE = "100";

        List<EnumPartition> parts = EnumPartition.generate(COLUMN, RANGE, null);

        assertEquals(2, parts.size());
        assertEnumPartitionEquals(parts.get(0), "100");
        assertEnumPartitionEquals(parts.get(1), new String[]{"100"});
    }

    private void assertEnumPartitionEquals(EnumPartition partition, String value) {
        assertEquals(value, partition.getValue());
        assertNull(partition.getValue());
    }

    private void assertEnumPartitionEquals(EnumPartition partition, String[] excludedValues) {
        assertArrayEquals(excludedValues, partition.getExcluded());
        assertNull(partition.getValue());
    }
}
