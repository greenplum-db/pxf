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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class EnumPartitionTestGenerate {

    @Test
    public void testPartitionByEnum() {
        String COLUMN = "col";
        String RANGE = "excellent:good:general:bad";

        EnumPartition[] parts = PartitionType.ENUM.generate(COLUMN, RANGE, null).stream().map(p -> EnumPartition.class.cast(p)).toArray(EnumPartition[]::new);

        assertEquals(5, parts.length);
        assertEnumPartitionEquals(parts[0], "excellent");
        assertEnumPartitionEquals(parts[1], "good");
        assertEnumPartitionEquals(parts[3], "bad");
        assertEnumPartitionEquals(parts[4], new String[]{"excellent", "good", "general", "bad"});
    }

    @Test
    public void testPartitionByEnumSingleValue() {
        String COLUMN = "col";
        String RANGE = "100";

        EnumPartition[] parts = PartitionType.ENUM.generate(COLUMN, RANGE, null).stream().map(p -> EnumPartition.class.cast(p)).toArray(EnumPartition[]::new);

        assertEquals(2, parts.length);
        assertEnumPartitionEquals(parts[0], "100");
        assertEnumPartitionEquals(parts[1], new String[]{"100"});
    }

    private void assertEnumPartitionEquals(EnumPartition partition, String value) {
        assertEquals(value, partition.getValue());
        assertNull(partition.getExcluded());
    }

    private void assertEnumPartitionEquals(EnumPartition partition, String[] excludedValues) {
        assertArrayEquals(excludedValues, partition.getExcluded());
        assertNull(partition.getValue());
    }
}
