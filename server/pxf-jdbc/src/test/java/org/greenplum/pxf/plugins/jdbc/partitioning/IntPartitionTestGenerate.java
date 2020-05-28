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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IntPartitionTestGenerate {

    @Test
    public void testPartitionByInt() {
        String COLUMN = "col";
        String RANGE = "2001:2012";
        String INTERVAL = "2";

        IntPartition[] parts = PartitionType.INT.generate(COLUMN, RANGE, INTERVAL).stream().map(p -> IntPartition.class.cast(p)).toArray(IntPartition[]::new);

        assertEquals(8, parts.length);
        assertFragmentRangeEquals(parts[0], null, 2001L);
        assertFragmentRangeEquals(parts[1], 2012L, null);
        assertFragmentRangeEquals(parts[2], 2001L, 2003L);
        assertFragmentRangeEquals(parts[3], 2003L, 2005L);
        assertFragmentRangeEquals(parts[4], 2005L, 2007L);
        assertFragmentRangeEquals(parts[5], 2007L, 2009L);
        assertFragmentRangeEquals(parts[6], 2009L, 2011L);
        assertFragmentRangeEquals(parts[7], 2011L, 2012L);
    }

    @Test
    public void testPartionByIntIncomplete() {
        String COLUMN = "col";
        String RANGE = "2001:2012";
        String INTERVAL = "7";

        IntPartition[] parts = PartitionType.INT.generate(COLUMN, RANGE, INTERVAL).stream().map(p -> IntPartition.class.cast(p)).toArray(IntPartition[]::new);

        assertEquals(4, parts.length);

        assertFragmentRangeEquals(parts[0], null, 2001L);
        assertFragmentRangeEquals(parts[1], 2012L, null);
        assertFragmentRangeEquals(parts[2], 2001L, 2008L);
        assertFragmentRangeEquals(parts[3], 2008L, 2012L);
    }

    @Test
    public void testRangeIntSwappedInvalid() {
        final String COLUMN = "col";
        final String RANGE = "42:17";
        final String INTERVAL = "2";
        assertThrows(IllegalArgumentException.class,
            () -> PartitionType.INT.generate(COLUMN, RANGE, INTERVAL));
    }

    /**
     * Assert fragment metadata and given range match.
     *
     * @param partition  partition information
     * @param rangeStart (null is allowed)
     * @param rangeEnd   (null is allowed)
     */
    private void assertFragmentRangeEquals(IntPartition partition, Long rangeStart, Long rangeEnd) {
        Long[] boundaries = partition.getBoundaries();
        assertEquals(rangeStart, boundaries[0]);
        assertEquals(rangeEnd, boundaries[1]);
    }
}
