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

import static org.junit.Assert.assertEquals;

import java.util.List;

public class IntPartitionTestGenerate {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testPartionByInt() throws Exception {
        String COLUMN = "col";
        String RANGE = "2001:2012";
        String INTERVAL = "2";

        List<IntPartition> parts = IntPartition.generate(COLUMN, RANGE, INTERVAL);

        assertEquals(8, parts.size());
        assertFragmentRangeEquals(parts.get(0), null, 2001L);
        assertFragmentRangeEquals(parts.get(1), 2001L, 2002L);
        assertFragmentRangeEquals(parts.get(3), 2005L, 2006L);
        assertFragmentRangeEquals(parts.get(6), 2011L, 2012L);
        assertFragmentRangeEquals(parts.get(7), 2012L, null);
    }

    @Test
    public void testPartionByIntIncomplete() throws Exception {
        String COLUMN = "col";
        String RANGE = "2001:2012";
        String INTERVAL = "7";

        List<IntPartition> parts = IntPartition.generate(COLUMN, RANGE, INTERVAL);

        assertEquals(3, parts.size());
        assertFragmentRangeEquals(parts.get(0), null, 2001L);
        assertFragmentRangeEquals(parts.get(1), 2001L, 2007L);
        assertFragmentRangeEquals(parts.get(2), 2008L, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRangeIntSwappedInvalid() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        final String COLUMN = "col";
        final String RANGE = "42:17";
        final String INTERVAL = "2";

        IntPartition.generate(COLUMN, RANGE, INTERVAL);
    }

    /**
     * Assert fragment metadata and given range match.
     * @param fragment
     * @param rangeStart (null is allowed)
     * @param rangeEnd (null is allowed)
     */
    private void assertFragmentRangeEquals(IntPartition partition, Long rangeStart, Long rangeEnd) {
        Long[] boundaries = partition.getBoundaries();
        assertEquals(rangeStart, boundaries[0]);
        assertEquals(rangeEnd, boundaries[1]);
    }
}
