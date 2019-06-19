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

import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

class IntPartition extends BasePartition implements JdbcFragmentMetadata {
    private static final long serialVersionUID = 0L;

    private final Long[] boundaries;
    private final boolean isBoundedBoundaryIncluded;

    /**
     * @return a type of {@link Partition} which is the caller of {@link DatePartition#generate} function.
     * Currently, only used for exception messages.
     */
    public static Partition type() {
        return Partition.INT;
    }

    /**
     * Generate an array of {@link IntPartition}s using the provided column name, RANGE and INTERVAL string values
     * @param column
     * @param range
     * @param interval
     * @return an array of properly initialized {@link IntPartition} objects
     */
    public static List<IntPartition> generate(String column, String range, String interval) {
        // Check input
        if (column == null) {
            throw new RuntimeException("The column name must be provided");
        }
        if (range == null) {
            throw new IllegalArgumentException(String.format(
                "The parameter 'RANGE' must be specified for partition of type '%s'", type()
            ));
        }
        if (interval == null) {
            throw new IllegalArgumentException(String.format(
                "The parameter 'INTERVAL' must be specified for partition of type '%s'", type()
            ));
        }

        // Parse RANGE
        long rangeStart;
        long rangeEnd;
        {
            String[] rangeBoundaries = range.split(":", 2);
            if (rangeBoundaries.length != 2) {
                throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' has incorrect format. The correct format for partition of type '%s' is '<start_value>:<end_value>'", type()
                ));
            }

            try {
                rangeStart = Long.parseLong(rangeBoundaries[0]);
                rangeEnd = Long.parseLong(rangeBoundaries[1]);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' is invalid. Both range boundaries must be integers for partition of type '%s'", type()
                ));
            }

            if (rangeEnd < rangeStart) {
                throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' is invalid. The <end_value> '%s' must be greater or equal to the <start_value> '%s' for partition of type '%s'", rangeEnd, rangeStart, type()
                ));
            }
        }

        // Parse INTERVAL
        long intervalNum;
        {
            try {
                intervalNum = Long.parseLong(interval);
            }
            catch (NumberFormatException ex) {
                throw new IllegalArgumentException(String.format(
                    "The '<interval_num>' in parameter 'INTERVAL' must be an integer for partition of type '%s'", type()
                ));
            }

            if (intervalNum < 1) {
                throw new IllegalArgumentException("The '<interval_num>' in parameter 'INTERVAL' must be at least 1, but actual is " + intervalNum);
            }
        }

        // Generate partitions
        List<IntPartition> partitions = new LinkedList<>();
        {
            partitions.add(new IntPartition(column, null, rangeStart));

            boolean isLeftBoundedPartitionAdded = false;
            long fragStart = rangeStart;
            while (fragStart <= rangeEnd) {
                long fragStartNext = fragStart + intervalNum;
                long fragEnd = fragStartNext - 1;

                if (fragEnd > rangeEnd) {
                    partitions.add(new IntPartition(column, fragStart, null, true));
                    isLeftBoundedPartitionAdded = true;
                }
                else {
                    partitions.add(new IntPartition(column, fragStart, fragEnd));
                }

                fragStart = fragStartNext;
            }

            if (!isLeftBoundedPartitionAdded) {
                partitions.add(new IntPartition(column, rangeEnd, null, false));
            }
        }

        return partitions;
    }

    /**
     * @param column
     * @param start null for right-bounded interval
     * @param end null for left-bounded interval
     * @param isBoundedBoundaryIncluded whether the bounded interval should include the boundary. Meaningful only for bounded intervals
     */
    public IntPartition(String column, Long start, Long end, boolean isBoundedBoundaryIncluded) {
        super(column);
        if (start == null && end == null) {
            throw new RuntimeException("Both boundaries cannot be null");
        }

        if (start == end) {
            this.boundaries = new Long[]{start};
        }
        else {
            this.boundaries = new Long[]{start, end};
        }
        this.isBoundedBoundaryIncluded = isBoundedBoundaryIncluded;
    }

    /**
     * @param column
     * @param start null for right-bounded interval
     * @param end null for left-bounded interval
     */
    public IntPartition(String column, Long start, Long end) {
        this(column, start, end, false);
    }

    @Override
    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        if (quoteString == null) {
            throw new RuntimeException("Quote string cannot be null");
        }

        return RangePartitionsFormatter.generateRangeConstraint(
            quoteString + column + quoteString,
            (String[])Stream.of(boundaries).map(b -> b == null ? null : b.toString()).toArray(),
            new boolean[]{
                !(boundaries[0] == null && !isBoundedBoundaryIncluded),
                !(boundaries[1] == null && !isBoundedBoundaryIncluded)
            }
        );
    }

    /**
     * Getter
     */
    public Long[] getBoundaries() {
        return boundaries;
    }
}
