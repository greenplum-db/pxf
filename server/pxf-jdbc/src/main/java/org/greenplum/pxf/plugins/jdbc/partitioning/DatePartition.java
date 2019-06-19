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
import org.greenplum.pxf.plugins.jdbc.IntervalType;
import org.greenplum.pxf.plugins.jdbc.partitioning.Partition;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

class DatePartition extends BasePartition implements JdbcFragmentMetadata {
    private static final long serialVersionUID = 0L;

    private final Date[] boundaries;

    /**
     * @return a type of {@link Partition} which is the caller of {@link DatePartition#generate} function.
     * Currently, only used for exception messages.
     */
    public static Partition type() {
        return Partition.DATE;
    }

    /**
     * Generate an array of {@link IntPartition}s using the provided column name, RANGE and INTERVAL string values
     * @param column
     * @param range
     * @param interval
     * @return an array of properly initialized {@link IntPartition} objects
     */
    public static List<DatePartition> generate(String column, String range, String interval) {
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
        Calendar rangeStart;
        Calendar rangeEnd;
        {
            String[] rangeBoundaries = range.split(":", 2);
            if (rangeBoundaries.length != 2) {
                throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' has incorrect format. The correct format for partition of type '%s' is '<start_value>:<end_value>'", type()
                ));
            }

            try {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                rangeStart = Calendar.getInstance();
                rangeStart.setTime(df.parse(rangeBoundaries[0]));
                rangeEnd = Calendar.getInstance();
                rangeEnd.setTime(df.parse(rangeBoundaries[1]));
            }
            catch (ParseException e) {
                throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' has invalid date format. The correct format for partition of type '%s' is 'yyyy-MM-dd'", type()
                ));
            }

            if (rangeEnd.before(rangeStart)) {
                throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' is invalid. The <end_value> '%s' must be bigger than the <start_value> '%s'", rangeStart, rangeEnd
                ));
            }
        }

        // Parse INTERVAL
        int intervalNum;
        IntervalType intervalType;
        {
            String[] intervalSplit = interval.split(":");
            if (intervalSplit.length != 2) {
                throw new IllegalArgumentException(String.format(
                    "The parameter 'INTERVAL' has invalid format. The correct format for partition of type '%s' is '<interval_num>:{year|month|day}'", type()
                ));
            }

            try {
                intervalNum = Integer.parseInt(intervalSplit[0]);
            }
            catch (NumberFormatException ex) {
                throw new IllegalArgumentException(String.format(
                    "The '<interval_num>' in parameter 'INTERVAL' must be an integer for partition of type '%s'", type()
                ));
            }
            if (intervalNum < 1) {
                throw new IllegalArgumentException("The '<interval_num>' in parameter 'INTERVAL' must be at least 1, but actual is " + intervalNum);
            }

            intervalType = IntervalType.typeOf(intervalSplit[1]);
        }

        // Generate partitions
        List<DatePartition> partitions = new LinkedList<>();
        {
            partitions.add(new DatePartition(column, null, rangeStart));

            boolean isLeftBoundedPartitionAdded = false;
            Calendar fragStart = rangeStart;
            do {
                // Calculate a new fragment
                Calendar fragEnd = (Calendar)fragStart.clone();
                switch (intervalType) {
                    case DAY:
                        fragEnd.add(Calendar.DAY_OF_MONTH, intervalNum);
                        break;
                    case MONTH:
                        fragEnd.add(Calendar.MONTH, intervalNum);
                        break;
                    case YEAR:
                        fragEnd.add(Calendar.YEAR, intervalNum);
                        break;
                    default:
                        throw new RuntimeException("Unknown INTERVAL type");
                }

                if (fragEnd.compareTo(rangeEnd) <= 0) {
                    partitions.add(new DatePartition(column, fragStart, fragEnd));
                }
                else {  // fragEnd.compareTo(rangeEnd) > 0
                    partitions.add(new DatePartition(column, fragStart, null));
                    isLeftBoundedPartitionAdded = true;
                }

                fragStart = fragEnd;
            }
            while (fragStart.before(rangeEnd));

            if (!isLeftBoundedPartitionAdded) {
                partitions.add(new DatePartition(column, rangeEnd, null));
            }
        }

        return partitions;
    }

    /**
     * Construct a DatePartition covering a range of values from 'start' to 'end'
     * @param column
     * @param start null for right-bounded interval
     * @param end null for left-bounded interval
     */
    public DatePartition(String column, Calendar start, Calendar end) {
        super(column);
        if (start == null && end == null) {
            throw new RuntimeException("Both boundaries cannot be null");
        }
        if (start != null && start.equals(end)) {
            throw new RuntimeException(String.format(
                "Boundaries cannot be equal for partition of type '%s'", type()
            ));
        }

        this.boundaries = new Date[]{
            start == null ? null : new Date(start.getTimeInMillis()),
            end == null ? null : new Date(end.getTimeInMillis())
        };
    }

    @Override
    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        if (quoteString == null) {
            throw new RuntimeException("Quote string cannot be null");
        }
        if (dbProduct == null) {
            throw new RuntimeException(String.format(
                "DbProduct cannot be null for partitions of type '%s'", type()
            ));
        }
        return RangePartitionsFormatter.generateRangeConstraint(
            quoteString + column + quoteString,
            (String[])Stream.of(boundaries).map(b -> b == null ? null : dbProduct.wrapDate(b)).toArray(),
            new boolean[]{true, false}
        );
    }

    /**
     * Getter
     */
    public Date[] getBoundaries() {
        return boundaries;
    }
}
