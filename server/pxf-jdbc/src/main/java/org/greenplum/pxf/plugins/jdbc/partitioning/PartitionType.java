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

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.plugins.jdbc.Interval;
import org.greenplum.pxf.plugins.jdbc.IntervalType;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * The high-level partitioning feature controller.
 * <p>
 * Encapsulates concrete classes implementing various types of partitions.
 */
public enum PartitionType {
    INT {
        @Override
        Object parseRange(String value) {
            return Long.parseLong(value);
        }

        @Override
        Interval parseInterval(String interval) {
            return new Interval.LongInterval(interval);
        }

        @Override
        boolean isLessThan(Object rangeStart, Object rangeEnd) {
            return (long) rangeStart < (long) rangeEnd;
        }

        @Override
        Object next(Object start, Object end, Interval interval) {
            return Math.min((long) start + interval.getValue(), (long) end);
        }

        @Override
        BasePartition createPartition(String column, Object start, Object end) {
            return new IntPartition(column, (Long) start, (Long) end);
        }

        @Override
        String getValidIntervalFormat() {
            return "Integer";
        }
    },
    DATE {
        @Override
        protected Object parseRange(String value) {
            return LocalDate.parse(value);
        }

        @Override
        Interval parseInterval(String interval) {
            return new Interval.DateInterval(interval);
        }

        @Override
        protected boolean isLessThan(Object rangeStart, Object rangeEnd) {
            return ((LocalDate) rangeStart).isBefore((LocalDate) rangeEnd);
        }

        @Override
        Object next(Object start, Object end, Interval interval) {
            ChronoUnit unit;
            if (interval.getType() == IntervalType.DAY) {
                unit = ChronoUnit.DAYS;
            } else if (interval.getType() == IntervalType.MONTH) {
                unit = ChronoUnit.MONTHS;
            } else if (interval.getType() == IntervalType.YEAR) {
                unit = ChronoUnit.YEARS;
            } else {
                throw new RuntimeException("Unknown INTERVAL type");
            }

            LocalDate next = ((LocalDate) start).plus(interval.getValue(), unit);
            return next.compareTo((LocalDate) end) > 0 ? end : next;
        }

        @Override
        BasePartition createPartition(String column, Object start, Object end) {
            return new DatePartition(column, (LocalDate) start, (LocalDate) end);
        }

        @Override
        String getValidIntervalFormat() {
            return "yyyy-mm-dd";
        }
    },
    ENUM {
        private static final String UNSUPPORTED_ERR_MESSAGE = "Current operation is not supported";

        @Override
        protected List<? extends BasePartition> generate(String column, String range, String interval) {
            // Parse RANGE
            String[] rangeValues = range.split(":");

            // Generate partitions
            List<BasePartition> partitions = new LinkedList<>();

            for (String rangeValue : rangeValues) {
                partitions.add(new EnumPartition(column, rangeValue));
            }

            // "excluded" values
            partitions.add(new EnumPartition(column, rangeValues));

            return partitions;
        }

        @Override
        protected boolean isIntervalMandatory() {
            return false;
        }

        @Override
        BasePartition createPartition(String column, Object start, Object end) {
            throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
        }

        @Override
        Object parseRange(String value) {
            throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
        }

        @Override
        Interval parseInterval(String interval) {
            throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
        }

        @Override
        boolean isLessThan(Object rangeStart, Object rangeEnd) {
            throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
        }

        @Override
        Object next(Object start, Object end, Interval interval) {
            throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
        }

        @Override
        String getValidIntervalFormat() {
            throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
        }
    };

    protected List<? extends BasePartition> generate(String column, String range, String interval) {
        String[] rangeBoundaries = range.split(":");
        if (rangeBoundaries.length != 2) {
            throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' has incorrect format. The correct format for partition of type '%s' is '<start_value>:<end_value>'", this
            ));
        }

        Object rangeStart, rangeEnd;
        try {
            rangeStart = parseRange(rangeBoundaries[0]);
            rangeEnd = parseRange(rangeBoundaries[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' is invalid. The correct format for partition of type '%s' is '%s'", this, getValidIntervalFormat()));
        }

        if (!isLessThan(rangeStart, rangeEnd)) {
            throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' is invalid. The <end_value> '%s' must be larger than the <start_value> '%s'", rangeStart, rangeEnd
            ));
        }

        Interval parsedInterval = parseInterval(interval);
        if (parsedInterval.getValue() < 1) {
            throw new IllegalArgumentException("The '<interval_num>' in parameter 'INTERVAL' must be at least 1, but actual is " + parsedInterval.getValue());
        }

        // Generate partitions
        List<BasePartition> partitions = new ArrayList<>();

        // create (-infinity to rangeStart) partition
        partitions.add(createPartition(column, null, rangeStart));

        // create (rangeEnd to infinity) partition
        partitions.add(createPartition(column, rangeEnd, null));

        Object fragmentEnd;
        Object fragmentStart = rangeStart;
        while (isLessThan(fragmentStart, rangeEnd)) {
            fragmentEnd = next(fragmentStart, rangeEnd, parsedInterval);
            partitions.add(createPartition(column, fragmentStart, fragmentEnd));
            fragmentStart = fragmentEnd;
        }

        return partitions;
    }

    abstract Object parseRange(String value) throws Exception;

    abstract boolean isLessThan(Object rangeStart, Object rangeEnd);

    abstract Interval parseInterval(String interval);

    abstract BasePartition createPartition(String column, Object start, Object end);

    /**
     * Return the start of the next partition
     * @param start the start of current partition
     * @param end the end of partition range
     * @param interval partition interval
     * @return min('end', 'start' + 'end')
     */
    abstract Object next(Object start, Object end, Interval interval);

    /**
     * @return valid format of interval for this partition type that can be provided to user
     */
    abstract String getValidIntervalFormat();

    /**
     * Analyze the user-provided parameters (column name, RANGE and INTERVAL values) and form a list of getFragmentsMetadata for this partition according to those parameters.
     *
     * @param column   the partition column name
     * @param range    RANGE string value
     * @param interval INTERVAL string value
     * @return a list of getFragmentsMetadata (of various concrete types)
     */
    public List<JdbcFragmentMetadata> getFragmentsMetadata(String column, String range, String interval) {
        checkValidInput(column, range, interval);

        List<JdbcFragmentMetadata> result = new LinkedList<>();
        result.addAll(generate(column, range, interval));
        result.add(new NullPartition(column));

        return result;
    }

    private void checkValidInput(String column, String range, String interval) {
        // Check input
        if (StringUtils.isBlank(column)) {
            throw new RuntimeException("The column name must be provided");
        }
        if (StringUtils.isBlank(range)) {
            throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' must be specified for partition of type '%s'", this
            ));
        }
        if (isIntervalMandatory() && StringUtils.isBlank(interval)) {
            throw new IllegalArgumentException(String.format(
                    "The parameter 'INTERVAL' must be specified for partition of type '%s'", this
            ));
        }
    }

    protected boolean isIntervalMandatory() {
        return true;
    }

    public static PartitionType of(String str) {
        return valueOf(str.toUpperCase());
    }
}
