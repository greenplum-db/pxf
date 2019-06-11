package org.greenplum.pxf.plugins.jdbc;

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

import org.apache.commons.lang.SerializationUtils;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.jdbc.partitioning.DatePartition;
import org.greenplum.pxf.plugins.jdbc.partitioning.EnumPartition;
import org.greenplum.pxf.plugins.jdbc.partitioning.IntPartition;
import org.greenplum.pxf.plugins.jdbc.partitioning.NullPartition;
import org.greenplum.pxf.plugins.jdbc.partitioning.PartitionType;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

/**
 * JDBC fragmenter
 *
 * Splits the query to allow multiple simultaneous SELECTs
 */
public class JdbcPartitionFragmenter extends BaseFragmenter {

    // A PXF engine to use as a host for fragments
    private static final String[] pxfHosts;

    static {
        String[] localhost = {"localhost"};
        try {
            localhost[0] = InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException ex) {
            // It is always possible to get 'localhost' address
        }
        pxfHosts = localhost;
    }

    // Partition parameters (filled by class constructor)
    private String[] range = null;
    private PartitionType partitionType;
    private String partitionColumn;
    private long intervalNum;

    // Partition parameters for INT partitions (filled by class constructor)
    private long rangeIntStart;
    private long rangeIntEnd;

    // Partition parameters for DATE partitions (filled by class constructor)
    private IntervalType intervalType;
    private Calendar rangeDateStart;
    private Calendar rangeDateEnd;

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        if (context.getOption("PARTITION_BY") == null) {
            return;
        }

        // PARTITION_BY
        try {
            String partitionBy[] = context.getOption("PARTITION_BY").split(":");
            partitionColumn = partitionBy[0];
            partitionType = PartitionType.typeOf(partitionBy[1]);
        }
        catch (IllegalArgumentException | ArrayIndexOutOfBoundsException ex) {
            throw new IllegalArgumentException("The parameter 'PARTITION_BY' is invalid. The pattern is '<column_name>:date|int|enum'");
        }

        // RANGE

        String rangeStr = context.getOption("RANGE");
        if (rangeStr != null) {
            if (partitionType == PartitionType.ENUM) {
                range = rangeStr.split(":");
            }
            else {
                range = rangeStr.split(":", 2);
                if (range.length != 2) {
                    throw new IllegalArgumentException("The parameter 'RANGE' has incorrect format. The correct format for this 'PARTITION_BY' type is '<start_value>:<end_value>'");
                }
            }
        }
        else {
            throw new IllegalArgumentException("The parameter 'RANGE' must be specified along with 'PARTITION_BY'");
        }

        if (partitionType == PartitionType.DATE) {
            // Parse DATE partition type values
            try {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                rangeDateStart = Calendar.getInstance();
                rangeDateStart.setTime(df.parse(range[0]));
                rangeDateEnd = Calendar.getInstance();
                rangeDateEnd.setTime(df.parse(range[1]));
            }
            catch (ParseException e) {
                throw new IllegalArgumentException("The parameter 'RANGE' has invalid date format. The correct format is 'yyyy-MM-dd'");
            }
            if (rangeDateEnd.before(rangeDateStart)) {
                throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' is invalid. The <end_value> '%s' must be bigger than the <start_value> '%s'", rangeDateStart, rangeDateEnd
                ));
            }
        }
        else if (partitionType == PartitionType.INT) {
            // Parse INT partition type values
            try {
                rangeIntStart = Long.parseLong(range[0]);
                rangeIntEnd = Long.parseLong(range[1]);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("The parameter 'RANGE' is invalid. Both range boundaries must be integers");
            }
            if (rangeIntEnd < rangeIntStart) {
                throw new IllegalArgumentException(String.format(
                    "The parameter 'RANGE' is invalid. The <end_value> '%s' must be bigger than the <start_value> '%s'", rangeIntEnd, rangeIntStart
                ));
            }
        }

        // INTERVAL

        String intervalStr = context.getOption("INTERVAL");
        if (intervalStr != null) {
            String[] interval = intervalStr.split(":");
            try {
                intervalNum = Long.parseLong(interval[0]);
            }
            catch (NumberFormatException ex) {
                throw new IllegalArgumentException("The '<interval_num>' in parameter 'INTERVAL' must be an integer");
            }
            if (intervalNum < 1) {
                throw new IllegalArgumentException("The '<interval_num>' in parameter 'INTERVAL' must be at least 1, but actual is " + intervalNum);
            }


            // Intervals of type DATE
            if (partitionType == PartitionType.DATE) {
                if (interval.length != 2) {
                    throw new IllegalArgumentException("The parameter 'INTERVAL' must specify unit (':year|month|day') for the PARTITION_TYPE = 'DATE'");
                }
                intervalType = IntervalType.typeOf(interval[1]);
            }
        }
        else if (partitionType != PartitionType.ENUM) {
            throw new IllegalArgumentException("The parameter 'INTERVAL' must be specified along with 'PARTITION_BY' for this PARTITION_TYPE");
        }
    }

    /**
     * @throws UnsupportedOperationException ANALYZE for Jdbc plugin is not supported
     *
     * @return fragment stats
     */
    @Override
    public FragmentStats getFragmentStats() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("ANALYZE for JDBC plugin is not supported");
    }

    /**
     * getFragments() implementation.
     * Note that all partition parameters must be verified before calling this procedure.
     *
     * @return a list of fragments to be passed to PXF segments
     */
    @Override
    public List<Fragment> getFragments() {
        if (partitionType == null) {
            // No partition case
            Fragment fragment = new Fragment(context.getDataSource(), pxfHosts, null);
            fragments.add(fragment);
            return fragments;
        }

        switch (partitionType) {
            case DATE: {
                // Right-bounded interval
                fragments.add(createFragment(SerializationUtils.serialize(
                    new DatePartition(partitionColumn, null, rangeDateStart)
                )));

                Calendar fragStart = rangeDateStart;
                do {
                    // Calculate a new fragment
                    Calendar fragEnd = (Calendar)fragStart.clone();
                    switch (intervalType) {
                        case DAY:
                            fragEnd.add(Calendar.DAY_OF_MONTH, (int)intervalNum);
                            break;
                        case MONTH:
                            fragEnd.add(Calendar.MONTH, (int)intervalNum);
                            break;
                        case YEAR:
                            fragEnd.add(Calendar.YEAR, (int)intervalNum);
                            break;
                        default:
                            throw new RuntimeException("Unknown INTERVAL type");
                    }

                    if (fragEnd.compareTo(rangeDateEnd) >= 0) {
                        // This is the last fragment, include the right boundary
                        fragEnd = (Calendar)rangeDateEnd.clone();
                        fragments.add(createFragment(SerializationUtils.serialize(
                            new DatePartition(partitionColumn, fragStart, fragEnd, true)
                        )));
                    }
                    else {
                        fragments.add(createFragment(SerializationUtils.serialize(
                            new DatePartition(partitionColumn, fragStart, fragEnd)
                        )));
                    }

                    // Prepare for the next fragment
                    fragStart = fragEnd;
                }
                while (fragStart.before(rangeDateEnd));

                // Left-bounded interval
                fragments.add(createFragment(SerializationUtils.serialize(
                    new DatePartition(partitionColumn, rangeDateEnd, null)
                )));
                break;
            }
            case INT: {
                // Right-bounded interval
                fragments.add(createFragment(SerializationUtils.serialize(
                    new IntPartition(partitionColumn, null, rangeIntStart)
                )));

                long fragStart = rangeIntStart;
                while (fragStart <= rangeIntEnd) {
                    // Calculate a new fragment
                    long fragStartNext = fragStart + intervalNum;
                    long fragEnd = fragStartNext - 1;
                    if (fragEnd > rangeIntEnd) {
                        fragEnd = rangeIntEnd;
                    }

                    // Add the fragment to the list
                    fragments.add(createFragment(SerializationUtils.serialize(
                        new IntPartition(partitionColumn, fragStart, fragEnd)
                    )));

                    // Prepare for the next fragment
                    fragStart = fragStartNext;
                }

                // Left-bounded interval
                fragments.add(createFragment(SerializationUtils.serialize(
                    new IntPartition(partitionColumn, rangeIntEnd, null)
                )));
                break;
            }
            case ENUM: {
                for (String enumValue : range) {
                    fragments.add(createFragment(SerializationUtils.serialize(
                        new EnumPartition(partitionColumn, enumValue)
                    )));
                }

                // "excluded" values
                fragments.add(createFragment(SerializationUtils.serialize(
                    new EnumPartition(partitionColumn, range)
                )));

                break;
            }
            case NULL: {
                // NOT NULL values
                fragments.add(createFragment(SerializationUtils.serialize(
                    new NullPartition(partitionColumn, false)
                )));
                break;
            }
        }

        // NULL values
        fragments.add(createFragment(SerializationUtils.serialize(
            new NullPartition(partitionColumn)
        )));

        return fragments;
    }

    /**
     * Create {@link Fragment} from byte array.
     * @param fragmentMetadata
     * @return {@link Fragment}
     */
    private Fragment createFragment(byte[] fragmentMetadata) {
        return new Fragment(context.getDataSource(), pxfHosts, fragmentMetadata);
    }
}
