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

import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.jdbc.utils.ByteUtil;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
    private boolean rangeLeftInfinite = false;
    private boolean rangeRightInfinite = false;
    private PartitionType partitionType;
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
            partitionType = PartitionType.typeOf(
                context.getOption("PARTITION_BY").split(":")[1]
            );
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
                range = rangeStr.split(":", 4);
                if (range.length == 1) {
                    throw new IllegalArgumentException("The parameter 'RANGE' must specify ':<end_value>' for this PARTITION_BY type");
                }
                if (range.length == 2) {
                    // This is normal partition, do nothing. If syntax is incorrect, it causes NumberFormatException later
                }
                else if (range.length == 3) {
                    if (range[0].isEmpty()) {
                        rangeLeftInfinite = true;
                        range = new String[] {range[1], range[2]};
                    }
                    else if (range[2].isEmpty()) {
                        rangeRightInfinite = true;
                        range = new String[] {range[0], range[1]};
                    }
                    else {
                        throw new IllegalArgumentException("The parameter 'RANGE' has incorrect syntax. To define non-enclosed partition, put ':' before the left limit value or after the right limit value");
                    }
                }
                else {  // range.length == 4
                    rangeLeftInfinite = rangeRightInfinite = true;
                    range = new String[] {range[1], range[2]};
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
        }

        // INTERVAL

        String intervalStr = context.getOption("INTERVAL");
        if (intervalStr != null) {
            String[] interval = intervalStr.split(":");
            try {
                intervalNum = Long.parseLong(interval[0]);
                if (intervalNum < 1) {
                    throw new IllegalArgumentException("The '<interval_num>' in parameter 'INTERVAL' must be at least 1, but actual is " + intervalNum);
                }
            }
            catch (NumberFormatException ex) {
                throw new IllegalArgumentException("The '<interval_num>' in parameter 'INTERVAL' must be an integer");
            }

            // Intervals of type DATE
            if (interval.length > 1) {
                intervalType = IntervalType.typeOf(interval[1]);
            }
            if (interval.length == 1 && partitionType == PartitionType.DATE) {
                throw new IllegalArgumentException("The parameter 'INTERVAL' must specify unit (':year|month|day') for the PARTITION_TYPE = 'DATE'");
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
     * getFragments() implementation
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
                if (rangeLeftInfinite) {
                    fragments.add(createFragment(Long.MAX_VALUE, rangeDateStart.getTimeInMillis()));
                }

                Calendar fragStart = rangeDateStart;
                while (fragStart.before(rangeDateEnd)) {
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
                    }
                    if (fragEnd.after(rangeDateEnd)) {
                        fragEnd = (Calendar)rangeDateEnd.clone();
                    }

                    // Add the fragment to the list
                    fragments.add(createFragment(fragStart.getTimeInMillis(), fragEnd.getTimeInMillis()));

                    // Prepare for the next fragment
                    fragStart = fragEnd;
                }

                if (rangeRightInfinite) {
                    fragments.add(createFragment(rangeDateEnd.getTimeInMillis(), Long.MIN_VALUE));
                }
                break;
            }
            case INT: {
                if (rangeLeftInfinite) {
                    fragments.add(createFragment(Long.MAX_VALUE, rangeIntStart));
                }

                long fragStart = rangeIntStart;
                while (fragStart < rangeIntEnd) {
                    // Calculate a new fragment
                    long fragEnd = fragStart + intervalNum;
                    if (fragEnd > rangeIntEnd) {
                        fragEnd = rangeIntEnd;
                    }

                    // Add the fragment to the list
                    fragments.add(createFragment(fragStart, fragEnd));

                    // Prepare for the next fragment
                    fragStart = fragEnd;
                }

                if (rangeRightInfinite) {
                    fragments.add(createFragment(rangeIntEnd, Long.MIN_VALUE));
                }
                break;
            }
            case ENUM: {
                for (String frag : range) {
                    fragments.add(createFragment(frag.getBytes()));
                }
                break;
            }
        }

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

    /**
     * Create {@link Fragment} from two long values.
     * Incapsulates {@link ByteUtil} calls.
     * @param start
     * @param end
     * @return {@link Fragment}
     */
    private Fragment createFragment(long start, long end) {
        return createFragment(ByteUtil.mergeBytes(ByteUtil.getBytes(start), ByteUtil.getBytes(end)));
    }
}
