package org.greenplum.pxf.plugins.ignite;

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

import org.apache.commons.compress.utils.ByteUtils;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * PXF-Ignite fragmenter class
 *
 * This fragmenter works just like the one in PXF JDBC plugin
 */
public class IgnitePartitionFragmenter extends BaseFragmenter {

    private static final Logger LOG = LoggerFactory.getLogger(IgnitePartitionFragmenter.class);
    /**
     * Insert partition constraints into the prepared SQL query.
     *
     * @param requestContext pre-validated PXF RequestContext
     * @param sb the SQL query that is prepared for appending extra WHERE constraints.
     */
    public static void buildFragmenterSql(RequestContext requestContext, StringBuilder sb) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("buildFragmenterSql() called");
        }

        if (requestContext.getOption("PARTITION_BY") == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("buildFragmenterSql(): Partition is not used");
            }
            return;
        }

        byte[] meta = requestContext.getFragmentMetadata();
        if (meta == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("buildFragmenterSql(): Fragment metadata is null, no partition constraints added");
            }
            return;
        }

        // Note that these parameters have already been validated when constructing fragment.
        String[] partitionBy = requestContext.getOption("PARTITION_BY").split(":");
        String partitionColumn = partitionBy[0];
        PartitionType partitionType = PartitionType.typeOf(partitionBy[1]);

        if (!sb.toString().contains("WHERE")) {
            sb.append(" WHERE ");
        }
        else {
            sb.append(" AND ");
        }

        switch (partitionType) {
            case DATE: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("buildFragmenterSql(): DATE partition found");
                }

                // Get fragment metadata
                Date fragStart = new Date(ByteUtils.fromLittleEndian(meta, 0, 8));
                Date fragEnd = new Date(ByteUtils.fromLittleEndian(meta, 8, 8));

                // Add constraints
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                sb.append(partitionColumn).append(">=").append("'" + df.format(fragStart) + "'");
                sb.append(" AND ");
                sb.append(partitionColumn).append("<").append("'" + df.format(fragEnd) + "'");

                if (LOG.isDebugEnabled()) {
                    LOG.debug("buildFragmenterSql(): DATE partition constraints added");
                }
                break;
            }
            case INT: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("buildFragmenterSql(): INT partition found");
                }

                // Get fragment metadata
                int fragStart = (int)ByteUtils.fromLittleEndian(meta, 0, 4);
                int fragEnd = (int)ByteUtils.fromLittleEndian(meta, 4, 4);

                // Add constraints
                sb.append(partitionColumn).append(">=").append(fragStart);
                sb.append(" AND ");
                sb.append(partitionColumn).append("<").append(fragEnd);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("buildFragmenterSql(): INT partition constraints added");
                }
                break;
            }
            case ENUM: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("buildFragmenterSql(): ENUM partition found");
                }

                // Add constraints. Fragment metadata should not be parsed.
                sb.append(partitionColumn).append("='").append(new String(meta)).append("'");

                if (LOG.isDebugEnabled()) {
                    LOG.debug("buildFragmenterSql(): ENUM partition constraints added");
                }
                break;
            }
        }
    }

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        LOG.debug("Initializer started");

        if (requestContext.getOption("PARTITION_BY") == null) {
            LOG.debug("Initializer successful; partition was not used");
            return;
        }

        try {
            partitionBy = requestContext.getOption("PARTITION_BY").split(":");
            partitionType = PartitionType.typeOf(partitionBy[1]);
        }
        catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e1) {
            throw new IllegalArgumentException("The parameter 'PARTITION_BY' is invalid. The pattern is 'column_name:DATE|INT|ENUM'");
        }

        // Parse and validate parameter-RANGE

        String rangeStr = requestContext.getOption("RANGE");
        if (rangeStr != null) {
            range = rangeStr.split(":");
            if (range.length == 1 && partitionType != PartitionType.ENUM) {
                throw new IllegalArgumentException("The parameter 'RANGE' does not specify '[:end_value]'");
            }
        }
        else {
            throw new IllegalArgumentException("The parameter 'RANGE' must be specified along with 'PARTITION_BY'");
        }

        // Parse and validate parameter-INTERVAL
        String intervalStr = requestContext.getOption("INTERVAL");
        if (intervalStr != null) {
            interval = intervalStr.split(":");
            intervalNum = Integer.parseInt(interval[0]);
            if (interval.length > 1) {
                intervalType = IntervalType.typeOf(interval[1]);
            }
            if (interval.length == 1 && partitionType == PartitionType.DATE) {
                throw new IllegalArgumentException("The parameter 'INTERVAL' does not specify unit [:year|month|day]");
            }
        }
        else if (partitionType != PartitionType.ENUM) {
            throw new IllegalArgumentException("The parameter 'INTERVAL' must be specified along with 'PARTITION_BY'");
        }
        if (intervalNum < 1) {
            throw new IllegalArgumentException("The parameter 'INTERVAL' must be at least 1. The actual is '" + intervalNum + "'");
        }

        // Parse date partition
        try {
            if (partitionType == PartitionType.DATE) {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                rangeStart = Calendar.getInstance();
                rangeStart.setTime(df.parse(range[0]));
                rangeEnd = Calendar.getInstance();
                rangeEnd.setTime(df.parse(range[1]));
            }
        } catch (ParseException e) {
            throw new IllegalArgumentException("The parameter 'RANGE' has invalid date format. Expected format is 'yyyy-MM-dd'");
        }

        LOG.debug("Initializer successful; some partition used");
    }

    /**
     * Returns statistics for the Ignite table. This is not implemented in the current version
     * @throws UnsupportedOperationException when operation is not supported
     */
    @Override
    public FragmentStats getFragmentStats() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("ANALYZE for Ignite plugin is not supported");
    }

    /**
     * Returns list of fragments for Ignite table queries
     *
     * @throws UnsupportedOperationException if a partition of unknown type was found
     *
     * @return a list of fragments
     */
    @Override
    public List<Fragment> getFragments() throws UnsupportedOperationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getFragments() called; dataSource is '" + context.getDataSource() + "'");
        }

        byte[] fragmentMetadata = null;
        byte[] fragmentUserdata = null;

        if (partitionType == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getFragments() found no partition");
            }
            Fragment fragment = new Fragment(context.getDataSource(), replicaHostAddressWrapped, fragmentMetadata, fragmentUserdata);
            fragments.add(fragment);
            if (LOG.isDebugEnabled()) {
                LOG.debug("getFragments() successful");
            }
            return fragments;
        }

        switch (partitionType) {
            case DATE: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getFragments() found DATE partition");
                }
                int currInterval = intervalNum;

                Calendar fragStart = rangeStart;
                while (fragStart.before(rangeEnd)) {
                    Calendar fragEnd = (Calendar) fragStart.clone();
                    switch (intervalType) {
                        case DAY:
                            fragEnd.add(Calendar.DAY_OF_MONTH, currInterval);
                            break;
                        case MONTH:
                            fragEnd.add(Calendar.MONTH, currInterval);
                            break;
                        case YEAR:
                            fragEnd.add(Calendar.YEAR, currInterval);
                            break;
                    }
                    if (fragEnd.after(rangeEnd))
                        fragEnd = (Calendar) rangeEnd.clone();

                    fragmentMetadata = new byte[16];
                    ByteUtils.toLittleEndian(fragmentMetadata, fragStart.getTimeInMillis(), 0, 8);
                    ByteUtils.toLittleEndian(fragmentMetadata, fragEnd.getTimeInMillis(), 8, 8);
                    Fragment fragment = new Fragment(context.getDataSource(), replicaHostAddressWrapped, fragmentMetadata, fragmentUserdata);

                    fragments.add(fragment);

                    // Continue the previous fragment
                    fragStart = fragEnd;
                }
                break;
            }
            case INT: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getFragments() found INT partition");
                }
                int rangeStart = Integer.parseInt(range[0]);
                int rangeEnd = Integer.parseInt(range[1]);
                int currInterval = intervalNum;

                int fragStart = rangeStart;
                while (fragStart < rangeEnd) {
                    int fragEnd = fragStart + currInterval;
                    if (fragEnd > rangeEnd) {
                        fragEnd = rangeEnd;
                    }

                    fragmentMetadata = new byte[8];
                    ByteUtils.toLittleEndian(fragmentMetadata, fragStart, 0, 4);
                    ByteUtils.toLittleEndian(fragmentMetadata, fragEnd, 4, 4);
                    Fragment fragment = new Fragment(context.getDataSource(), replicaHostAddressWrapped, fragmentMetadata, fragmentUserdata);

                    fragments.add(fragment);

                    // Continue the previous fragment
                    fragStart = fragEnd;
                }
                break;
            }
            case ENUM: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getFragments() found ENUM partition");
                }
                for (String frag : range) {
                    fragmentMetadata = frag.getBytes();
                    Fragment fragment = new Fragment(context.getDataSource(), replicaHostAddressWrapped, fragmentMetadata, fragmentUserdata);
                    fragments.add(fragment);
                }
                break;
            }
            default: {
                throw new UnsupportedOperationException("getFragments() found a partition of unknown type and failed");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("getFragments() successful");
        }
        return fragments;
    }


    /**
     * Partition type
     */
    private static enum PartitionType {
        DATE,
        INT,
        ENUM;

        public static PartitionType typeOf(String str) {
            return valueOf(str.toUpperCase());
        }
    }
    /**
     * Partition interval, for 'partitionType == DATE'
     */
    private static enum IntervalType {
        DAY,
        MONTH,
        YEAR;

        public static IntervalType typeOf(String str) {
            return valueOf(str.toUpperCase());
        }
    }

    /**
     * The replica holder. This is an address of a *PXF* host that processes fragments.
     * It is always 'localhost'
     */
    private static final String[] replicaHostAddressWrapped;
    static {
        String[] temp;
        try {
            temp = new String[]{InetAddress.getLocalHost().getHostAddress()};
        }
        catch (UnknownHostException e) {
            // In fact, 'localhost' can always be obtained
            temp = new String[]{"localhost"};
        }
        replicaHostAddressWrapped = temp;
    }

    private String[] partitionBy = null;
    private String[] range = null;
    private String[] interval = null;
    private PartitionType partitionType = null;
    private IntervalType intervalType = null;
    private int intervalNum = 1;

    // Used only if 'partitionType' is 'DATE'
    private Calendar rangeStart = null;
    private Calendar rangeEnd = null;
}
