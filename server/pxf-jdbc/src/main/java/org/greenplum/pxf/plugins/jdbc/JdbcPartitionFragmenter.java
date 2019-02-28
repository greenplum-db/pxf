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

import org.apache.hadoop.conf.Configuration;

/**
 * JDBC fragmenter
 *
 * Splits the query to allow multiple simultaneous SELECTs
 */
public class JdbcPartitionFragmenter extends BaseFragmenter {
    /**
     * Insert fragment constraints into the SQL query.
     *
     * @param context RequestContext of the fragment
     * @param dbName Database name (affects the behaviour for DATE partitions)
     * @param query SQL query to insert constraints to. The query may may contain other WHERE statements
     */
    public static void buildFragmenterSql(RequestContext context, Configuration configuration, String dbName, StringBuilder query) {
        byte[] meta = context.getFragmentMetadata();
        if (meta == null) {
            return;
        }

        String partitionByRaw = JdbcPluginSettings.partitionBy.loadFromContextOrConfiguration(context, configuration);
        if (partitionByRaw == null) {
            return;
        }
        String[] partitionBy = partitionByRaw.split(":");
        String partitionColumn = partitionBy[0];
        PartitionType partitionType = PartitionType.typeOf(partitionBy[1]);
        DbProduct dbProduct = DbProduct.getDbProduct(dbName);

        if (!query.toString().contains("WHERE")) {
            query.append(" WHERE ");
        }
        else {
            query.append(" AND ");
        }

        switch (partitionType) {
            case DATE: {
                byte[][] newb = ByteUtil.splitBytes(meta);
                Date fragStart = new Date(ByteUtil.toLong(newb[0]));
                Date fragEnd = new Date(ByteUtil.toLong(newb[1]));

                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                query.append(partitionColumn).append(" >= ").append(dbProduct.wrapDate(df.format(fragStart)));
                query.append(" AND ");
                query.append(partitionColumn).append(" < ").append(dbProduct.wrapDate(df.format(fragEnd)));

                break;
            }
            case INT: {
                byte[][] newb = ByteUtil.splitBytes(meta);
                long fragStart = ByteUtil.toLong(newb[0]);
                long fragEnd = ByteUtil.toLong(newb[1]);

                query.append(partitionColumn).append(" >= ").append(fragStart);
                query.append(" AND ");
                query.append(partitionColumn).append(" < ").append(fragEnd);
                break;
            }
            case ENUM: {
                query.append(partitionColumn).append(" = '").append(new String(meta)).append("'");
                break;
            }
        }
    }

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        String partitionByRaw = JdbcPluginSettings.partitionBy.loadFromContextOrConfiguration(context, configuration);
        if (partitionByRaw == null) {
            return;
        }

        // PARTITION_BY

        try {
            partitionType = PartitionType.typeOf(partitionByRaw.split(":")[1]);
        }
        catch (IllegalArgumentException | ArrayIndexOutOfBoundsException ex) {
            throw new IllegalArgumentException(JdbcPluginSettings.partitionBy + " is invalid. The pattern is '<column_name>:date|int|enum'");
        }

        // RANGE

        String rangeRaw = JdbcPluginSettings.partitionRange.loadFromContextOrConfiguration(context, configuration);
        if (rangeRaw != null) {
            range = rangeRaw.split(":");
            if (range.length == 1 && partitionType != PartitionType.ENUM) {
                throw new IllegalArgumentException(JdbcPluginSettings.partitionRange + " must specify ':<end_value>' for partition of type " + partitionType);
            }
        }
        else {
            throw new IllegalArgumentException(JdbcPluginSettings.partitionRange + " must be specified along with " + JdbcPluginSettings.partitionBy);
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
                throw new IllegalArgumentException(JdbcPluginSettings.partitionRange + " has invalid date format. The correct format is 'yyyy-MM-dd'");
            }
        }
        else if (partitionType == PartitionType.INT) {
            // Parse INT partition type values
            try {
                rangeIntStart = Long.parseLong(range[0]);
                rangeIntEnd = Long.parseLong(range[1]);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(JdbcPluginSettings.partitionRange + " is invalid. Both boundaries must be integers");
            }
        }

        // INTERVAL

        String intervalStr = JdbcPluginSettings.partitionInterval.loadFromContextOrConfiguration(context, configuration);
        if (intervalStr != null) {
            String[] interval = intervalStr.split(":");
            try {
                intervalNum = Long.parseLong(interval[0]);
                if (intervalNum < 1) {
                    throw new IllegalArgumentException("'<interval_num>' in " + JdbcPluginSettings.partitionInterval + " must be at least 1, but actual is " + intervalNum);
                }
            }
            catch (NumberFormatException ex) {
                throw new IllegalArgumentException("'<interval_num>' in " + JdbcPluginSettings.partitionInterval + " must be an integer");
            }

            // Intervals of type DATE
            if (interval.length > 1) {
                intervalType = IntervalType.typeOf(interval[1]);
            }
            if (interval.length == 1 && partitionType == PartitionType.DATE) {
                throw new IllegalArgumentException(JdbcPluginSettings.partitionInterval + " must specify unit (':year|month|day') for partition of type " + partitionType);
            }
        }
        else if (partitionType != PartitionType.ENUM) {
            throw new IllegalArgumentException(JdbcPluginSettings.partitionInterval + " must be specified along with " + JdbcPluginSettings.partitionBy + " for partition of type " + partitionType);
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
                    if (fragEnd.after(rangeDateEnd))
                        fragEnd = (Calendar)rangeDateEnd.clone();

                    // Convert to byte[]
                    byte[] msStart = ByteUtil.getBytes(fragStart.getTimeInMillis());
                    byte[] msEnd = ByteUtil.getBytes(fragEnd.getTimeInMillis());
                    byte[] fragmentMetadata = ByteUtil.mergeBytes(msStart, msEnd);

                    // Write fragment
                    Fragment fragment = new Fragment(context.getDataSource(), pxfHosts, fragmentMetadata);
                    fragments.add(fragment);

                    // Prepare for the next fragment
                    fragStart = fragEnd;
                }
                break;
            }
            case INT: {
                long fragStart = rangeIntStart;

                while (fragStart < rangeIntEnd) {
                    // Calculate a new fragment
                    long fragEnd = fragStart + intervalNum;
                    if (fragEnd > rangeIntEnd) {
                        fragEnd = rangeIntEnd;
                    }

                    // Convert to byte[]
                    byte[] bStart = ByteUtil.getBytes(fragStart);
                    byte[] bEnd = ByteUtil.getBytes(fragEnd);
                    byte[] fragmentMetadata = ByteUtil.mergeBytes(bStart, bEnd);

                    // Write fragment
                    Fragment fragment = new Fragment(context.getDataSource(), pxfHosts, fragmentMetadata);
                    fragments.add(fragment);

                    // Prepare for the next fragment
                    fragStart = fragEnd;
                }
                break;
            }
            case ENUM: {
                for (String frag : range) {
                    byte[] fragmentMetadata = frag.getBytes();
                    Fragment fragment = new Fragment(context.getDataSource(), pxfHosts, fragmentMetadata);
                    fragments.add(fragment);
                }
                break;
            }
        }

        return fragments;
    }

    // Partition parameters (filled by class constructor)
    private String[] range = null;
    private PartitionType partitionType = null;
    private long intervalNum;

    // Partition parameters for INT partitions (filled by class constructor)
    private long rangeIntStart;
    private long rangeIntEnd;

    // Partition parameters for DATE partitions (filled by class constructor)
    private IntervalType intervalType;
    private Calendar rangeDateStart;
    private Calendar rangeDateEnd;

    private static enum PartitionType {
        DATE,
        INT,
        ENUM;

        public static PartitionType typeOf(String str) {
            return valueOf(str.toUpperCase());
        }
    }

    private static enum IntervalType {
        DAY,
        MONTH,
        YEAR;

        public static IntervalType typeOf(String str) {
            return valueOf(str.toUpperCase());
        }
    }

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
}
