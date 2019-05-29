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
import org.greenplum.pxf.plugins.jdbc.partitioning.PartitionType;

import java.io.Serializable;
import java.sql.Date;
import java.util.Calendar;

public class DatePartition implements JdbcFragmentMetadata, Serializable {
    private static final long serialVersionUID = 1L;

    private final String column;
    private final Date[] boundaries;
    private final boolean isRightClosed;

    /**
     * Construct a DatePartition with given column, boundaries and right-closedness
     * @param column
     * @param boundaries
     * @param isRightClosed
     */
    private DatePartition(String column, Calendar[] boundaries, boolean isRightClosed) {
        // API checks
        assert column != null;
        assert boundaries.length <= 2;
        assert boundaries.length == 2 ?
            (boundaries[0] != null || boundaries[1] != null) :
            (boundaries[0] != null);

        this.column = column;

        if (boundaries.length == 2 && boundaries[0] == boundaries[1]) {
            // Use equality instead of two comparation constraints
            boundaries = new Calendar[]{boundaries[0]};
            isRightClosed = true;
        }
        this.boundaries = new Date[boundaries.length];
        for (int i = 0; i < boundaries.length; i++) {
            this.boundaries[i] = boundaries[i] == null ? null : new Date(boundaries[i].getTimeInMillis());
        }

        this.isRightClosed = isRightClosed;
    }

    /**
     * Construct a DatePartition covering a range of values from 'start' to 'end'.
     * 'start' is always included in the partition (partition is always left-closed)
     * @param column
     * @param start null for right-bounded interval
     * @param end null for left-bounded interval
     * @param includeEnd true if this partition must be right-closed
     */
    public DatePartition(String column, Calendar start, Calendar end, boolean includeEnd) {
        this(column, new Calendar[]{start, end}, includeEnd);
    }

    /**
     * Construct a DatePartition covering a range of values from 'start' to 'end'
     * @param column
     * @param start null for right-bounded interval
     * @param end null for left-bounded interval
     */
    public DatePartition(String column, Calendar start, Calendar end) {
        this(column, start, end, false);
    }

    @Override
    public String getColumn() {
        return column;
    }

    @Override
    public PartitionType getType() {
        return PartitionType.DATE;
    }

    @Override
    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        StringBuilder sb = new StringBuilder();

        String columnQuoted = quoteString + column + quoteString;

        if (boundaries.length == 1) {
            sb.append(columnQuoted).append(" = ").append(dbProduct.wrapDate(boundaries[0]));
        }
        else {
            if (boundaries[0] == null) {
                sb.append(columnQuoted).append(" < ").append(dbProduct.wrapDate(boundaries[1]));
            }
            else if (boundaries[1] == null) {
                sb.append(columnQuoted).append(" > ").append(dbProduct.wrapDate(boundaries[0]));
            }
            else {
                sb.append(columnQuoted).append(" >= ").append(dbProduct.wrapDate(boundaries[0]));
                sb.append(" AND ");
                sb.append(columnQuoted);
                if (isRightClosed) {
                    sb.append(" <= ");
                }
                else {
                    sb.append(" < ");
                }
                sb.append(dbProduct.wrapDate(boundaries[1]));
            }
        }

        return sb.toString();
    }

    public Date[] getBoundaries() {
        return boundaries;
    }
}
