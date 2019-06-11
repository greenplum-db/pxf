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

public class IntPartition implements JdbcFragmentMetadata, Serializable {
    private static final long serialVersionUID = 1L;

    private final String column;
    private final Long[] boundaries;

    /**
     * Construct an IntPartition with given column and boundaries
     * @param column
     * @param boundaries
     */
    private IntPartition(String column, Long[] boundaries) {
        assert column != null;
        assert boundaries.length == 2;
        assert boundaries[0] != null || boundaries[1] != null;

        this.column = column;

        if (boundaries[0] != null && boundaries[1] != null && boundaries[0].equals(boundaries[1])) {
            // Use equality instead of two comparation constraints
            boundaries = new Long[]{boundaries[0]};
        }
        this.boundaries = boundaries;
    }

    /**
     * Construct an IntPartition covering an inclusive range of values from 'start' to 'end'
     * @param column
     * @param start null for right-bounded interval
     * @param end null for right-bounded interval
     */
    public IntPartition(String column, Long start, Long end) {
        this(column, new Long[]{start, end});
    }

    @Override
    public String getColumn() {
        return column;
    }

    @Override
    public PartitionType getType() {
        return PartitionType.INT;
    }

    @Override
    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        assert quoteString != null;

        StringBuilder sb = new StringBuilder();

        String columnQuoted = quoteString + column + quoteString;

        if (boundaries.length == 1) {
            sb.append(columnQuoted).append(" = ").append(boundaries[0]);
        }
        else {
            if (boundaries[0] == null) {
                sb.append(columnQuoted).append(" < ").append(boundaries[1]);
            }
            else if (boundaries[1] == null) {
                sb.append(columnQuoted).append(" > ").append(boundaries[0]);
            }
            else {
                sb.append(columnQuoted).append(" >= ").append(boundaries[0]);
                sb.append(" AND ");
                sb.append(columnQuoted).append(" <= ").append(boundaries[1]);
            }
        }

        return sb.toString();
    }

    public Long[] getBoundaries() {
        return boundaries;
    }
}
