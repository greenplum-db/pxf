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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.sql.Date;
import java.time.LocalDate;
import java.util.stream.Stream;

class DatePartition extends BasePartition implements JdbcFragmentMetadata {

    @Getter
    private final Date[] boundaries;

    /**
     * Construct a DatePartition covering a range of values from 'start' to 'end'
     *
     * @param column the partitioned column
     * @param start  null for right-bounded interval
     * @param end    null for left-bounded interval
     */
    public DatePartition(String column, LocalDate start, LocalDate end) {
        this(column, new Date[]{
                start == null ? null : Date.valueOf(start),
                end == null ? null : Date.valueOf(end)
        });
        if (start == null && end == null) {
            throw new RuntimeException("Both boundaries cannot be null");
        }
        if (start != null && start.equals(end)) {
            throw new RuntimeException(String.format(
                    "Boundaries cannot be equal for partition of type '%s'", PartitionType.DATE
            ));
        }
    }

    @JsonCreator
    public DatePartition(@JsonProperty("column") String column,
                         @JsonProperty("boundaries") Date[] boundaries) {
        super(column);
        this.boundaries = boundaries;
    }

    @Override
    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        if (quoteString == null) {
            throw new RuntimeException("Quote string cannot be null");
        }
        if (dbProduct == null) {
            throw new RuntimeException(String.format(
                    "DbProduct cannot be null for partitions of type '%s'", PartitionType.DATE
            ));
        }

        return generateRangeConstraint(
                quoteString + column + quoteString,
                Stream.of(boundaries).map(b -> b == null ? null : dbProduct.wrapDate(b)).toArray(String[]::new)
        );
    }
}
