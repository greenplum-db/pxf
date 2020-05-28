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

import java.util.stream.Stream;

public class IntPartition extends BasePartition implements JdbcFragmentMetadata {

    @Getter
    private final Long[] boundaries;

    /**
     * @param column the partition column
     * @param start  null for right-bounded interval
     * @param end    null for left-bounded interval
     */
    public IntPartition(String column, Long start, Long end) {
        this(column, (start == end) ? new Long[]{start} : new Long[]{start, end});
        if (start == null && end == null) {
            throw new RuntimeException("Both boundaries cannot be null");
        }
    }

    @JsonCreator
    public IntPartition(@JsonProperty("column") String column,
                        @JsonProperty("boundaries") Long[] boundaries) {
        super(column);
        this.boundaries = boundaries;
    }

    @Override
    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        if (quoteString == null) {
            throw new RuntimeException("Quote string cannot be null");
        }

        return generateRangeConstraint(
                quoteString + column + quoteString,
                Stream.of(boundaries).map(b -> b == null ? null : b.toString()).toArray(String[]::new)
        );
    }
}
