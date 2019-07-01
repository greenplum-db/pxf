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

import java.util.stream.Collectors;
import java.util.stream.Stream;

class EnumPartition extends BasePartition implements JdbcFragmentMetadata {
    private static final long serialVersionUID = 0L;

    private final String value;
    private final String[] excluded;

    /**
     * Construct an EnumPartition with given column and constraint
     *
     * @param column
     * @param value
     */
    public EnumPartition(String column, String value) {
        super(column);
        if (value == null) {
            throw new RuntimeException("Value cannot be null");
        }

        this.value = value;
        excluded = null;
    }

    /**
     * Construct an EnumPartition with given column and a special (exclusion) constraint.
     * The partition created by this constructor contains all values that differ from the given ones.
     *
     * @param column
     * @param excluded array of values this partition must NOT include
     */
    public EnumPartition(String column, String[] excluded) {
        super(column);
        if (excluded == null) {
            throw new RuntimeException("Excluded values cannot be null");
        }
        if (excluded.length == 0) {
            throw new RuntimeException("Array of excluded values cannot be of zero length");
        }

        value = null;
        this.excluded = excluded;
    }

    @Override
    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        if (quoteString == null) {
            throw new RuntimeException("Quote string cannot be null");
        }

        StringBuilder sb = new StringBuilder();

        String quotedColumn = quoteString + column + quoteString;

        if (excluded == null) {
            sb.append(quotedColumn).append(" = '").append(value).append("'");
        } else {
            // We use inequality operator as it is the widest supported method
            sb.append("( ")
                    .append(Stream.of(excluded)
                            .map(excludedValue -> quotedColumn + " <> '" + excludedValue + "'")
                            .collect(Collectors.joining(" AND "))
                    ).append(" )");
        }

        return sb.toString();
    }

    /**
     * Getter
     */
    public String getValue() {
        return value;
    }

    /**
     * Getter
     */
    public String[] getExcluded() {
        return excluded;
    }
}
