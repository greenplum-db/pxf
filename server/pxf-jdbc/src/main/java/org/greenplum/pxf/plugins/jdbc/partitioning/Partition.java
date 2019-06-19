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

import java.util.LinkedList;
import java.util.List;

/**
 * The high-level partitioning feature controller.
 *
 * Incapsulates concrete classes implementing various types of partitions.
 */
public enum Partition {
    INT {
        @Override
        public List<JdbcFragmentMetadata> fragments(String column, String range, String interval) {
            List<JdbcFragmentMetadata> result = new LinkedList<>();

            result.addAll(IntPartition.generate(column, range, interval));
            result.add(new NullPartition(column));

            return result;
        }
    },
    DATE {
        @Override
        public List<JdbcFragmentMetadata> fragments(String column, String range, String interval) {
            List<JdbcFragmentMetadata> result = new LinkedList<>();

            result.addAll(DatePartition.generate(column, range, interval));
            result.add(new NullPartition(column));

            return result;
        }
    },
    ENUM {
        @Override
        public List<JdbcFragmentMetadata> fragments(String column, String range, String interval) {
            List<JdbcFragmentMetadata> result = new LinkedList<>();

            result.addAll(EnumPartition.generate(column, range, interval));
            result.add(new NullPartition(column));

            return result;
        }
    };

    /**
     * Analyze the user-provided parameters (column name, RANGE and INTERVAL values) and form a list of fragments for this partition according to those parameters.
     * @param column the partition column name
     * @param range RANGE string value
     * @param interval INTERVAL string value
     * @return a list of fragments (of various concrete types)
     */
    public abstract List<JdbcFragmentMetadata> fragments(String column, String range, String interval);

    public static Partition of(String str) {
        return valueOf(str.toUpperCase());
    }
}
