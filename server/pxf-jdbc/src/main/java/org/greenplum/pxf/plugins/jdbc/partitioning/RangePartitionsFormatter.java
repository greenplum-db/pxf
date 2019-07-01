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

 /**
  * A class incapsulating methods common for all range-based partitions.
  */
class RangePartitionsFormatter {
    /**
     * Generate a range-based SQL constraint
     * @param columnQuoted column name (used as is, thus it should be quoted if necessary)
     * @param range range to base constraint on
     * @param rangeInclusionMask a binary mask to mark the boundaries of the range the constraint should include. When some 'range' boundary is null, the corresponding value of the mask has no effect
     * @return a pure SQL constraint (without WHERE)
     */
    public static String generateRangeConstraint(String columnQuoted, String[] range, boolean[] rangeInclusionMask) {
        if (columnQuoted == null) {
            throw new IllegalArgumentException("Partition column cannot be null");
        }
        if (range == null || rangeInclusionMask == null) {
            throw new IllegalArgumentException("Range or range inclusion mask cannot be null");
        }
        if (range.length != rangeInclusionMask.length) {
            throw new IllegalArgumentException("Range length and range inclusion mask length must be the sames");
        }

        StringBuilder sb = new StringBuilder();

        if (range.length == 1) {
            sb.append(columnQuoted).append(" = ").append(range[0]);
        }
        else {
            if (range[0] == null) {
                sb.append(columnQuoted).append(
                    rangeInclusionMask[1] ? " <= " : " < "
                ).append(range[1]);
            }
            else if (range[1] == null) {
                sb.append(columnQuoted).append(
                    rangeInclusionMask[0] ? " >= " : " > "
                ).append(range[0]);
            }
            else {
                sb.append(columnQuoted).append(rangeInclusionMask[0] ? " >= " : " > ").append(range[0]);
                sb.append(" AND ");
                sb.append(columnQuoted).append(rangeInclusionMask[1] ? " <= " : " < ").append(range[1]);
            }
        }

        return sb.toString();
    }
}
