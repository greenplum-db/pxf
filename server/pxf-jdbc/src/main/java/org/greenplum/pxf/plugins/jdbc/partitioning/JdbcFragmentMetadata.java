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

/**
 * A class to store and transform partition constraints for JDBC partitioning feature.
 */
public interface JdbcFragmentMetadata {
    /**
     * Translate these fragment metadata to a SQL constraint
     *
     * @param quoteString string to use to quote partition column
     * @param dbProduct {@link DbProduct} to wrap constraint values
     * @return pure SQL constraint (without WHERE)
     */
    public String toSqlConstraint(String quoteString, DbProduct dbProduct);

    /**
     * @return partition column name of these fragment metadata
     */
	public String getColumn();

    /**
     * @return partition type of these fragment metadata
     */
	public PartitionType getType();
}
