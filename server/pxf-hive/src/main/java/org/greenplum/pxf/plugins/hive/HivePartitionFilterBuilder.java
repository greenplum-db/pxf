package org.greenplum.pxf.plugins.hive;

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

import org.greenplum.pxf.api.filter.ColumnPredicateBuilder;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.List;

/**
 * A tree visitor to generate a filter string for partition filtering.
 * Build filter string for HiveMetaStoreClient.listPartitionsByFilter API
 * method.
 */
public class HivePartitionFilterBuilder extends ColumnPredicateBuilder {

    private static final String HIVE_API_D_QUOTE = "\"";

    public HivePartitionFilterBuilder(List<ColumnDescriptor> tupleDescription) {
        super(tupleDescription);
    }

    @Override
    public String toString() {
        StringBuilder sb = getStringBuilder();
        return sb.length() > 0 ? sb.toString() : null;
    }

    @Override
    protected String serializeValue(DataType type, String value) {
        return String.format("%s%s%s", HIVE_API_D_QUOTE, value, HIVE_API_D_QUOTE);
    }
}
