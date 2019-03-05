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

import org.greenplum.pxf.api.BasicFilter;
import static org.greenplum.pxf.api.FilterParser.Operation.*;

import java.util.List;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JdbcFilterParserTest {
    @Test
    public void parseFilterWithThreeOperations() throws Exception {
        // column_#1 > '2008-02-01' and column_#1 < '2008-12-01' and column_#2 > 1200
        String filterString = "a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0";
        List<BasicFilter> filterList = JdbcFilterParser.parseFilters(filterString);
        BasicFilter currentFilter = null;

        // column_#1 > '2008-02-01'
        currentFilter = filterList.get(0);
        assertEquals(1, currentFilter.getColumn().index());
        assertEquals(HDOP_GT, currentFilter.getOperation());
        assertEquals("2008-02-01", currentFilter.getConstant().constant());

        // column_#1 < '2008-12-01'
        currentFilter = filterList.get(1);
        assertEquals(1, currentFilter.getColumn().index());
        assertEquals(HDOP_LT, currentFilter.getOperation());
        assertEquals("2008-12-01", currentFilter.getConstant().constant());

        // column_#2 > 1200
        currentFilter = filterList.get(2);
        assertEquals(2, currentFilter.getColumn().index());
        assertEquals(HDOP_GT, currentFilter.getOperation());
        assertEquals(1200L, currentFilter.getConstant().constant());
    }

    @Test
    public void parseIsNullExpression() throws Exception {
        String filterString = "a1o8";
        List<BasicFilter> filterList = JdbcFilterParser.parseFilters(filterString);
        BasicFilter currentFilter = null;

        currentFilter = filterList.get(0);
        assertEquals(HDOP_IS_NULL, currentFilter.getOperation());
        assertEquals(1, currentFilter.getColumn().index());
        assertNull(currentFilter.getConstant());
    }

    @Test
    public void parseIsNotNullExpression() throws Exception {
        String filterString = "a1o9";
        List<BasicFilter> filterList = JdbcFilterParser.parseFilters(filterString);
        BasicFilter currentFilter = null;

        currentFilter = filterList.get(0);
        assertEquals(HDOP_IS_NOT_NULL, currentFilter.getOperation());
        assertEquals(1, currentFilter.getColumn().index());
        assertNull(currentFilter.getConstant());
    }
}
