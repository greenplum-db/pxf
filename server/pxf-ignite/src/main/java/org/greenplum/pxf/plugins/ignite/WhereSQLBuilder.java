package org.greenplum.pxf.plugins.ignite;
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

import org.greenplum.pxf.api.LogicalFilter;
import org.greenplum.pxf.api.BasicFilter;
import org.greenplum.pxf.api.FilterParser;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Parse filter object generated by the parent class {@link IgniteFilterBuilder} and build WHERE statement.
 * Only HDOP_AND is supported as a logicalFilter at the moment.
 *
 * If an error occurs, the filter is not applied at all and no error is returned. Note that in this case the filter will not be applied on Ignite side, but it will be applied by PXF itself (later), thus the results of the query will be correct.
 */
public class WhereSQLBuilder extends IgniteFilterBuilder {
    /**
     * Class constructor
     *
     * @param input
     * @param quoteMark quote string to use. `null` is allowed
     */
    public WhereSQLBuilder(RequestContext input, String quoteMark) {
        requestContext = input;
        QUOTE = quoteMark != null ? quoteMark : new String("");
    }

    /**
     * Build a WHERE statement using the RequestContext provided to constructor.
     *
     * @return SQL string
     * @throws Exception if 'RequestContext' has filter, but its 'filterString' is incorrect
     */
    public String buildWhereSQL() throws Exception {
        if (!requestContext.hasFilter()) {
            return null;
        }

        List<BasicFilter> filters = null;
        try {
            String filterString = requestContext.getFilterString();
            Object filterObj = getFilterObject(filterString);

            filters = convertBasicFilterList(filterObj, filters);

            StringBuffer sb = new StringBuffer();
            String andDivisor = "";
            for (Object obj : filters) {
                BasicFilter filter = (BasicFilter) obj;

                sb.append(andDivisor);
                andDivisor = " AND ";
                ColumnDescriptor column = requestContext.getColumn(filter.getColumn().index());
                sb.append(QUOTE + column.columnName() + QUOTE);

                FilterParser.Operation op = filter.getOperation();
                switch (op) {
                    case HDOP_LT:
                        sb.append("<");
                        break;
                    case HDOP_GT:
                        sb.append(">");
                        break;
                    case HDOP_LE:
                        sb.append("<=");
                        break;
                    case HDOP_GE:
                        sb.append(">=");
                        break;
                    case HDOP_EQ:
                        sb.append("=");
                        break;
                    default:
                        throw new UnsupportedFilterException("unsupported Filter operation : " + op);
                }

                Object val = filter.getConstant().constant();
                switch (DataType.get(column.columnTypeCode())) {
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                    case FLOAT8:
                    case REAL:
                    case BOOLEAN:
                        sb.append(val.toString());
                        break;
                    case TEXT:
                        sb.append("'").append(val.toString()).append("'");
                        break;
                    case DATE:
                        sb.append("'" + val.toString() + "'");
                        break;
                    default:
                        throw new UnsupportedFilterException("unsupported column type for filtering : " + column.columnTypeCode());
                }

            }
            return sb.toString();
        } catch (UnsupportedFilterException ex) {
            // Do not throw exception, impose no constraints instead
            return null;
        }
    }


    /**
     * Parses PXF {@link RequestContext} 'FilterObject'
     * Only 'HDOP_AND' is supported as a 'LogicalOperation' at the moment
     *
     * @param filter 'FilterObject' to parse
     * @param returnList a list of 'BasicFilter' to append 'BasicFilter' to
     *
     * @throws UnsupportedFilterException if the provided filter string or 'LogicalOperation' is incorrect
     */
    private List<BasicFilter> convertBasicFilterList(Object filter, List<BasicFilter> returnList) throws UnsupportedFilterException {
        if (returnList == null) {
            returnList = new ArrayList<>();
        }

        if (filter instanceof BasicFilter) {
            returnList.add((BasicFilter) filter);
            return returnList;
        }

        LogicalFilter lfilter = (LogicalFilter) filter;
        if (lfilter.getOperator() != FilterParser.LogicalOperation.HDOP_AND) {
            throw new UnsupportedFilterException("unsupported LogicalOperation : " + lfilter.getOperator());
        }

        for (Object f : lfilter.getFilterList()) {
            returnList = convertBasicFilterList(f, returnList);
        }

        return returnList;
    }

    /**
     * Unsupported filter exception class.
     * Thrown when the filter string passed to constructor cannot be parsed
     */
    private static class UnsupportedFilterException extends Exception {
        UnsupportedFilterException(String message) {
            super(message);
        }
    }

    // PXF RequestContext
    private RequestContext requestContext;

    // Quotation mark (provided by IgniteAccessor)
    private final String QUOTE;
}
