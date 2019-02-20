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

import com.google.gson.JsonArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

/**
 * PXF-Ignite resolver class
 */
public class IgniteResolver extends IgniteBasePlugin implements Resolver {

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        columns = requestContext.getTupleDescription();
    }

    /**
     * Transform a {@link JsonArray} stored in {@link OneRow} into a list of {@link OneField}
     *
     * @throws ParseException if the response could not be correctly parsed
     * @throws UnsupportedOperationException if the type of some field is not supported
     */
    @Override
    public List<OneField> getFields(OneRow row) throws ParseException, UnsupportedOperationException {
        List<?> result = List.class.cast(row.getData());
        LinkedList<OneField> fields = new LinkedList<OneField>();

        if (result.size() != columns.size()) {
            throw new ParseException("getFields(): Failed (a tuple received from Ignite contains more or less fields than requested). Raw tuple: '" + result.toString() + "'", 0);
        }

        for (int i = 0; i < columns.size(); i++) {
            Object value = null;
            OneField oneField = new OneField(columns.get(i).columnTypeCode(), null);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Parsing oneField #" + i + " of type '" + DataType.get(oneField.type).toString() + "'");
            }

            // Handle null values
            if (result.get(i) == null) {
                fields.add(oneField);
                continue;
            }
            switch (DataType.get(oneField.type)) {
                case INTEGER:
                case FLOAT8:
                case REAL:
                case BIGINT:
                case SMALLINT:
                case BOOLEAN:
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                case BYTEA:
                case TIMESTAMP:
                case DATE:
                    value = result.get(i);
                    break;
                default:
                    throw new UnsupportedOperationException("Field type '" + DataType.get(oneField.type).toString() + "' (column '" + columns.get(i).columnName() + "') is not supported");
            }

            oneField.val = value;
            fields.add(oneField);
        }

        return fields;
    }

    /**
     * Transforms a list of {@link OneField} from PXF into a {@link OneRow} with an Object[] inside
     *
     * @throws UnsupportedOperationException if the type of some field is not supported
     */
    @Override
    public OneRow setFields(List<OneField> record) throws UnsupportedOperationException, ParseException {
        Object[] queryArgs = new Object[record.size()];

        int column_index = 0;
        for (OneField oneField : record) {
            ColumnDescriptor column = columns.get(column_index);
            if (
                LOG.isDebugEnabled() &&
                DataType.get(column.columnTypeCode()) != DataType.get(oneField.type)
            ) {
                LOG.warn("The provided tuple of data may be disordered. Datatype of column with descriptor '" + column.toString() + "' must be '" + DataType.get(column.columnTypeCode()).toString() + "', but actual is '" + DataType.get(oneField.type).toString() + "'");
            }

            // Check that data type is supported
            switch (DataType.get(oneField.type)) {
                case BOOLEAN:
                case INTEGER:
                case FLOAT8:
                case REAL:
                case BIGINT:
                case SMALLINT:
                case NUMERIC:
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case BYTEA:
                case TIMESTAMP:
                case DATE:
                    break;
                default:
                    throw new UnsupportedOperationException("Field type '" + DataType.get(oneField.type).toString() + "' (column '" + column.toString() + "') is not supported");
            }

            // Convert TEXT columns into native data types
            if ((DataType.get(oneField.type) == DataType.TEXT) && (DataType.get(column.columnTypeCode()) != DataType.TEXT)) {
                oneField.type = column.columnTypeCode();
                if (oneField.val != null) {
                    String rawVal = (String)oneField.val;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("OneField content (conversion from TEXT): '" + rawVal + "'");
                    }
                    switch (DataType.get(column.columnTypeCode())) {
                        case VARCHAR:
                        case BPCHAR:
                        case TEXT:
                        case BYTEA:
                            break;
                        case BOOLEAN:
                            oneField.val = (Object)Boolean.parseBoolean(rawVal);
                            break;
                        case INTEGER:
                            oneField.val = (Object)Integer.parseInt(rawVal);
                            break;
                        case FLOAT8:
                            oneField.val = (Object)Double.parseDouble(rawVal);
                            break;
                        case REAL:
                            oneField.val = (Object)Float.parseFloat(rawVal);
                            break;
                        case BIGINT:
                            oneField.val = (Object)Long.parseLong(rawVal);
                            break;
                        case SMALLINT:
                            oneField.val = (Object)Short.parseShort(rawVal);
                            break;
                        case NUMERIC:
                            oneField.val = (Object)new BigDecimal(rawVal);
                            break;
                        case TIMESTAMP:
                            boolean isConversionSuccessful = false;
                            for (SimpleDateFormat sdf : timestampSDFs.get()) {
                                try {
                                    java.util.Date parsedTimestamp = sdf.parse(rawVal);
                                    oneField.val = (Object)new Timestamp(parsedTimestamp.getTime());
                                    isConversionSuccessful = true;
                                    break;
                                }
                                catch (ParseException e) {
                                    // pass
                                }
                            }
                            if (!isConversionSuccessful) {
                                throw new ParseException(rawVal, 0);
                            }
                            break;
                        case DATE:
                            oneField.val = (Object)new Date(dateSDF.get().parse(rawVal).getTime());
                            break;
                        default:
                            throw new UnsupportedOperationException("Conversion from TEXT for fields of type '" + DataType.get(oneField.type).toString() + "' (column '" + column.toString() + "') is not supported");
                    }
                }
            }

            queryArgs[column_index] = oneField.val;
            column_index += 1;
        }

        return new OneRow(queryArgs);
    }

    private static final Log LOG = LogFactory.getLog(IgniteResolver.class);

    // SimpleDateFormat to parse TEXT into DATE
    private static ThreadLocal<SimpleDateFormat> dateSDF = new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };
    // SimpleDateFormat to parse TEXT into TIMESTAMP (with microseconds)
    private static ThreadLocal<SimpleDateFormat[]> timestampSDFs = new ThreadLocal<SimpleDateFormat[]>() {
        @Override protected SimpleDateFormat[] initialValue() {
            SimpleDateFormat[] retRes = {
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
                new SimpleDateFormat("yyyy-MM-dd")
            };
            return retRes;
        }
    };

    // GPDB column descriptors
    private List<ColumnDescriptor> columns = null;
}
