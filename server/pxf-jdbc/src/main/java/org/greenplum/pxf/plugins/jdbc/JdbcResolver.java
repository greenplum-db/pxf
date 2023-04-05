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

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_TIME;

/**
 * JDBC tables resolver
 */
public class JdbcResolver extends JdbcBasePlugin implements Resolver {
    private static final DateTimeFormatter LOCAL_DATE_GET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 9, SignStyle.NORMAL).appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendPattern(" G")
            .toFormatter();

    private static final DateTimeFormatter LOCAL_DATE_TIME_GET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 9, SignStyle.NORMAL).appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2).appendLiteral(" ")
            .append(ISO_LOCAL_TIME)
            .appendPattern(" G")
            .toFormatter();

    private static final DateTimeFormatter OFFSET_DATE_TIME_GET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 9, SignStyle.NORMAL).appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2).appendLiteral(" ")
            .append(ISO_OFFSET_TIME)
            .appendPattern(" G")
            .toFormatter();

    private static final DateTimeFormatter LOCAL_DATE_SET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 1, 9, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .optionalStart().appendPattern(" G").optionalEnd()
            .toFormatter();

    private static final DateTimeFormatter LOCAL_DATE_TIME_SET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 1, 9, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            .appendLiteral(" ")
            .append(ISO_LOCAL_TIME)
            .optionalStart().appendPattern(" G").optionalEnd()
            .toFormatter();

    private static final Set<DataType> DATATYPES_SUPPORTED = EnumSet.of(
            DataType.VARCHAR,
            DataType.BPCHAR,
            DataType.TEXT,
            DataType.BYTEA,
            DataType.BOOLEAN,
            DataType.INTEGER,
            DataType.FLOAT8,
            DataType.REAL,
            DataType.BIGINT,
            DataType.SMALLINT,
            DataType.NUMERIC,
            DataType.TIMESTAMP,
            DataType.DATE
    );

    private static final Logger LOG = LoggerFactory.getLogger(JdbcResolver.class);

    /**
     * getFields() implementation
     *
     * @param row one row
     * @throws SQLException if the provided {@link OneRow} object is invalid
     */
    @Override
    public List<OneField> getFields(OneRow row) throws SQLException {
        ResultSet result = (ResultSet) row.getData();
        LinkedList<OneField> fields = new LinkedList<>();

        for (ColumnDescriptor column : columns) {
            String colName = column.columnName();
            Object value;

            OneField oneField = new OneField();
            oneField.type = column.columnTypeCode();

            fields.add(oneField);

            /*
             * Non-projected columns get null values
             */
            if (!column.isProjected()) continue;

            switch (DataType.get(oneField.type)) {
                case INTEGER:
                    value = result.getInt(colName);
                    break;
                case FLOAT8:
                    value = result.getDouble(colName);
                    break;
                case REAL:
                    value = result.getFloat(colName);
                    break;
                case BIGINT:
                    value = result.getLong(colName);
                    break;
                case SMALLINT:
                    value = result.getShort(colName);
                    break;
                case BOOLEAN:
                    value = result.getBoolean(colName);
                    break;
                case BYTEA:
                    value = result.getBytes(colName);
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                    value = result.getString(colName);
                    break;
                case DATE:
                    if (isDateWideRange) {
                        LocalDate localDate = result.getObject(colName, LocalDate.class);
                        value = localDate != null ? localDate.format(LOCAL_DATE_GET_FORMATTER) : null;
                    } else {
                        value = result.getDate(colName);
                    }
                    break;
                case TIMESTAMP:
                    if (isDateWideRange) {
                        LocalDateTime localDateTime = result.getObject(colName, LocalDateTime.class);
                        value = localDateTime != null ? localDateTime.format(LOCAL_DATE_TIME_GET_FORMATTER) : null;
                    } else {
                        value = result.getTimestamp(colName);
                    }
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    if (isDateWideRange) {
                        OffsetDateTime offsetDateTime = result.getObject(colName, OffsetDateTime.class);
                        value = offsetDateTime != null ? offsetDateTime.format(OFFSET_DATE_TIME_GET_FORMATTER) : null;
                    } else {
                        throw new UnsupportedOperationException(
                                String.format("Field type '%s' (column '%s') is not supported",
                                        DataType.get(oneField.type),
                                        column));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Field type '%s' (column '%s') is not supported",
                                    DataType.get(oneField.type),
                                    column));
            }

            oneField.val = result.wasNull() ? null : value;
        }
        return fields;
    }

    /**
     * setFields() implementation
     *
     * @param record List of fields
     * @return OneRow with the data field containing a List of fields
     * OneFields are not reordered before being passed to Accessor; at the
     * moment, there is no way to correct the order of the fields if it is not.
     * In practice, the 'record' provided is always ordered the right way.
     * @throws UnsupportedOperationException if field of some type is not supported
     * @throws ParseException                if the record cannot be parsed
     */
    @Override
    public OneRow setFields(List<OneField> record) throws UnsupportedOperationException, ParseException {
        int columnIndex = 0;

        for (OneField oneField : record) {
            ColumnDescriptor column = columns.get(columnIndex++);

            DataType oneFieldType = DataType.get(oneField.type);
            DataType columnType = column.getDataType();

            if (!DATATYPES_SUPPORTED.contains(oneFieldType)) {
                throw new UnsupportedOperationException(
                        String.format("Field type '%s' (column '%s') is not supported",
                                oneFieldType, column));
            }

            if (LOG.isDebugEnabled()) {
                String valDebug;
                if (oneField.val == null) {
                    valDebug = "null";
                } else if (oneFieldType == DataType.BYTEA) {
                    valDebug = String.format("'{}'", new String((byte[]) oneField.val));
                } else {
                    valDebug = String.format("'{}'", oneField.val.toString());
                }

                LOG.debug("Column {} OneField: type {}, content {}", columnIndex, oneFieldType, valDebug);
            }

            // Convert TEXT columns into native data types
            if ((oneFieldType == DataType.TEXT) && (columnType != DataType.TEXT)) {
                oneField.type = columnType.getOID();

                if (oneField.val == null) {
                    continue;
                }

                String rawVal = (String) oneField.val;
                switch (columnType) {
                    case VARCHAR:
                    case BPCHAR:
                    case TEXT:
                    case BYTEA:
                        break;
                    case BOOLEAN:
                        oneField.val = Boolean.parseBoolean(rawVal);
                        break;
                    case INTEGER:
                        oneField.val = Integer.parseInt(rawVal);
                        break;
                    case FLOAT8:
                        oneField.val = Double.parseDouble(rawVal);
                        break;
                    case REAL:
                        oneField.val = Float.parseFloat(rawVal);
                        break;
                    case BIGINT:
                        oneField.val = Long.parseLong(rawVal);
                        break;
                    case SMALLINT:
                        oneField.val = Short.parseShort(rawVal);
                        break;
                    case NUMERIC:
                        oneField.val = new BigDecimal(rawVal);
                        break;
                    case TIMESTAMP:
                        if (isDateWideRange) {
                            oneField.val = getLocalDateTime(rawVal);
                        } else {
                            oneField.val = Timestamp.valueOf(rawVal);
                        }
                        break;
                    case DATE:
                        if (isDateWideRange) {
                            oneField.val = getLocalDate(rawVal);
                        } else {
                            oneField.val = Date.valueOf(rawVal);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                String.format("Field type '%s' (column '%s') is not supported",
                                        oneFieldType, column));
                }
            }
        }

        return new OneRow(record);
    }

    /**
     * Decode OneRow object and pass all its contents to a PreparedStatement
     *
     * @param row       one row
     * @param statement PreparedStatement
     * @throws IOException  if data in a OneRow is corrupted
     * @throws SQLException if the given statement is broken
     */
    @SuppressWarnings("unchecked")
    public static void decodeOneRowToPreparedStatement(OneRow row, PreparedStatement statement) throws IOException, SQLException {
        // This is safe: OneRow comes from JdbcResolver
        List<OneField> tuple = (List<OneField>) row.getData();
        for (int i = 1; i <= tuple.size(); i++) {
            OneField field = tuple.get(i - 1);
            switch (DataType.get(field.type)) {
                case INTEGER:
                    if (field.val == null) {
                        statement.setNull(i, Types.INTEGER);
                    } else {
                        statement.setInt(i, (int) field.val);
                    }
                    break;
                case BIGINT:
                    if (field.val == null) {
                        statement.setNull(i, Types.INTEGER);
                    } else {
                        statement.setLong(i, (long) field.val);
                    }
                    break;
                case SMALLINT:
                    if (field.val == null) {
                        statement.setNull(i, Types.INTEGER);
                    } else {
                        statement.setShort(i, (short) field.val);
                    }
                    break;
                case REAL:
                    if (field.val == null) {
                        statement.setNull(i, Types.FLOAT);
                    } else {
                        statement.setFloat(i, (float) field.val);
                    }
                    break;
                case FLOAT8:
                    if (field.val == null) {
                        statement.setNull(i, Types.DOUBLE);
                    } else {
                        statement.setDouble(i, (double) field.val);
                    }
                    break;
                case BOOLEAN:
                    if (field.val == null) {
                        statement.setNull(i, Types.BOOLEAN);
                    } else {
                        statement.setBoolean(i, (boolean) field.val);
                    }
                    break;
                case NUMERIC:
                    if (field.val == null) {
                        statement.setNull(i, Types.NUMERIC);
                    } else {
                        statement.setBigDecimal(i, (BigDecimal) field.val);
                    }
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                    if (field.val == null) {
                        statement.setNull(i, Types.VARCHAR);
                    } else {
                        statement.setString(i, (String) field.val);
                    }
                    break;
                case BYTEA:
                    if (field.val == null) {
                        statement.setNull(i, Types.BINARY);
                    } else {
                        statement.setBytes(i, (byte[]) field.val);
                    }
                    break;
                case TIMESTAMP:
                    if (field.val == null) {
                        statement.setNull(i, Types.TIMESTAMP);
                    } else {
                        if (field.val instanceof LocalDateTime) {
                            statement.setObject(i, (LocalDateTime) field.val);
                        } else {
                            statement.setTimestamp(i, (Timestamp) field.val);
                        }
                    }
                    break;
                case DATE:
                    if (field.val == null) {
                        statement.setNull(i, Types.DATE);
                    } else {
                        if (field.val instanceof LocalDate) {
                            statement.setObject(i, (LocalDate) field.val);
                        } else {
                            statement.setDate(i, (Date) field.val);
                        }
                    }
                    break;
                default:
                    throw new IOException("The data tuple from JdbcResolver is corrupted");
            }
        }
    }

    /**
     * Convert a string to LocalDate class with formatter
     *
     * @param rawVal the LocalDate in a string format
     * @return LocalDate
     */
    private LocalDate getLocalDate(String rawVal) {
        try {
            return LocalDate.parse(rawVal, LOCAL_DATE_SET_FORMATTER);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to convert date '" + rawVal + "' to LocalDate class: " + e.getMessage(), e);
        }
    }

    /**
     * Convert a string to LocalDateTime class with formatter
     *
     * @param rawVal the LocalDateTime in a string format
     * @return LocalDateTime
     */
    private LocalDateTime getLocalDateTime(String rawVal) {
        try {
            return LocalDateTime.parse(rawVal, LOCAL_DATE_TIME_SET_FORMATTER);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to convert timestamp '" + rawVal + "' to the LocalDateTime class: " + e.getMessage(), e);
        }
    }
}
