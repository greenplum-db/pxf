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

import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
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
import java.util.UUID;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_TIME;

/**
 * JDBC tables resolver
 */
public class JdbcResolver extends JdbcBasePlugin implements Resolver {
    // Signifies the ERA format
    final private static String DATE_TIME_FORMATTER_SPECIFIER = " G";
    /**
     * LOCAL_DATE_GET_FORMATTER is used to format LocalDate to String.
     * Examples: 2023-01-10 -> "2023-01-10 AD"; +12345-02-01 -> "12345-02-01 AD"; -0009-12-11 -> "0010-12-11 BC"
     */
    private static final DateTimeFormatter LOCAL_DATE_GET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 9, SignStyle.NORMAL).appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendPattern(DATE_TIME_FORMATTER_SPECIFIER)
            .toFormatter();

    /**
     * LOCAL_DATE_TIME_GET_FORMATTER is used to format LocalDateTime to String.
     * Examples: 2018-10-19T10:11 -> "2018-10-19 10:11:00 AD"; +123456-10-19T11:12:13 -> "123456-10-19 11:12:13 AD";
     * -1233-10-19T10:11:15.456 -> "1234-10-19 10:11:15.456 BC"
     */
    private static final DateTimeFormatter LOCAL_DATE_TIME_GET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 9, SignStyle.NORMAL).appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2).appendLiteral(" ")
            .append(ISO_LOCAL_TIME)
            .appendPattern(DATE_TIME_FORMATTER_SPECIFIER)
            .toFormatter();


    /**
     * OFFSET_DATE_TIME_GET_FORMATTER is used to format OffsetDateTime to String.
     * Examples: 1956-02-01T07:15:16Z -> "1956-02-01 07:15:16Z AD"; +12345-02-01T10:15:16Z -> "12345-02-01 10:15:16Z AD";
     * -1999-02-01T04:15:16Z -> "2000-02-01 04:15:16Z BC"
     */
    private static final DateTimeFormatter OFFSET_DATE_TIME_GET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 4, 9, SignStyle.NORMAL).appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2).appendLiteral(" ")
            .append(ISO_OFFSET_TIME)
            .appendPattern(DATE_TIME_FORMATTER_SPECIFIER)
            .toFormatter();

    /**
     * LOCAL_DATE_SET_FORMATTER is used to format String to LocalDate.
     * Examples: "1977-12-11" -> 1977-12-11; "456789-12-11" -> +456789-12-11; "0010-12-11 BC" -> -0009-12-11
     */
    private static final DateTimeFormatter LOCAL_DATE_SET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 1, 9, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .optionalStart().appendPattern(DATE_TIME_FORMATTER_SPECIFIER).optionalEnd()
            .toFormatter();

    /**
     * LOCAL_DATE_TIME_SET_FORMATTER is used to transfer String to LocalDateTime.
     * Examples: "1980-08-10 17:10:20" -> 1980-08-10T17:10:20; "123456-10-19 11:12:13" -> +123456-10-19T11:12:13;
     * "1234-10-19 10:11:15.456 BC" -> -1233-10-19T10:11:15.456
     */
    private static final DateTimeFormatter LOCAL_DATE_TIME_SET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 1, 9, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL).appendLiteral(" ")
            .append(ISO_LOCAL_TIME)
            .optionalStart().appendPattern(DATE_TIME_FORMATTER_SPECIFIER).optionalEnd()
            .toFormatter();

    /**
     * OFFSET_DATE_TIME_SET_FORMATTER is used to transfer String to OffsetDateTime.
     * Examples: "1980-08-10 17:10:20-07" -> 1980-08-10T17:10:20-07; "123456-10-19 11:12:13+06:30" -> +123456-10-19T11:12:13+6:30;
     * "1234-10-19 10:11:15.456+00 BC" -> -1233-10-19T10:11:15.456+00
     */
    private static final DateTimeFormatter OFFSET_DATE_TIME_SET_FORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR_OF_ERA, 1, 9, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL).appendLiteral(" ")
            .append(ISO_LOCAL_TIME)
            .appendOffset("+HH:mm", "Z")
            .optionalStart().appendPattern(DATE_TIME_FORMATTER_SPECIFIER).optionalEnd()
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
     * Creates a new instance of the JdbcResolver
     */
    public JdbcResolver() {
        super();
    }

    /**
     * Creates a new instance of the resolver with provided connection manager.
     *
     * @param connectionManager connection manager
     * @param secureLogin       the instance of the secure login
     */
    JdbcResolver(ConnectionManager connectionManager, SecureLogin secureLogin) {
        super(connectionManager, secureLogin);
    }

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
                        Date date = result.getDate(colName);
                        value = date != null ? date.toLocalDate().format(GreenplumDateTime.DATE_FORMATTER) : null;
                    }
                    break;
                case TIMESTAMP:
                    if (isDateWideRange) {
                        LocalDateTime localDateTime = result.getObject(colName, LocalDateTime.class);
                        value = localDateTime != null ? localDateTime.format(LOCAL_DATE_TIME_GET_FORMATTER) : null;
                    } else {
                        Timestamp timestamp = result.getTimestamp(colName);
                        value = timestamp != null ? timestamp.toLocalDateTime().format(GreenplumDateTime.DATETIME_FORMATTER) : null;
                    }
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    OffsetDateTime offsetDateTime = result.getObject(colName, OffsetDateTime.class);
                    DateTimeFormatter timestamptzFormatter = isDateWideRange ? OFFSET_DATE_TIME_GET_FORMATTER : GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER;
                    value = offsetDateTime != null ? offsetDateTime.format(timestamptzFormatter) : null;
                    break;
                case UUID:
                    value = result.getObject(colName, java.util.UUID.class);
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
                    case TIMESTAMP_WITH_TIME_ZONE:
                        oneField.val = getOffsetDateTime(rawVal);
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
                    case UUID:
                        oneField.val = UUID.fromString(rawVal);
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
                            statement.setObject(i, field.val);
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
                            statement.setObject(i, field.val);
                        } else {
                            statement.setDate(i, (Date) field.val);
                        }
                    }
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                case UUID:
                    statement.setObject(i, field.val);
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

    /**
     * Convert a string to OffsetDateTime class with formatter
     *
     * @param rawVal the OffsetDateTime in a string format
     * @return OffsetDateTime
     */
    private OffsetDateTime getOffsetDateTime(String rawVal) {
        try {
            if (isDateWideRange) {
                return OffsetDateTime.parse(rawVal, OFFSET_DATE_TIME_SET_FORMATTER);
            } else {
                return OffsetDateTime.parse(rawVal, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to convert timestamp with timezone '" + rawVal + "' to the OffsetDateTime class: " + e.getMessage(), e);
        }
    }
}
