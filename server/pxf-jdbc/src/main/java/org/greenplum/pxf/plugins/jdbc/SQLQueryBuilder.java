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
import org.greenplum.pxf.api.FilterParser;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.util.List;
import java.util.regex.Pattern;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL query builder.
 *
 * Uses {@link JdbcFilterParser} to get array of filters
 */
public class SQLQueryBuilder {
    /**
     * Construct a new SQLQueryBuilder
     *
     * @param context {@link RequestContext}
     * @param metaData {@link DatabaseMetaData}
     *
     * @throws SQLException if some call of DatabaseMetaData method fails
     */
    public SQLQueryBuilder(RequestContext context, DatabaseMetaData metaData) throws SQLException {
        if (context == null) {
            throw new IllegalArgumentException("Provided RequestContext is null");
        }
        requestContext = context;
        if (metaData == null) {
            throw new IllegalArgumentException("Provided DatabaseMetaData is null");
        }
        databaseMetaData = metaData;

        columns = context.getTupleDescription();
        tableName = context.getDataSource();

        quoteString = "";
    }

    /**
     * Build SELECT query (with "WHERE" and partition constraints).
     *
     * @return Complete SQL query
     *
     * @throws ParseException if the constraints passed in RequestContext are incorrect
     * @throws SQLException if some call of DatabaseMetaData method fails
     */
    public String buildSelectQuery() throws ParseException, SQLException {
        DbProduct dbProduct = DbProduct.getDbProduct(databaseMetaData.getDatabaseProductName());

        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");

        // Insert columns' names
        String columnDivisor = "";
        for (ColumnDescriptor column : columns) {
            sb.append(columnDivisor);
            columnDivisor = ", ";
            sb.append(quoteString + column.columnName() + quoteString);
        }

        // Insert table name
        sb.append(" FROM ");
        sb.append(tableName);

        // Insert regular WHERE constraints
        buildWhereSQL(dbProduct, sb);

        // Insert partition constraints
        JdbcPartitionFragmenter.buildFragmenterSql(requestContext, dbProduct, quoteString, sb);

        return sb.toString();
    }

    /**
     * Build INSERT query template (field values are replaced by placeholders '?')
     *
     * @return SQL query with placeholders instead of actual values
     *
     * @throws SQLException if some call of DatabaseMetaData method fails
     */
    public String buildInsertQuery() throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        sb.append(tableName);

        // Insert columns' names
        sb.append("(");
        String fieldDivisor = "";
        for (ColumnDescriptor column : columns) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append(quoteString + column.columnName() + quoteString);
        }
        sb.append(")");

        sb.append(" VALUES ");

        // Insert values placeholders
        sb.append("(");
        fieldDivisor = "";
        for (int i = 0; i < columns.size(); i++) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append("?");
        }
        sb.append(")");

        return sb.toString();
    }

    /**
     * Check whether column names must be quoted and set quoteString if so.
     *
     * Quote string is set to value provided by {@link DatabaseMetaData}.
     *
     * @throws SQLException if some method of {@link DatabaseMetaData} fails
     */
    public void autoSetQuoteString() throws SQLException {
        // Prepare a pattern of characters that may be not quoted
        String extraNameCharacters = databaseMetaData.getExtraNameCharacters();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Extra name characters supported by external database: '" + extraNameCharacters + "'");
        }
        extraNameCharacters = extraNameCharacters.replace("-", "\\-");
        Pattern normalCharactersPattern = Pattern.compile("[" + "\\w" + extraNameCharacters + "]+");

        // Check if some column name should be quoted
        boolean mixedCaseNamePresent = false;
        boolean specialCharactersNamePresent = false;
        for (ColumnDescriptor column : columns) {
            // Define whether column name is mixed-case
            // GPDB uses lower-case names if column name was not quoted
            if (column.columnName().toLowerCase() != column.columnName()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Column " + column.columnIndex() + " '" + column.columnName() + "' is mixed-case");
                }
                mixedCaseNamePresent = true;
                break;
            }
            // Define whether column name contains special symbols
            if (!normalCharactersPattern.matcher(column.columnName()).matches()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Column " + column.columnIndex() + " '" + column.columnName() + "' contains special characters");
                }
                specialCharactersNamePresent = true;
                break;
            }
        }

        if (
            specialCharactersNamePresent ||
            (mixedCaseNamePresent && !databaseMetaData.supportsMixedCaseIdentifiers())
        ) {
            quoteString = databaseMetaData.getIdentifierQuoteString();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Quotation auto-enabled; quote string set to '" + quoteString + "'");
            }
        }
    }

    /**
     * Set quoteString to value provided by {@link DatabaseMetaData}.
     *
     * @throws SQLException if some method of {@link DatabaseMetaData} fails
     */
    public void forceSetQuoteString() throws SQLException {
        quoteString = databaseMetaData.getIdentifierQuoteString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Quotation force-enabled; quote string set to '" + quoteString + "'");
        }
    }

    /**
     * Insert WHERE constraints into a given query.
     * Note that if filter is not supported, query is left unchanged.
     *
     * @param dbName Database name (affects the behaviour for DATE constraints)
     * @param query SQL query to insert constraints to. The query may may contain other WHERE statements
     *
     * @throws ParseException if filter string is invalid
     */
    private void buildWhereSQL(DbProduct dbProduct, StringBuilder query) throws ParseException {
        if (!requestContext.hasFilter()) {
            return;
        }

        try {
            StringBuilder prepared = new StringBuilder();
            if (!query.toString().contains("WHERE")) {
                prepared.append(" WHERE ");
            }
            else {
                prepared.append(" AND ");
            }

            // Get constraints
            List<BasicFilter> filters = JdbcFilterParser.parseFilters(requestContext.getFilterString());

            String andDivisor = "";
            for (Object obj : filters) {
                prepared.append(andDivisor);
                andDivisor = " AND ";

                // Insert constraint column name
                BasicFilter filter = (BasicFilter) obj;
                ColumnDescriptor column = requestContext.getColumn(filter.getColumn().index());
                prepared.append(quoteString + column.columnName() + quoteString);

                // Insert constraint operator
                FilterParser.Operation op = filter.getOperation();
                switch (op) {
                    case HDOP_LT:
                        prepared.append(" < ");
                        break;
                    case HDOP_GT:
                        prepared.append(" > ");
                        break;
                    case HDOP_LE:
                        prepared.append(" <= ");
                        break;
                    case HDOP_GE:
                        prepared.append(" >= ");
                        break;
                    case HDOP_EQ:
                        prepared.append(" = ");
                        break;
                    case HDOP_LIKE:
                        prepared.append(" LIKE ");
                        break;
                    case HDOP_NE:
                        prepared.append(" <> ");
                        break;
                    case HDOP_IS_NULL:
                        prepared.append(" IS NULL");
                        continue;
                    case HDOP_IS_NOT_NULL:
                        prepared.append(" IS NOT NULL");
                        continue;
                    default:
                        throw new UnsupportedOperationException("Unsupported Filter operation: " + op);
                }

                // Insert constraint constant
                Object val = filter.getConstant().constant();
                switch (DataType.get(column.columnTypeCode())) {
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                    case FLOAT8:
                    case REAL:
                    case BOOLEAN:
                        prepared.append(val.toString());
                        break;
                    case TEXT:
                        prepared.append("'").append(val.toString()).append("'");
                        break;
                    case DATE:
                        // Date field has different format in different databases
                        prepared.append(dbProduct.wrapDate(val));
                        break;
                    case TIMESTAMP:
                        // Timestamp field has different format in different databases
                        prepared.append(dbProduct.wrapTimestamp(val));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported column type for filtering: " + column.columnTypeCode());
                }
            }

            // No exceptions were thrown, change the provided query
            query.append(prepared);
        }
        catch (UnsupportedOperationException e) {
            LOG.debug("WHERE clause is omitted: " + e.toString());
            // Silence the exception and do not insert constraints
        }
    }

    private RequestContext requestContext;
    private DatabaseMetaData databaseMetaData;
    private String quoteString;
    private List<ColumnDescriptor> columns;
    private String tableName;

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryBuilder.class);
}
