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
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.util.List;
import java.util.regex.Pattern;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;

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
    public SQLQueryBuilder(RequestContext context, Configuration configuration, DatabaseMetaData metaData) throws SQLException {
        if (context == null) {
            throw new IllegalArgumentException("Provided RequestContext is null");
        }
        requestContext = context;
        if (configuration == null) {
            throw new IllegalArgumentException("Provided Configuration is null");
        }
        requestConfiguration = configuration;
        if (metaData == null) {
            throw new IllegalArgumentException("Provided DatabaseMetaData is null");
        }
        databaseMetaData = metaData;

        tableName = requestContext.getDataSource();
        columns = requestContext.getTupleDescription();
        dbProduct = DbProduct.getDbProduct(databaseMetaData.getDatabaseProductName());
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
        String columnsQuery = columns.stream()
                .filter(ColumnDescriptor::isProjected)
                .map(c -> quoteString + c.columnName() + quoteString)
                .collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder("SELECT ")
                .append(columnsQuery)
                .append(" FROM ")
                .append(tableName);

        // Insert regular WHERE constraints
        buildWhereSQL(sb);

        // Insert partition constraints
        JdbcPartitionFragmenter.buildFragmenterSql(
            requestContext, requestConfiguration,
            dbProduct, quoteString,
            sb
        );

        if (LOG.isDebugEnabled()) {
            LOG.debug(sb.toString());
        }

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
        String columnsQuery = columns.stream()
                .map(c -> quoteString + c.columnName() + quoteString)
                .collect(Collectors.joining(", "));

        String placeholdersQuery = Stream.generate(() -> "?").limit(columns.size())
                .collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder("INSERT")
                .append(" INTO ")
                .append(tableName)
                .append("(").append(columnsQuery).append(")")
                .append(" VALUES ")
                .append("(").append(placeholdersQuery).append(")");

        if (LOG.isDebugEnabled()) {
            LOG.debug(sb.toString());
        }

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
     * @param query SQL query to insert constraints to. The query may may contain other WHERE statements
     *
     * @throws ParseException if filter string is invalid
     */
    private void buildWhereSQL(StringBuilder query) throws ParseException {
        if (!requestContext.hasFilter()) {
            return;
        }

        try {
            String whereSQL = JdbcFilterParser.parseFilters(requestContext.getFilterString()).stream()
                    .map(f -> resolveConstraint(f))
                    .collect(Collectors.joining(" AND "));

            // No exceptions were thrown, change the provided query
            if (!query.toString().toUpperCase().contains("WHERE")) {
                query.append(" WHERE ").append(whereSQL);
            }
            else {
                query.append(" AND ").append(whereSQL);
            }
        }
        catch (UnsupportedOperationException e) {
            LOG.debug(String.format(
                "WHERE clause is omitted: %s",
                e.toString()
            ));
            // Silence the exception and do not insert constraints
        }
    }

    /**
     * Resolve constraint into a constraint string
     *
     * @param filter {@link BasicFilter}
     *
     * @throws UnsupportedOperationException if filter is not supported
     */
    private String resolveConstraint(BasicFilter filter) throws UnsupportedOperationException {
        ColumnDescriptor column = requestContext.getColumn(filter.getColumn().index());

        StringBuilder result = new StringBuilder()
                .append(quoteString)
                .append(column.columnName())
                .append(quoteString);

        // Insert constraint operator
        switch (filter.getOperation()) {
            case HDOP_LT:
                result.append(" < ");
                break;
            case HDOP_GT:
                result.append(" > ");
                break;
            case HDOP_LE:
                result.append(" <= ");
                break;
            case HDOP_GE:
                result.append(" >= ");
                break;
            case HDOP_EQ:
                result.append(" = ");
                break;
            case HDOP_LIKE:
                result.append(" LIKE ");
                break;
            case HDOP_NE:
                result.append(" <> ");
                break;
            case HDOP_IS_NULL:
                result.append(" IS NULL");
                return result.toString();
            case HDOP_IS_NOT_NULL:
                result.append(" IS NOT NULL");
                return result.toString();
            default:
                throw new UnsupportedOperationException(String.format(
                    "Unsupported constraint operator %s in column %s",
                    filter.getOperation().toString(),
                    column
                ));
        }

        // Insert constraint constant
        Object val = filter.getConstant().constant();
        if (val == null) {
            // NULL should be checked with 'IS [NOT] NULL'.
            throw new UnsupportedOperationException(String.format(
                "NULL constraint constant in column %s",
                column
            ));
        }
        switch (DataType.get(column.columnTypeCode())) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT8:
            case REAL:
            case BOOLEAN:
                result.append(val.toString());
                break;
            case TEXT:
                result.append("'").append(val.toString()).append("'");
                break;
            case DATE:
                // Date field has different format in different databases
                result.append(dbProduct.wrapDate(val));
                break;
            case TIMESTAMP:
                // Timestamp field has different format in different databases
                result.append(dbProduct.wrapTimestamp(val));
                break;
            default:
                throw new UnsupportedOperationException(String.format(
                    "Unsupported constraint type %s in column %s",
                    DataType.get(column.columnTypeCode()).toString(),
                    column
                ));
        }

        return result.toString();
    }

    private RequestContext requestContext;
    private Configuration requestConfiguration;
    private DatabaseMetaData databaseMetaData;

    private String tableName;
    private List<ColumnDescriptor> columns;
    private DbProduct dbProduct;
    private String quoteString;

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryBuilder.class);
}
