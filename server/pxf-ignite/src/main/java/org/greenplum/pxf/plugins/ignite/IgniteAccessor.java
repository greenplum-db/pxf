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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

/**
 * PXF-Ignite accessor class
 */
public class IgniteAccessor extends IgniteBasePlugin implements Accessor {
    /**
     * openForRead() implementation
     */
    @Override
    public boolean openForRead() throws Exception {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses(hosts);
        if (user != null) {
            cfg.setUserName(user);
        }
        if (password != null) {
            cfg.setUserPassword(password);
        }
        if (bufferSize > 0) {
            cfg.setReceiveBufferSize(bufferSize);
        }
        cfg.setTcpNoDelay(tcpNodelay);

        SqlFieldsQuery query = buildSelectQuery();
        if (igniteCache != null) {
            query.setSchema(igniteCache);
        }
        query.setLazy(lazy);
        query.setReplicatedOnly(replicatedOnly);

        if (LOG.isDebugEnabled()) {
            LOG.debug("openForRead(): making a request to Ignite. Query: '" + query.getSql() + "'");
        }

        igniteClient = Ignition.startClient(cfg);
        readIgniteCursor = igniteClient.query(query);
        readIterator = readIgniteCursor.iterator();

        if (LOG.isDebugEnabled()) {
            LOG.debug("openForRead() finished successfully");
        }

        return true;
    }

    /**
     * readNextObject() implementation
     */
    @Override
    public OneRow readNextObject() throws Exception {
        return readIterator.hasNext() ? new OneRow(readIterator.next()) : null;
    }

    /**
     * closeForRead() implementation
     *
     * Any number of resources, each of which are to be closed, can be closed by this procedure. Only one exception will be thrown - the last that happened
     */
    @Override
    public void closeForRead() throws Exception {
        Exception exception = null;

        if (readIgniteCursor != null) {
            try {
                readIgniteCursor.close();
                readIgniteCursor = null;
            }
            catch(Exception e) {
                exception = e;
            }
        }
        if (igniteClient != null) {
            try {
                igniteClient.close();
                igniteClient = null;
            }
            catch(Exception e) {
                exception = e;
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * openForWrite() implementation
     */
    @Override
    public boolean openForWrite() throws IllegalArgumentException {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses(hosts);
        if (user != null) {
            cfg.setUserName(user);
        }
        if (password != null) {
            cfg.setUserPassword(password);
        }
        if (bufferSize > 0) {
            cfg.setSendBufferSize(bufferSize);
        }

        writeQuery = buildInsertQuery();
        writeQuery.setSchema(igniteCache);

        if (LOG.isDebugEnabled()) {
            LOG.debug("openForWrite(): connecting to Ignite");
        }

        igniteClient = Ignition.startClient(cfg);

        if (LOG.isDebugEnabled()) {
            LOG.debug("openForWrite() finished successfully");
        }

        return true;
    }

    /**
     * writeNextObject() implementation
     */
    @Override
    public boolean writeNextObject(OneRow currentRow) throws Exception {
        Object[] args = (Object[])currentRow.getData();
        writeQuery.setArgs(args);

        igniteClient.query(writeQuery).getAll();

        return true;
    }

    /**
     * closeForWrite() implementation
     */
    @Override
    public void closeForWrite() throws Exception {
        if (igniteClient != null) {
            igniteClient.close();
            igniteClient = null;
        }
    }

    /**
     * Build a SELECT query using the existing plugin parameters
     * @return a {@link SqlFieldsQuery} (with default settings) containing a SQL query
     * @throws Exception in case plugin parameters are incorrect or {@link WhereSQLBuilder} failed
     */
    private SqlFieldsQuery buildSelectQuery() throws Exception {
        StringBuilder sb = new StringBuilder();

        // Insert a list of fields to be selected
        List<ColumnDescriptor> columns = context.getTupleDescription();
        if (columns == null) {
            throw new IllegalArgumentException("Tuple description must be present.");
        }
        sb.append("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor column = columns.get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(
                quoteColumns ?
                QUOTE + column.columnName() + QUOTE
                : column.columnName()
            );
        }

        // Insert the name of the table to select values from
        sb.append(" FROM ");
        String tableName = context.getDataSource();
        if (tableName == null) {
            throw new IllegalArgumentException("Table name must be set as DataSource.");
        }
        sb.append(tableName);

        // Insert query constraints
        if (context.hasFilter()) {
            WhereSQLBuilder filterBuilder = new WhereSQLBuilder(context, quoteColumns ? QUOTE : null);
            String whereSql = filterBuilder.buildWhereSQL();

            if (whereSql != null) {
                sb.append(" WHERE ").append(whereSql);
            }
        }

        // Insert partition constraints
        IgnitePartitionFragmenter.buildFragmenterSql(context, sb);

        sb.append(";");

        return new SqlFieldsQuery(sb.toString());
    }

    /**
     * Build an INSERT query using the existing plugin parameters
     * @return a {@link SqlFieldsQuery} (with default settings) containing a SQL query with placeholders
     * @throws IllegalArgumentException in case plugin parameters are incorrect
     *
     * This function also checks the external data source (target table name) against a regexp. At the moment there is no other way (except for the usage of user-defined parameters) to get correct name of that table: GPDB inserts extra data into the address, because it is required required by Hadoop.
     * If no extra data is present, the source is left unchanged
     */
    private SqlFieldsQuery buildInsertQuery() throws IllegalArgumentException {
        // Check data source
        String definedSource = context.getDataSource();
        Matcher matcher = writeAddressPattern.matcher(definedSource);
        if (matcher.find()) {
            context.setDataSource(matcher.group(1));
        }

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");

        // Insert table name
        String tableName = context.getDataSource();
        if (tableName == null) {
            throw new IllegalArgumentException("Table name must be set as DataSource.");
        }
        sb.append(tableName);

        // Insert column names
        sb.append("(");
        List<ColumnDescriptor> columns = context.getTupleDescription();
        if (columns == null) {
            throw new IllegalArgumentException("Tuple description must be present.");
        }
        String fieldDivisor = "";
        for (int i = 0; i < columns.size(); i++) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append(
                quoteColumns ?
                QUOTE + columns.get(i).columnName() + QUOTE
                : columns.get(i).columnName()
            );
        }
        sb.append(")");

        sb.append(" VALUES ");

        sb.append("(");
        fieldDivisor = "";
        for (int i = 0; i < columns.size(); i++) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append("?");
        }
        sb.append(")");

        sb.append(";");

        return new SqlFieldsQuery(sb.toString());
    }

    private static final Log LOG = LogFactory.getLog(IgniteAccessor.class);

    // A pattern to cut extra parameters from 'InputData.dataSource' when write operation is performed. See {@link openForWrite()} for the details
    private static final Pattern writeAddressPattern = Pattern.compile("/(.*)/[0-9]*-[0-9]*_[0-9]*");

    // An instance of Ignite thin client, used by both read and write pipelines
    private IgniteClient igniteClient = null;

    // Read pipeline objects
    private FieldsQueryCursor<List<?>> readIgniteCursor = null;
    private Iterator<List<?>> readIterator = null;

    // Write pipeline objects
    private SqlFieldsQuery writeQuery = null;
}
