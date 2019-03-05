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

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.plugins.jdbc.writercallable.WriterCallable;
import org.greenplum.pxf.plugins.jdbc.writercallable.WriterCallableFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC tables accessor
 *
 * The SELECT queries are processed by {@link java.sql.Statement}
 *
 * The INSERT queries are processed by {@link java.sql.PreparedStatement} and
 * built-in JDBC batches of arbitrary size
 */
public class JdbcAccessor extends JdbcBasePlugin implements Accessor {
    /**
     * openForRead() implementation
     * Create query, open JDBC connection, execute query and store the result into resultSet
     *
     * @return true if successful
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     * @throws ParseException if th SQL statement provided in PXF RequestContext is incorrect
     * @throws ClassNotFoundException if the JDBC driver was not found
     */
    @Override
    public boolean openForRead() throws SQLException, SQLTimeoutException, ParseException, ClassNotFoundException {
        if (statementRead != null && !statementRead.isClosed()) {
            return true;
        }

        Connection connection = super.getConnection();

        queryRead = new SQLQueryBuilder(context, connection.getMetaData()).buildSelectQuery();

        statementRead = connection.createStatement();
        resultSetRead = statementRead.executeQuery(queryRead);

        return true;
    }

    /**
     * readNextObject() implementation
     * Retreive the next tuple from resultSet and return it
     *
     * @return row
     * @throws SQLException if a problem in resultSet occurs
     */
    @Override
    public OneRow readNextObject() throws SQLException {
        if (resultSetRead.next()) {
            return new OneRow(resultSetRead);
        }
        return null;
    }

    /**
     * closeForRead() implementation
     */
    @Override
    public void closeForRead() {
        JdbcBasePlugin.closeStatement(statementRead);
    }

    /**
     * openForWrite() implementation
     * Create query template and open JDBC connection
     *
     * @return true if successful
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     * @throws ParseException if the SQL statement provided in PXF RequestContext is incorrect
     * @throws ClassNotFoundException if the JDBC driver was not found
     */
    @Override
    public boolean openForWrite() throws SQLException, SQLTimeoutException, ParseException, ClassNotFoundException {
        if (statementWrite != null && !statementWrite.isClosed()) {
            throw new SQLException("The connection to an external database is already open.");
        }

        Connection connection = super.getConnection();

        queryWrite = new SQLQueryBuilder(context, connection.getMetaData()).buildInsertQuery();

        statementWrite = super.getPreparedStatement(connection, queryWrite);

        // Process batchSize
        if (!connection.getMetaData().supportsBatchUpdates()) {
            if ((batchSizeIsSetByUser) && (batchSize > 1)) {
                throw new SQLException("The external database does not support batch updates");
            }
            else {
                batchSize = 1;
            }
        }

        // Process poolSize
        if (poolSize < 1) {
            poolSize = Runtime.getRuntime().availableProcessors();
            LOG.info(
                "The POOL_SIZE is set to the number of CPUs available (" + Integer.toString(poolSize) + ")"
            );
        }
        if (poolSize > 1) {
            executorServiceWrite = Executors.newFixedThreadPool(poolSize);
            poolTasks = new LinkedList<>();
        }

        // Setup WriterCallableFactory
        writerCallableFactory = new WriterCallableFactory();
        writerCallableFactory.setPlugin(this);
        writerCallableFactory.setQuery(queryWrite);
        writerCallableFactory.setBatchSize(batchSize);
        if (poolSize == 1) {
            writerCallableFactory.setStatement(statementWrite);
        }

        writerCallable = writerCallableFactory.get();

        return true;
    }

	/**
     * writeNextObject() implementation
     *
     * If batchSize is not 0 or 1, add a tuple to the batch of statementWrite
     * Otherwise, execute an INSERT query immediately
     *
     * In both cases, a {@link java.sql.PreparedStatement} is used
     *
     * @param row one row
     * @return true if successful
     * @throws SQLException if a database access error occurs
     * @throws IOException if the data provided by {@link JdbcResolver} is corrupted
     * @throws ClassNotFoundException if pooling is used and the JDBC driver was not found
     * @throws IllegalStateException if writerCallableFactory was not properly initialized
     * @throws Exception if it happens in writerCallable.call()
     */
    @Override
    public boolean writeNextObject(OneRow row) throws Exception {
        if (writerCallable == null) {
            throw new IllegalStateException("The JDBC connection was not properly initialized (writerCallable is null)");
        }

        writerCallable.supply(row);
        if (writerCallable.isCallRequired()) {
            if (poolSize > 1) {
                // Pooling is used. Create new writerCallable
                poolTasks.add(executorServiceWrite.submit(writerCallable));
                writerCallable = writerCallableFactory.get();
            }
            else {
                // Pooling is not used
                writerCallable.call();
            }
        }

        return true;
    }

    /**
     * closeForWrite() implementation
     *
     * @throws Exception if it happens in writerCallable.call() or due to runtime errors in thread pool
     */
    @Override
    public void closeForWrite() throws Exception {
        if ((statementWrite == null) || (writerCallable == null)) {
            return;
        }

        try {
            if (poolSize > 1) {
                // Process thread pool
                Exception firstException = null;
                for (Future<SQLException> task : poolTasks) {
                    // We need this construction to ensure that we try to close all connections opened by pool threads
                    try {
                        SQLException currentSqlException = task.get();
                        if (currentSqlException != null) {
                            if (firstException == null) {
                                firstException = currentSqlException;
                            }
                            LOG.error(
                                "A SQLException in a pool thread occured: " + currentSqlException.getClass() + " " + currentSqlException.getMessage()
                            );
                        }
                    }
                    catch (Exception e) {
                        // This exception must have been caused by some thread execution error. However, there may be other exception (maybe of class SQLException) that happened in one of threads that were not examined yet. That is why we do not modify firstException
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                "A runtime exception in a thread pool occured: " + e.getClass() + " " + e.getMessage()
                            );
                        }
                    }
                }
                try {
                    executorServiceWrite.shutdown();
                    executorServiceWrite.shutdownNow();
                }
                catch (Exception e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("executorServiceWrite.shutdown() or .shutdownNow() threw an exception: " + e.getClass() + " " + e.getMessage());
                    }
                }
                if (firstException != null) {
                    throw firstException;
                }
            }

            // Send data that is left
            writerCallable.call();
        }
        finally {
            JdbcBasePlugin.closeStatement(statementWrite);
        }
    }

    // Read variables
    private String queryRead = null;
    private Statement statementRead = null;
    private ResultSet resultSetRead = null;

    // Write variables
    private String queryWrite = null;
    private PreparedStatement statementWrite = null;
    private WriterCallableFactory writerCallableFactory = null;
    private WriterCallable writerCallable = null;
    private ExecutorService executorServiceWrite = null;
    private List<Future<SQLException> > poolTasks = null;

    // Static variables
    private static final Logger LOG = LoggerFactory.getLogger(JdbcAccessor.class);
}
