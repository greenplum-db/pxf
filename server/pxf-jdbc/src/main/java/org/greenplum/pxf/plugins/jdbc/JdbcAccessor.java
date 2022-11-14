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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.greenplum.pxf.plugins.jdbc.writercallable.WriterCallable;
import org.greenplum.pxf.plugins.jdbc.writercallable.WriterCallableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * JDBC tables accessor
 * <p>
 * The SELECT queries are processed by {@link java.sql.Statement}
 * <p>
 * The INSERT queries are processed by {@link java.sql.PreparedStatement} and
 * built-in JDBC batches of arbitrary size
 */
public class JdbcAccessor extends JdbcBasePlugin implements Accessor {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcAccessor.class);

    private static final String JDBC_READ_PREPARED_STATEMENT_PROPERTY_NAME = "jdbc.read.prepared-statement";

    private Statement statementRead = null;
    private ResultSet resultSetRead = null;

    private PreparedStatement statementWrite = null;
    private WriterCallableFactory writerCallableFactory = null;
    private WriterCallable writerCallable = null;
    private ExecutorService executorServiceWrite = null;
    private List<Future<SQLException>> poolTasks = null;

    /**
     * Creates a new instance of the JdbcAccessor
     */
    public JdbcAccessor() {
        super();
    }

    /**
     * Creates a new instance of accessor with provided connection manager.
     *
     * @param connectionManager connection manager
     * @param secureLogin       the instance of the secure login
     */
    JdbcAccessor(ConnectionManager connectionManager, SecureLogin secureLogin) {
        super(connectionManager, secureLogin);
    }

    /**
     * openForRead() implementation
     * Create query, open JDBC connection, execute query and store the result into resultSet
     *
     * @return true if successful
     * @throws SQLException        if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     */
    @Override
    public boolean openForRead() throws SQLException, SQLTimeoutException {
        if (statementRead != null && !statementRead.isClosed()) {
            return true;
        }

        Connection connection = super.getConnection();
        try {
            return openForReadInner(connection);
        } catch (Throwable e) {
            if (statementRead == null) {
                closeConnection(connection);
            }
            throw new PxfRuntimeException(e.getMessage(), e);
        }
    }

    private boolean openForReadInner(Connection connection) throws SQLException {
        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(context, connection.getMetaData(), getQueryText());

        // Build SELECT query
        if (quoteColumns == null) {
            sqlQueryBuilder.autoSetQuoteString();
        } else if (quoteColumns) {
            sqlQueryBuilder.forceSetQuoteString();
        }
        // Read variables
        String queryRead = sqlQueryBuilder.buildSelectQuery();
        LOG.trace("Select query: {}", queryRead);

        // Execute queries
        // Certain features of third-party JDBC drivers may require the use of a PreparedStatement, even if there are no

        // bind parameters. For example, Teradata's FastExport only works with PreparedStatements
        // https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BGBFBBEG
        boolean usePreparedStatement = parseJdbcUsePreparedStatementProperty();
        if (usePreparedStatement) {
            LOG.debug("Using a PreparedStatement instead of a Statement because {} was set to true", JDBC_READ_PREPARED_STATEMENT_PROPERTY_NAME);
        }
        statementRead = usePreparedStatement ?
                connection.prepareStatement(queryRead) :
                connection.createStatement();

        statementRead.setFetchSize(fetchSize);

        if (queryTimeout != null) {
            LOG.debug("Setting query timeout to {} seconds", queryTimeout);
            statementRead.setQueryTimeout(queryTimeout);
        }

        resultSetRead = usePreparedStatement ?
                ((PreparedStatement) statementRead).executeQuery() :
                statementRead.executeQuery(queryRead);

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
    public void closeForRead() throws SQLException {
        closeStatementAndConnection(statementRead);
    }

    /**
     * openForWrite() implementation
     * Create query template and open JDBC connection
     *
     * @return true if successful
     * @throws SQLException        if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     */
    @Override
    public boolean openForWrite() throws SQLException, SQLTimeoutException {
        if (queryName != null) {
            throw new IllegalArgumentException("specifying query name in data path is not supported for JDBC writable external tables");
        }

        if (statementWrite != null && !statementWrite.isClosed()) {
            throw new SQLException("The connection to an external database is already open.");
        }

        Connection connection = super.getConnection();
        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(context, connection.getMetaData());

        // Build INSERT query
        if (quoteColumns == null) {
            sqlQueryBuilder.autoSetQuoteString();
        } else if (quoteColumns) {
            sqlQueryBuilder.forceSetQuoteString();
        }
        // Write variables
        String queryWrite = sqlQueryBuilder.buildInsertQuery();
        LOG.trace("Insert query: {}", queryWrite);

        statementWrite = super.getPreparedStatement(connection, queryWrite);

        // Process batchSize
        if (!connection.getMetaData().supportsBatchUpdates()) {
            if ((batchSizeIsSetByUser) && (batchSize > 1)) {
                throw new SQLException("The external database does not support batch updates");
            } else {
                batchSize = 1;
            }
        }

        // Process poolSize
        if (poolSize < 1) {
            poolSize = Runtime.getRuntime().availableProcessors();
            LOG.info("The POOL_SIZE is set to the number of CPUs available ({})", poolSize);
        }
        if (poolSize > 1) {
            executorServiceWrite = Executors.newFixedThreadPool(poolSize);
            poolTasks = new LinkedList<>();
        }

        // Setup WriterCallableFactory
        writerCallableFactory = new WriterCallableFactory(this, queryWrite, statementWrite, batchSize, poolSize);

        writerCallable = writerCallableFactory.get();

        return true;
    }

    /**
     * writeNextObject() implementation
     * <p>
     * If batchSize is not 0 or 1, add a tuple to the batch of statementWrite
     * Otherwise, execute an INSERT query immediately
     * <p>
     * In both cases, a {@link java.sql.PreparedStatement} is used
     *
     * @param row one row
     * @return true if successful
     * @throws SQLException           if a database access error occurs
     * @throws IOException            if the data provided by {@link JdbcResolver} is corrupted
     * @throws ClassNotFoundException if pooling is used and the JDBC driver was not found
     * @throws IllegalStateException  if writerCallableFactory was not properly initialized
     * @throws Exception              if it happens in writerCallable.call()
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
            } else {
                // Pooling is not used, call directly and process potential error
                SQLException e = writerCallable.call();
                if (e != null) {
                    throw e;
                }
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
                                    "A SQLException in a pool thread occurred: " + currentSqlException.getClass() + " " + currentSqlException.getMessage()
                            );
                        }
                    } catch (Exception e) {
                        // This exception must have been caused by some thread execution error. However, there may be other exception (maybe of class SQLException) that happened in one of threads that were not examined yet. That is why we do not modify firstException
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "A runtime exception in a thread pool occurred: " + e.getClass() + " " + e.getMessage()
                            );
                        }
                    }
                }
                try {
                    executorServiceWrite.shutdown();
                    executorServiceWrite.shutdownNow();
                } catch (Exception e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("executorServiceWrite.shutdown() or .shutdownNow() threw an exception: " + e.getClass() + " " + e.getMessage());
                    }
                }
                if (firstException != null) {
                    throw firstException;
                }
            }

            // Send data that is left
            SQLException e = writerCallable.call();
            if (e != null) {
                throw e;
            }
        } finally {
            closeStatementAndConnection(statementWrite);
        }
    }


    /**
     * Gets the text of the query by reading the file from the server configuration directory. The name of the file
     * is expected to be the same as the name of the query provided by the user and have extension ".sql"
     *
     * @return text of the query
     */
    private String getQueryText() {
        if (StringUtils.isBlank(queryName)) {
            return null;
        }
        // read the contents of the file holding the text of the query with a given name
        String serverDirectory = context.getConfiguration().get(ConfigurationFactory.PXF_CONFIG_SERVER_DIRECTORY_PROPERTY);
        if (StringUtils.isBlank(serverDirectory)) {
            throw new IllegalStateException("No server configuration directory found for server " + context.getServerName());
        }

        String queryText;
        try {
            File queryFile = new File(serverDirectory, queryName + ".sql");
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reading text of query={} from {}", queryName, queryFile.getCanonicalPath());
            }
            queryText = FileUtils.readFileToString(queryFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to read text of query %s : %s", queryName, e.getMessage()), e);
        }
        if (StringUtils.isBlank(queryText)) {
            throw new RuntimeException(String.format("Query text file is empty for query %s", queryName));
        }

        // Remove one or more semicolons followed by optional blank space
        // happening at the end of the query
        queryText = queryText.replaceFirst("(;+\\s*)+$", "");

        return queryText;
    }

    private boolean parseJdbcUsePreparedStatementProperty() {
        return Utilities.parseBooleanProperty(configuration, JDBC_READ_PREPARED_STATEMENT_PROPERTY_NAME, false);
    }
}
