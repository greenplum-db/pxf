package org.greenplum.pxf.plugins.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.plugins.jdbc.partitioning.IntPartition;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
public class JdbcAccessorTest {

    private Configuration configuration;
    private JdbcAccessor accessor;
    private RequestContext context;

    @Mock
    private ConnectionManager mockConnectionManager;
    @Mock
    private DatabaseMetaData mockMetaData;
    @Mock
    private Connection mockConnection;
    @Mock
    private SecureLogin mockSecureLogin;
    @Mock
    private Statement mockStatement;
    @Mock
    private PreparedStatement mockPreparedStatement;
    @Mock
    private ResultSet mockResultSet;

    @BeforeEach
    public void setup() {

        accessor = new JdbcAccessor(mockConnectionManager, mockSecureLogin);
        configuration = new Configuration();
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("test-table");
        context.setUser("test-user");
        context.setConfiguration(configuration);

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
    }

    @Test
    public void testWriteFailsWhenQueryIsSpecified() {
        context.setDataSource("query:foo");
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> accessor.openForWrite());
        assertEquals("specifying query name in data path is not supported for JDBC writable external tables", e.getMessage());
    }

    @Test
    public void testReadFromQueryFailsWhenServerDirectoryIsNotSpecified() throws SQLException {
        try (MockedStatic<JdbcBasePlugin> jdbcBasePluginMockedStatic = mockStatic(JdbcBasePlugin.class)) {
            wireMocksForRead();
            context.setServerName("unknown");
            context.setDataSource("query:foo");
            accessor.setRequestContext(context);
            accessor.afterPropertiesSet();
            Exception e = assertThrows(PxfRuntimeException.class,
                    () -> accessor.openForRead());
            assertEquals("No server configuration directory found for server unknown", e.getMessage());
            jdbcBasePluginMockedStatic.verify(() -> JdbcBasePlugin.closeConnection(mockConnection));
        }
    }

    @Test
    public void testReadFromQueryFailsWhenServerDirectoryDoesNotExist() throws SQLException {
        try (MockedStatic<JdbcBasePlugin> jdbcBasePluginMockedStatic = mockStatic(JdbcBasePlugin.class)) {
            wireMocksForRead();
            configuration.set("pxf.config.server.directory", "/non-existing-directory");
            context.setDataSource("query:foo");
            accessor.setRequestContext(context);
            accessor.afterPropertiesSet();
            Exception e = assertThrows(RuntimeException.class,
                    () -> accessor.openForRead());
            assertEquals("Failed to read text of query foo : File '/non-existing-directory/foo.sql' does not exist", e.getMessage());
            jdbcBasePluginMockedStatic.verify(() -> JdbcBasePlugin.closeConnection(mockConnection));
        }
    }

    @Test
    public void testReadFromQueryFailsWhenQueryFileIsNotFoundInExistingDirectory() throws SQLException {
        try (MockedStatic<JdbcBasePlugin> jdbcBasePluginMockedStatic = mockStatic(JdbcBasePlugin.class)) {
            wireMocksForRead();
            configuration.set("pxf.config.server.directory", "/tmp/");
            context.setDataSource("query:foo");
            accessor.setRequestContext(context);
            accessor.afterPropertiesSet();
            Exception e = assertThrows(RuntimeException.class,
                    () -> accessor.openForRead());
            assertEquals("Failed to read text of query foo : File '/tmp/foo.sql' does not exist", e.getMessage());
            jdbcBasePluginMockedStatic.verify(() -> JdbcBasePlugin.closeConnection(mockConnection));
        }
    }

    @Test
    public void testReadFromQueryFailsWhenQueryFileIsEmpty() throws Exception {
        try (MockedStatic<JdbcBasePlugin> jdbcBasePluginMockedStatic = mockStatic(JdbcBasePlugin.class)) {
            wireMocksForRead();
            String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
            configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
            context.setDataSource("query:emptyquery");
            accessor.setRequestContext(context);
            accessor.afterPropertiesSet();
            Exception e = assertThrows(RuntimeException.class,
                    () -> accessor.openForRead());
            assertEquals("Query text file is empty for query emptyquery", e.getMessage());
            jdbcBasePluginMockedStatic.verify(() -> JdbcBasePlugin.closeConnection(mockConnection));
        }
    }

    @Test
    public void testReadFromQuery() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        wireMocksForReadWithCreateStatement();

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
                "FROM dept JOIN emp\n" +
                "ON dept.id = emp.dept_id\n" +
                "GROUP BY dept.name) pxfsubquery";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testReadFromQueryWithPreparedStatement() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        configuration.set("jdbc.read.prepared-statement", "true");
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        ArgumentCaptor<String> queryPassed = wireMocksForReadWithPrepareStatement();

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
                "FROM dept JOIN emp\n" +
                "ON dept.id = emp.dept_id\n" +
                "GROUP BY dept.name) pxfsubquery";

        assertEquals(expected, queryPassed.getValue());
        verify(mockPreparedStatement, times(1)).executeQuery();
    }

    @Test
    public void testReadFromQueryEndingInSemicolon() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquerywithsemicolon");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        wireMocksForReadWithCreateStatement();

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
                "FROM dept JOIN emp\n" +
                "ON dept.id = emp.dept_id\n" +
                "GROUP BY dept.name) pxfsubquery";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testReadFromQueryWithValidSemicolon() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquerywithvalidsemicolon");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        wireMocksForReadWithCreateStatement();

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
                "FROM dept JOIN emp\n" +
                "ON dept.id = emp.dept_id\n" +
                "WHERE dept.name LIKE '%;%'\n" +
                "GROUP BY dept.name) pxfsubquery";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testReadFromQueryWithPartitions() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        context.addOption("PARTITION_BY", "count:int");
        context.addOption("RANGE", "1:10");
        context.addOption("INTERVAL", "1");
        context.setFragmentMetadata(new IntPartition("count", 1L, 2L));
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        wireMocksForReadWithCreateStatement();

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
                "FROM dept JOIN emp\n" +
                "ON dept.id = emp.dept_id\n" +
                "GROUP BY dept.name) pxfsubquery WHERE count >= 1 AND count < 2";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testReadFromQueryWithWhereWithPartitions() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquerywithwhere");
        context.addOption("PARTITION_BY", "count:int");
        context.addOption("RANGE", "1:10");
        context.addOption("INTERVAL", "1");
        context.setFragmentMetadata(new IntPartition("count", 1L, 2L));
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        wireMocksForReadWithCreateStatement();

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
                "FROM dept JOIN emp\n" +
                "ON dept.id = emp.dept_id\n" +
                "WHERE dept.id < 10\n" +
                "GROUP BY dept.name) pxfsubquery WHERE count >= 1 AND count < 2";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testGetFragmentsAndReadFromQueryWithPartitions() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        context.addOption("PARTITION_BY", "count:int");
        context.addOption("RANGE", "1:10");
        context.addOption("INTERVAL", "1");

        JdbcPartitionFragmenter fragmenter = new JdbcPartitionFragmenter();
        fragmenter.setRequestContext(context);
        fragmenter.afterPropertiesSet();
        context.setFragmentMetadata(fragmenter.getFragments().get(2).getMetadata());

        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        wireMocksForReadWithCreateStatement();

        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForRead();

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
                "FROM dept JOIN emp\n" +
                "ON dept.id = emp.dept_id\n" +
                "GROUP BY dept.name) pxfsubquery WHERE count >= 1 AND count < 2";
        assertEquals(expected, queryPassed.getValue());
    }

    private void wireMocksForReadWithCreateStatement() throws SQLException {
        wireMocksForRead();
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Greenplum");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");
    }

    private ArgumentCaptor<String> wireMocksForReadWithPrepareStatement() throws SQLException {
        wireMocksForRead();
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockConnection.prepareStatement(queryPassed.capture())).thenReturn(mockPreparedStatement);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Greenplum");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        return queryPassed;
    }

    private void wireMocksForRead() throws SQLException {
        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    }
}
