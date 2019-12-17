package org.greenplum.pxf.plugins.jdbc;

import org.apache.commons.lang.SerializationUtils;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.jdbc.partitioning.PartitionType;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JdbcAccessorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JdbcAccessor accessor;
    private RequestContext context;

    @Mock
    private ConnectionManager mockConnectionManager;
    @Mock
    private DatabaseMetaData mockMetaData;
    @Mock
    private Connection mockConnection;
    @Mock
    private Statement mockStatement;
    @Mock
    private ResultSet mockResultSet;

    @Before
    public void setup() throws SQLException {

        accessor = new JdbcAccessor(mockConnectionManager);
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        context.setAdditionalConfigProps(additionalProps);
        context.setUser("test-user");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Greenplum");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");
    }

    @Test
    public void testWriteFailsWhenQueryIsSpecified() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("specifying query name in data path is not supported for JDBC writable external tables");
        context.setDataSource("query:foo");
        accessor.initialize(context);
        accessor.openForWrite();
    }

    @Test
    public void testReadFromQueryFailsWhenServerDirectoryIsNotSpecified() throws Exception {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("No server configuration directory found for server unknown");
        context.setServerName("unknown");
        context.setDataSource("query:foo");
        accessor.initialize(context);
        accessor.openForRead();
    }

    @Test
    public void testReadFromQueryFailsWhenServerDirectoryDoesNotExist() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Failed to read text of query foo : File '/non-existing-directory/foo.sql' does not exist");
        context.getAdditionalConfigProps().put("pxf.config.server.directory", "/non-existing-directory");
        context.setDataSource("query:foo");
        accessor.initialize(context);
        accessor.openForRead();
    }

    @Test
    public void testReadFromQueryFailsWhenQueryFileIsNotFoundInExistingDirectory() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Failed to read text of query foo : File '/tmp/foo.sql' does not exist");
        context.getAdditionalConfigProps().put("pxf.config.server.directory", "/tmp/");
        context.setDataSource("query:foo");
        accessor.initialize(context);
        accessor.openForRead();
    }

    @Test
    public void testReadFromQueryFailsWhenQueryFileIsEmpty() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Query text file is empty for query emptyquery");
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        context.getAdditionalConfigProps().put("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:emptyquery");
        accessor.initialize(context);
        accessor.openForRead();
    }

    @Test
    public void testReadFromQuery() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        context.getAdditionalConfigProps().put("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);

        accessor.initialize(context);
        accessor.openForRead();

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
                "FROM dept JOIN emp\n" +
                "ON dept.id = emp.dept_id\n" +
                "GROUP BY dept.name) pxfsubquery";
        assertEquals(expected, queryPassed.getValue());

    }

    @Test
    public void testReadFromQueryEndingInSemicolon() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        context.getAdditionalConfigProps().put("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquerywithsemicolon");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);

        accessor.initialize(context);
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
        context.getAdditionalConfigProps().put("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquerywithvalidsemicolon");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);

        accessor.initialize(context);
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
        context.getAdditionalConfigProps().put("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        context.addOption("PARTITION_BY", "count:int");
        context.addOption("RANGE", "1:10");
        context.addOption("INTERVAL", "1");
        context.setFragmentMetadata(SerializationUtils.serialize(PartitionType.INT.getFragmentsMetadata("count", "1:10", "1").get(2)));
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);

        accessor.initialize(context);
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
        context.getAdditionalConfigProps().put("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquerywithwhere");
        context.addOption("PARTITION_BY", "count:int");
        context.addOption("RANGE", "1:10");
        context.addOption("INTERVAL", "1");
        context.setFragmentMetadata(SerializationUtils.serialize(PartitionType.INT.getFragmentsMetadata("count", "1:10", "1").get(2)));
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);

        accessor.initialize(context);
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
        context.getAdditionalConfigProps().put("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        context.addOption("PARTITION_BY", "count:int");
        context.addOption("RANGE", "1:10");
        context.addOption("INTERVAL", "1");

        JdbcPartitionFragmenter fragmenter = new JdbcPartitionFragmenter();
        fragmenter.initialize(context);
        context.setFragmentMetadata(fragmenter.getFragments().get(2).getMetadata());

        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);

        accessor.initialize(context);
        accessor.openForRead();

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
                "FROM dept JOIN emp\n" +
                "ON dept.id = emp.dept_id\n" +
                "GROUP BY dept.name) pxfsubquery WHERE count >= 1 AND count < 2";
        assertEquals(expected, queryPassed.getValue());
    }
}
