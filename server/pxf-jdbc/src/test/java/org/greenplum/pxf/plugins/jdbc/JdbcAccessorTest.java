package org.greenplum.pxf.plugins.jdbc;

import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DriverManager.class, JdbcAccessor.class})
public class JdbcAccessorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JdbcAccessor accessor;
    private RequestContext context;

    private Statement mockStatement;
    private ResultSet mockResultSet;

    @Before
    public void setup() throws SQLException {
        accessor = new JdbcAccessor();
        context = new RequestContext();
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        context.setAdditionalConfigProps(additionalProps);

        PowerMockito.mockStatic(DriverManager.class);
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        Connection mockConnection = mock(Connection.class);
        mockStatement = mock(Statement.class);
        mockResultSet = mock(ResultSet.class);

        when(DriverManager.getConnection(anyString(), anyObject())).thenReturn(mockConnection);
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
    public void testReadFromQuery() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        context.getAdditionalConfigProps().put("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);

        accessor.initialize(context);
        accessor.openForRead();

        StringBuilder b = new StringBuilder()
        .append("SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n")
        .append("FROM dept JOIN emp\n")
        .append("ON dept.id = emp.dept_id\n")
        .append("GROUP BY dept.name) AS source");

        assertEquals(b.toString(), queryPassed.getValue());

    }

}
