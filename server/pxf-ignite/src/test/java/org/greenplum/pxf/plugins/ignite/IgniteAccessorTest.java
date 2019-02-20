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

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest({Ignition.class})
public class IgniteAccessorTest {
    private static final List<ColumnDescriptor> testColumns = Arrays.asList(
        new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null),
        new ColumnDescriptor("name", DataType.TEXT.getOID(), 1, "text", null),
        new ColumnDescriptor("birthday", DataType.DATE.getOID(), 2, "date", null)
    );
    private static final String testInsertQueryBase = "INSERT INTO TableTest(id, name, birthday) VALUES (?, ?, ?);";
    private static final String testSelectQuery = "SELECT id, name, birthday FROM TableTest;";

    @Mock
    private RequestContext requestContext;
    @Mock
    private IgniteClient igniteClient;
    @Mock
    private FieldsQueryCursor<List<?>> cursor;

    @Before
    public void setup() throws Exception {
        when(requestContext.getDataSource()).thenReturn("TableTest");
        when(requestContext.getOption("IGNITE_HOST")).thenReturn("0.0.0.0");
        when(requestContext.hasFilter()).thenReturn(false);
        when(requestContext.getTupleDescription()).thenReturn(testColumns);
        when(requestContext.getColumn(0)).thenReturn(testColumns.get(0));
        when(requestContext.getColumn(1)).thenReturn(testColumns.get(1));
        when(requestContext.getColumn(2)).thenReturn(testColumns.get(2));

        when(cursor.getAll()).thenReturn(null);

        when(igniteClient.query(any(SqlFieldsQuery.class))).thenReturn(cursor);

        PowerMockito.mockStatic(Ignition.class);
        PowerMockito.when(Ignition.startClient(any(ClientConfiguration.class))).thenReturn(igniteClient);
    }

    @Test
    public void testSelect() throws Exception {
        // Initialize accessor
        IgniteAccessor acc = PowerMockito.spy(new IgniteAccessor());
        acc.initialize(requestContext);

        // Prepare resultSetMock
        List<List<?>> resultSetMock = new ArrayList<List<?>>();
        Object[] resultRow = new Object[3];
        resultRow[0] = Object.class.cast(new Integer(1));
        resultRow[1] = Object.class.cast(new String("Mocked name"));
        resultRow[2] = Object.class.cast(new Date(946674000));
        resultSetMock.add(Arrays.asList(resultRow.clone()));
        resultRow[0] = Object.class.cast(new Integer(2));
        resultSetMock.add(Arrays.asList(resultRow.clone()));
        resultRow[0] = Object.class.cast(new Integer(3));
        resultSetMock.add(Arrays.asList(resultRow.clone()));

        when(cursor.iterator()).thenReturn(resultSetMock.iterator());

        // Conduct test
        OneRow rows[] = new OneRow[3];
        acc.openForRead();
        rows[0] = acc.readNextObject();
        rows[1] = acc.readNextObject();
        rows[2] = acc.readNextObject();
        Object rows_4 = acc.readNextObject();
        acc.closeForRead();

        // Check calls
        ArgumentCaptor<SqlFieldsQuery> captor = ArgumentCaptor.forClass(SqlFieldsQuery.class);
        verify(igniteClient, times(1)).query(captor.capture());
        assertEquals(new SqlFieldsQuery(testSelectQuery).toString(), captor.getValue().toString());

        // Check results
        assertNull(rows_4);
        assertEquals(new Integer(1), Integer.class.cast(List.class.cast(rows[0].getData()).get(0)));
        assertEquals(resultRow[1], String.class.cast(List.class.cast(rows[0].getData()).get(1)));
        assertEquals(resultRow[2], Date.class.cast(List.class.cast(rows[0].getData()).get(2)));
        assertEquals(new Integer(2), Integer.class.cast(List.class.cast(rows[1].getData()).get(0)));
        assertEquals(new Integer(3), Integer.class.cast(List.class.cast(rows[2].getData()).get(0)));
    }

    @Test
    public void testInsert() throws Exception {
        // Initialize accessor
        IgniteAccessor acc = PowerMockito.spy(new IgniteAccessor());
        acc.initialize(requestContext);

        // Prepare objects to be inserted
        Object[] insertRow = new Object[3];
        OneRow[] insertOneRows = new OneRow[2];
        SqlFieldsQuery[] insertQueries = new SqlFieldsQuery[2];
        insertRow[0] = Object.class.cast(new Integer(1));
        insertRow[1] = Object.class.cast(new String("Mocked name"));
        insertRow[2] = Object.class.cast(new Date(946674000));
        insertOneRows[0] = new OneRow(insertRow);
        insertQueries[0] = new SqlFieldsQuery(testInsertQueryBase);
        insertQueries[0].setArgs(insertRow);
        insertRow[0] = Object.class.cast(new Integer(2));
        insertOneRows[1] = new OneRow(insertRow);
        insertQueries[1] = new SqlFieldsQuery(testInsertQueryBase);
        insertQueries[1].setArgs(insertRow);

        // Conduct test
        acc.openForWrite();
        acc.writeNextObject(insertOneRows[0]);
        acc.writeNextObject(insertOneRows[1]);
        acc.closeForWrite();

        // Check calls
        ArgumentCaptor<SqlFieldsQuery> captor = ArgumentCaptor.forClass(SqlFieldsQuery.class);
        verify(igniteClient, times(2)).query(captor.capture());
        List<SqlFieldsQuery> captorResults = captor.getAllValues();
        assertEquals(insertQueries[0].toString(), captorResults.get(0).toString());
        assertEquals(insertQueries[1].toString(), captorResults.get(1).toString());
    }
}
