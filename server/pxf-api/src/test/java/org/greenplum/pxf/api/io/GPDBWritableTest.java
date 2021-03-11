package org.greenplum.pxf.api.io;

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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;

@ExtendWith(MockitoExtension.class)
public class GPDBWritableTest {

    private DataInput inputStream;
    private DataOutput mockOutput;

    /*
     * Test the readFields method: empty stream
     */
    @Test
    public void testReadFieldsEmpty() throws Exception {

        GPDBWritable gpdbWritable = buildGPDBWritable();

        int[] empty = new int[0];
        buildStream(empty, true);

        gpdbWritable.readFields(inputStream);

        assertTrue(gpdbWritable.isEmpty());
    }

    /*
     * Test the readFields method: first int -1
     */
    @Test
    public void testReadFieldsFirstIntMinusOne() throws Exception {

        GPDBWritable gpdbWritable = buildGPDBWritable();

        int[] firstInt = new int[]{-1};
        buildStream(firstInt, false);

        gpdbWritable.readFields(inputStream);

        assertTrue(gpdbWritable.isEmpty());
    }

    /*
     * Test the readFields method: first int ok (negative and positive numbers)
     */
    @Test
    public void testReadFieldsFirstIntOK() throws Exception {
        GPDBWritable gpdbWritable = buildGPDBWritable();

        int[] firstInt = new int[]{-2};
        buildStream(firstInt, true);
        when(inputStream.readShort()).thenThrow(new EOFException());

        try {
            gpdbWritable.readFields(inputStream);
        } catch (EOFException e) {
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }

        assertFalse(gpdbWritable.isEmpty()); // len < 0

        firstInt = new int[]{8};
        buildStream(firstInt, true);
        when(inputStream.readShort()).thenThrow(new EOFException());

        try {
            gpdbWritable.readFields(inputStream);
        } catch (EOFException e) {
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }
        assertFalse(gpdbWritable.isEmpty()); // len > 0
    }

    @Test
    public void testGetType() {
        String typeName = GPDBWritable.getTypeName(-1);
        assertEquals(typeName, DataType.TEXT.name());

        typeName = GPDBWritable.getTypeName(-7777);
        assertEquals(typeName, DataType.TEXT.name());

        typeName = GPDBWritable.getTypeName(DataType.BOOLEAN.getOID());
        assertEquals(typeName, DataType.BOOLEAN.name());

        typeName = GPDBWritable.getTypeName(DataType.BYTEA.getOID());
        assertEquals(typeName, DataType.BYTEA.name());

        typeName = GPDBWritable.getTypeName(DataType.BIGINT.getOID());
        assertEquals(typeName, DataType.BIGINT.name());

        typeName = GPDBWritable.getTypeName(DataType.SMALLINT.getOID());
        assertEquals(typeName, DataType.SMALLINT.name());

        typeName = GPDBWritable.getTypeName(DataType.INTEGER.getOID());
        assertEquals(typeName, DataType.INTEGER.name());

        typeName = GPDBWritable.getTypeName(DataType.TEXT.getOID());
        assertEquals(typeName, DataType.TEXT.name());

        typeName = GPDBWritable.getTypeName(DataType.REAL.getOID());
        assertEquals(typeName, DataType.REAL.name());

        typeName = GPDBWritable.getTypeName(DataType.FLOAT8.getOID());
        assertEquals(typeName, DataType.FLOAT8.name());

        typeName = GPDBWritable.getTypeName(DataType.BPCHAR.getOID());
        assertEquals(typeName, DataType.BPCHAR.name());

        typeName = GPDBWritable.getTypeName(DataType.VARCHAR.getOID());
        assertEquals(typeName, DataType.VARCHAR.name());

        typeName = GPDBWritable.getTypeName(DataType.DATE.getOID());
        assertEquals(typeName, DataType.DATE.name());

        typeName = GPDBWritable.getTypeName(DataType.TIME.getOID());
        assertEquals(typeName, DataType.TIME.name());

        typeName = GPDBWritable.getTypeName(DataType.TIMESTAMP.getOID());
        assertEquals(typeName, DataType.TIMESTAMP.name());

        typeName = GPDBWritable.getTypeName(DataType.NUMERIC.getOID());
        assertEquals(typeName, DataType.NUMERIC.name());
    }

    @Test
    public void testWriteBytes() throws IOException {
        int[] schema = new int[2];
        schema[0] = DataType.INTEGER.getOID();
        schema[1] = DataType.VARCHAR.getOID();

        GPDBWritable gw = new GPDBWritable(schema, StandardCharsets.UTF_8);
        gw.setInt(0, 123);
        gw.setString(1, "hello");

        mockOutput = mock(DataOutput.class);
        long bytes = gw.write(mockOutput);
        assertEquals(32, bytes);

        InOrder inOrder = inOrder(mockOutput);
        // total length
        inOrder.verify(mockOutput).writeInt(32);
        // version
        inOrder.verify(mockOutput).writeShort(2);
        // error flag
        inOrder.verify(mockOutput).writeByte(0);
        // number of columns
        inOrder.verify(mockOutput).writeShort(2);
        // column type
        inOrder.verify(mockOutput).writeByte(3);
        // column type
        inOrder.verify(mockOutput).writeByte(7);
        // null bits
        inOrder.verify(mockOutput).write(new byte[]{0});
        // integer column value
        inOrder.verify(mockOutput).writeInt(123);
        // varchar column value
        inOrder.verify(mockOutput).writeInt(6);
        inOrder.verify(mockOutput).write(new byte[]{104, 101, 108, 108, 111, 0});
        // padding
        inOrder.verify(mockOutput).write(any(), eq(0), eq(6));
        inOrder.verifyNoMoreInteractions();
    }

    /*
     * helpers functions
     */
    private GPDBWritable buildGPDBWritable() {
        return new GPDBWritable(StandardCharsets.UTF_8);
    }

    // add data to stream, end with EOFException on demand.
    private DataInput buildStream(int[] data, boolean throwException) throws Exception {
        inputStream = mock(DataInput.class);
        OngoingStubbing<Integer> ongoing = when(inputStream.readInt());
        for (int b : data) {
            ongoing = ongoing.thenReturn(b);
        }

        if (throwException) {
            ongoing.thenThrow(new EOFException());
        }
        return inputStream;
    }
}
