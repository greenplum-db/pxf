package org.greenplum.pxf.service;

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


import org.greenplum.pxf.api.BadRecordException;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.io.BufferWritable;
import org.greenplum.pxf.service.io.GPDBWritable;
import org.greenplum.pxf.service.io.Writable;
import org.junit.Test;

import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BridgeOutputBuilderTest {

    private static final int UN_SUPPORTED_TYPE = -1;
    private GPDBWritable output = null;
    private DataOutputToBytes dos = new DataOutputToBytes();

    @Test
    public void testFillGPDBWritable() throws Exception {
        RequestContext context = new RequestContext();

        addColumn(context, 0, DataType.INTEGER, "col0");
        addColumn(context, 1, DataType.FLOAT8, "col1");
        addColumn(context, 2, DataType.REAL, "col2");
        addColumn(context, 3, DataType.BIGINT, "col3");
        addColumn(context, 4, DataType.SMALLINT, "col4");
        addColumn(context, 5, DataType.BOOLEAN, "col5");
        addColumn(context, 6, DataType.BYTEA, "col6");
        addColumn(context, 7, DataType.VARCHAR, "col7");
        addColumn(context, 8, DataType.BPCHAR, "col8");
        addColumn(context, 9, DataType.TEXT, "col9");
        addColumn(context, 10, DataType.NUMERIC, "col10");
        addColumn(context, 11, DataType.TIMESTAMP, "col11");
        addColumn(context, 12, DataType.DATE, "col12");

        BridgeOutputBuilder builder = makeBuilder(context);
        output = builder.makeGPDBWritableOutput();

        List<OneField> recFields = Arrays.asList(
                new OneField(DataType.INTEGER.getOID(), 0), new OneField(
                        DataType.FLOAT8.getOID(), (double) 0), new OneField(
                        DataType.REAL.getOID(), (float) 0), new OneField(
                        DataType.BIGINT.getOID(), (long) 0), new OneField(
                        DataType.SMALLINT.getOID(), (short) 0), new OneField(
                        DataType.BOOLEAN.getOID(), true), new OneField(
                        DataType.BYTEA.getOID(), new byte[]{0}),
                new OneField(DataType.VARCHAR.getOID(), "value"), new OneField(
                        DataType.BPCHAR.getOID(), "value"), new OneField(
                        DataType.TEXT.getOID(), "value"), new OneField(
                        DataType.NUMERIC.getOID(), "0"), new OneField(
                        DataType.TIMESTAMP.getOID(), new Timestamp(0)),
                new OneField(DataType.DATE.getOID(), new Date(1)));
        builder.fillGPDBWritable(recFields);

        assertEquals(output.getInt(0), Integer.valueOf(0));
        assertEquals(output.getDouble(1), Double.valueOf(0));
        assertEquals(output.getFloat(2), Float.valueOf(0));
        assertEquals(output.getLong(3), Long.valueOf(0));
        assertEquals(output.getShort(4), Short.valueOf((short) 0));
        assertEquals(output.getBoolean(5), true);
        assertArrayEquals(output.getBytes(6), new byte[]{0});
        assertEquals(output.getString(7), "value\0");
        assertEquals(output.getString(8), "value\0");
        assertEquals(output.getString(9), "value\0");
        assertEquals(output.getString(10), "0\0");
        assertEquals(Timestamp.valueOf(output.getString(11)), new Timestamp(0));
        assertEquals(Date.valueOf(output.getString(12).trim()).toString(),
                new Date(1).toString());
    }

    @Test
    public void testFillOneGPDBWritableField() throws Exception {
        RequestContext context = new RequestContext();
        addColumn(context, 0, DataType.INTEGER, "col0");
        BridgeOutputBuilder builder = makeBuilder(context);
        output = builder.makeGPDBWritableOutput();

        OneField unSupportedField = new OneField(UN_SUPPORTED_TYPE, (byte) 0);
        try {
            builder.fillOneGPDBWritableField(unSupportedField, 0);
            fail("Unsupported data type should throw exception");
        } catch (UnsupportedOperationException e) {
            assertEquals(e.getMessage(),
                    "Byte is not supported for GPDB conversion");
        }
    }

    @Test
    public void testRecordSmallerThanSchema() throws Exception {
        RequestContext context = new RequestContext();

        addColumn(context, 0, DataType.INTEGER, "col0");
        addColumn(context, 1, DataType.INTEGER, "col1");
        addColumn(context, 2, DataType.INTEGER, "col2");
        addColumn(context, 3, DataType.INTEGER, "col3");

        BridgeOutputBuilder builder = makeBuilder(context);
        output = builder.makeGPDBWritableOutput();

        /* all four fields */
        List<OneField> complete = Arrays.asList(
                new OneField(DataType.INTEGER.getOID(), 10), new OneField(
                        DataType.INTEGER.getOID(), 20), new OneField(
                        DataType.INTEGER.getOID(), 30), new OneField(
                        DataType.INTEGER.getOID(), 40));
        builder.fillGPDBWritable(complete);
        assertEquals(output.getColType().length, 4);
        assertEquals(output.getInt(0), Integer.valueOf(10));
        assertEquals(output.getInt(1), Integer.valueOf(20));
        assertEquals(output.getInt(2), Integer.valueOf(30));
        assertEquals(output.getInt(3), Integer.valueOf(40));

        /* two fields instead of four */
        List<OneField> incomplete = Arrays.asList(
                new OneField(DataType.INTEGER.getOID(), 10), new OneField(
                        DataType.INTEGER.getOID(), 20));
        try {
            builder.fillGPDBWritable(incomplete);
            fail("testRecordBiggerThanSchema should have failed on - Record has 2 fields but the schema size is 4");
        } catch (BadRecordException e) {
            assertEquals(e.getMessage(),
                    "Record has 2 fields but the schema size is 4");
        }
    }

    @Test
    public void testRecordBiggerThanSchema() {
        RequestContext context = new RequestContext();

        addColumn(context, 0, DataType.INTEGER, "col0");
        addColumn(context, 1, DataType.INTEGER, "col1");
        addColumn(context, 2, DataType.INTEGER, "col2");
        addColumn(context, 3, DataType.INTEGER, "col3");

        BridgeOutputBuilder builder = makeBuilder(context);
        output = builder.makeGPDBWritableOutput();

        /* five fields instead of four */
        List<OneField> complete = Arrays.asList(
                new OneField(DataType.INTEGER.getOID(), 10), new OneField(
                        DataType.INTEGER.getOID(), 20), new OneField(
                        DataType.INTEGER.getOID(), 30), new OneField(
                        DataType.INTEGER.getOID(), 40), new OneField(
                        DataType.INTEGER.getOID(), 50));
        try {
            builder.fillGPDBWritable(complete);
            fail("testRecordBiggerThanSchema should have failed on - Record has 5 fields but the schema size is 4");
        } catch (BadRecordException e) {
            assertEquals(e.getMessage(),
                    "Record has 5 fields but the schema size is 4");
        }
    }

    @Test
    public void testFieldTypeMismatch() {
        RequestContext context = new RequestContext();

        addColumn(context, 0, DataType.INTEGER, "col0");
        addColumn(context, 1, DataType.INTEGER, "col1");
        addColumn(context, 2, DataType.INTEGER, "col2");
        addColumn(context, 3, DataType.INTEGER, "col3");

        BridgeOutputBuilder builder = makeBuilder(context);
        output = builder.makeGPDBWritableOutput();

        /* last field is REAL while schema requires INT */
        List<OneField> complete = Arrays.asList(
                new OneField(DataType.INTEGER.getOID(), 10), new OneField(
                        DataType.INTEGER.getOID(), 20), new OneField(
                        DataType.INTEGER.getOID(), 30), new OneField(
                        DataType.REAL.getOID(), 40.0));
        try {
            builder.fillGPDBWritable(complete);
            fail("testFieldTypeMismatch should have failed on - For field 3 schema requires type INTEGER but input record has type REAL");
        } catch (BadRecordException e) {
            assertEquals(e.getMessage(),
                    "For field col3 schema requires type INTEGER but input record has type REAL");
        }
    }

    @Test
    public void convertTextDataToLines() throws Exception {

        String data = "Que será será\n" + "Whatever will be will be\n"
                + "We are going\n" + "to Wembeley!\n";
        byte[] dataBytes = data.getBytes();
        String[] dataLines = new String[]{
                "Que será será\n",
                "Whatever will be will be\n",
                "We are going\n",
                "to Wembeley!\n"};

        OneField field = new OneField(DataType.BYTEA.getOID(), dataBytes);
        List<OneField> fields = new ArrayList<>();
        fields.add(field);

        RequestContext context = new RequestContext();
        addColumn(context, 0, DataType.TEXT, "col0");
        // activate sampling code
        context.setStatsMaxFragments(100);
        context.setStatsSampleRatio(1f);

        BridgeOutputBuilder builder = makeBuilder(context);
        LinkedList<Writable> outputQueue = builder.makeOutput(fields);

        assertEquals(4, outputQueue.size());

        for (int i = 0; i < dataLines.length; ++i) {
            Writable line = outputQueue.get(i);
            compareBufferWritable(line, dataLines[i]);
        }

        assertNull(builder.getPartialLine());
    }

    @Test
    public void convertTextDataToLinesPartial() throws Exception {
        String data = "oh well\n" + "what the hell";

        OneField field = new OneField(DataType.BYTEA.getOID(), data.getBytes());
        List<OneField> fields = new ArrayList<>();
        fields.add(field);

        RequestContext context = new RequestContext();
        addColumn(context, 0, DataType.TEXT, "col0");
        // activate sampling code
        context.setStatsMaxFragments(100);
        context.setStatsSampleRatio(1f);

        BridgeOutputBuilder builder = makeBuilder(context);
        LinkedList<Writable> outputQueue = builder.makeOutput(fields);

        assertEquals(1, outputQueue.size());

        Writable line = outputQueue.get(0);
        compareBufferWritable(line, "oh well\n");

        Writable partial = builder.getPartialLine();
        assertNotNull(partial);
        compareBufferWritable(partial, "what the hell");

        // check that append works
        data = " but the show must go on\n" + "!!!\n";
        field = new OneField(DataType.BYTEA.getOID(), data.getBytes());
        fields.clear();
        fields.add(field);

        outputQueue = builder.makeOutput(fields);

        assertNull(builder.getPartialLine());
        assertEquals(2, outputQueue.size());

        line = outputQueue.get(0);
        compareBufferWritable(line, "what the hell but the show must go on\n");
        line = outputQueue.get(1);
        compareBufferWritable(line, "!!!\n");

        // check that several partial lines gets appended to each other
        data = "I want to ride my bicycle\n" + "I want to ride my bike";

        field = new OneField(DataType.BYTEA.getOID(), data.getBytes());
        fields.clear();
        fields.add(field);

        outputQueue = builder.makeOutput(fields);

        assertEquals(1, outputQueue.size());

        line = outputQueue.get(0);
        compareBufferWritable(line, "I want to ride my bicycle\n");

        partial = builder.getPartialLine();
        assertNotNull(partial);
        compareBufferWritable(partial, "I want to ride my bike");

        // data consisting of one long line
        data = " I want to ride my bicycle";

        field = new OneField(DataType.BYTEA.getOID(), data.getBytes());
        fields.clear();
        fields.add(field);

        outputQueue = builder.makeOutput(fields);

        assertEquals(0, outputQueue.size());

        partial = builder.getPartialLine();
        assertNotNull(partial);
        compareBufferWritable(partial,
                "I want to ride my bike I want to ride my bicycle");

        // data with lines
        data = " bicycle BICYCLE\n" + "bicycle BICYCLE\n";

        field = new OneField(DataType.BYTEA.getOID(), data.getBytes());
        fields.clear();
        fields.add(field);

        outputQueue = builder.makeOutput(fields);

        assertEquals(2, outputQueue.size());

        line = outputQueue.get(0);
        compareBufferWritable(line,
                "I want to ride my bike I want to ride my bicycle bicycle BICYCLE\n");
        line = outputQueue.get(1);
        compareBufferWritable(line, "bicycle BICYCLE\n");

        partial = builder.getPartialLine();
        assertNull(partial);

    }

    private void compareBufferWritable(Writable line, String expected)
            throws IOException {
        assertTrue(line instanceof BufferWritable);
        line.write(dos);
        assertArrayEquals(expected.getBytes(), dos.getOutput());
    }

    private void addColumn(RequestContext context, int idx,
                           DataType dataType, String name) {
        ColumnDescriptor column = new ColumnDescriptor(name, dataType.getOID(), idx, dataType.toString(), null);
        context.getTupleDescription().add(column);
    }

    private BridgeOutputBuilder makeBuilder(RequestContext context) {
        System.setProperty("greenplum.alignment", "8");

        context.setSegmentId(-44);
        context.setTotalSegments(2);
        context.setFilterStringValid(false);
        context.setOutputFormat(OutputFormat.TEXT);
        context.setHost("my://bags");
        context.setPort(-8020);
        context.setAccessor("are");
        context.setResolver("packed");
        context.setUser("alex");
        context.addOption("I'M-STANDING-HERE", "outside-your-door");
        context.setDataSource("i'm/ready/to/go");
        context.setFragmentMetadata(Utilities.parseBase64("U29tZXRoaW5nIGluIHRoZSB3YXk=", "Fragment metadata information"));

        return new BridgeOutputBuilder(context);
    }

    /**
     * Test class to check the data inside BufferWritable.
     */
    private class DataOutputToBytes implements DataOutput {

        byte[] output;

        byte[] getOutput() {
            return output;
        }

        @Override
        public void write(int b) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void write(byte[] b) {
            output = b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeBoolean(boolean v) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeByte(int v) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeShort(int v) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeChar(int v) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeInt(int v) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeLong(long v) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeFloat(float v) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeDouble(double v) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeBytes(String s) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeChars(String s) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public void writeUTF(String s) throws IOException {
            throw new IOException("not implemented");
        }
    }
}
