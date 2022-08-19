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

import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.examples.DemoFragmentMetadata;
import org.greenplum.pxf.api.io.BufferWritable;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.io.GPDBWritable;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.Test;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BridgeOutputBuilderTest {

    private static final int UN_SUPPORTED_TYPE = -1;
    private GPDBWritable output = null;
    private final DataOutputToBytes dos = new DataOutputToBytes();
    private enum TestEnum { HELLO }

    @Test
    public void testFillGPDBWritable_NativePrimitiveTypes() throws Exception {
        RequestContext context = new RequestContext();

        // go in the order of types defined in DataType enum
        addColumn(context,  0, DataType.BOOLEAN                 , "col00");
        addColumn(context,  1, DataType.BYTEA                   , "col01");
        addColumn(context,  2, DataType.BIGINT                  , "col02");
        addColumn(context,  3, DataType.SMALLINT                , "col03");
        addColumn(context,  4, DataType.INTEGER                 , "col04");
        addColumn(context,  5, DataType.TEXT                    , "col05");
        addColumn(context,  6, DataType.REAL                    , "col06");
        addColumn(context,  7, DataType.FLOAT8                  , "col07");
        addColumn(context,  8, DataType.BPCHAR                  , "col08");
        addColumn(context,  9, DataType.VARCHAR                 , "col09");
        addColumn(context, 10, DataType.DATE                    , "col10");
        addColumn(context, 11, DataType.TIME                    , "col11");
        addColumn(context, 12, DataType.TIMESTAMP               , "col12");
        addColumn(context, 13, DataType.TIMESTAMP_WITH_TIME_ZONE, "col13");
        addColumn(context, 14, DataType.NUMERIC                 , "col14");
        addColumn(context, 15, DataType.UUID                    , "col15");

        BridgeOutputBuilder builder = makeBuilder(context);
        output = builder.makeGPDBWritableOutput();

        List<OneField> recFields = Arrays.asList(
                new OneField(DataType.BOOLEAN.getOID()                 , true),
                new OneField(DataType.BYTEA.getOID()                   , new byte[]{0,1}),
                new OneField(DataType.BIGINT.getOID()                  , 1L),
                new OneField(DataType.SMALLINT.getOID()                , (short) 2),
                new OneField(DataType.INTEGER.getOID()                 , 3),
                new OneField(DataType.TEXT.getOID()                    , "text-value"),
                new OneField(DataType.REAL.getOID()                    , 4.5f),
                new OneField(DataType.FLOAT8.getOID()                  , 6.7d),
                new OneField(DataType.BPCHAR.getOID()                  , "char-value"),
                new OneField(DataType.VARCHAR.getOID()                 , "varchar-value"),
                new OneField(DataType.DATE.getOID()                    , Date.valueOf("1994-08-03")),
                new OneField(DataType.TIME.getOID()                    , "10:11:12"),
                new OneField(DataType.TIMESTAMP.getOID()               , Timestamp.valueOf("2022-06-10 11:44:33.123456")),
                new OneField(DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), Timestamp.valueOf("2022-06-10 11:44:55.123456")),
                new OneField(DataType.NUMERIC.getOID()                 , "9876.54321"),
                new OneField(DataType.UUID.getOID()                    , "667b97ba-38d0-4b91-9c7d-1f8b30a75c6e"));
        builder.fillGPDBWritable(recFields);

        assertTrue(output.getBoolean(0));
        assertArrayEquals(new byte[]{0, 1}, output.getBytes(1));
        assertEquals(1L, output.getLong(2));
        assertEquals((short) 2, output.getShort(3));
        assertEquals(3, output.getInt(4));
        assertEquals("text-value\0", output.getString(5));
        assertEquals(4.5f, output.getFloat(6));
        assertEquals(6.7d, output.getDouble(7));
        assertEquals("char-value\0", output.getString(8));
        assertEquals("varchar-value\0", output.getString(9));
        assertEquals("1994-08-03\0", output.getString(10));
        assertEquals("10:11:12\0", output.getString(11));
        assertEquals("2022-06-10 11:44:33.123456\0", output.getString(12));
        assertEquals("2022-06-10 11:44:55.123456\0", output.getString(13));
        assertEquals("9876.54321\0", output.getString(14));
        assertEquals("667b97ba-38d0-4b91-9c7d-1f8b30a75c6e\0", output.getString(15));
    }

    @Test
    public void testFillGPDBWritable_StringifiedPrimitiveTypes() throws Exception {
        RequestContext context = new RequestContext();

        // go in the order of types defined in DataType enum
        addColumn(context,  0, DataType.BOOLEAN                 , "col00");
        addColumn(context,  1, DataType.BYTEA                   , "col01");
        addColumn(context,  2, DataType.BIGINT                  , "col02");
        addColumn(context,  3, DataType.SMALLINT                , "col03");
        addColumn(context,  4, DataType.INTEGER                 , "col04");
        addColumn(context,  5, DataType.TEXT                    , "col05");
        addColumn(context,  6, DataType.REAL                    , "col06");
        addColumn(context,  7, DataType.FLOAT8                  , "col07");
        addColumn(context,  8, DataType.BPCHAR                  , "col08");
        addColumn(context,  9, DataType.VARCHAR                 , "col09");
        addColumn(context, 10, DataType.DATE                    , "col10");
        addColumn(context, 11, DataType.TIME                    , "col11");
        addColumn(context, 12, DataType.TIMESTAMP               , "col12");
        addColumn(context, 13, DataType.TIMESTAMP_WITH_TIME_ZONE, "col13");
        addColumn(context, 14, DataType.NUMERIC                 , "col14");
        addColumn(context, 15, DataType.UUID                    , "col15");

        BridgeOutputBuilder builder = makeBuilder(context);
        output = builder.makeGPDBWritableOutput();

        // in OneField objects use DataType.TEXT and String values for primitive types that are serialized as strings
        List<OneField> recFields = Arrays.asList(
                new OneField(DataType.BOOLEAN.getOID()  , true),
                new OneField(DataType.BYTEA.getOID()    , new byte[]{0,1}),
                new OneField(DataType.BIGINT.getOID()   , 1L),
                new OneField(DataType.SMALLINT.getOID() , (short) 2),
                new OneField(DataType.INTEGER.getOID()  , 3),
                new OneField(DataType.TEXT.getOID()     , "text-value"),
                new OneField(DataType.REAL.getOID()     , 4.5f),
                new OneField(DataType.FLOAT8.getOID()   , 6.7d),
                new OneField(DataType.TEXT.getOID()     , "char-value"),
                new OneField(DataType.TEXT.getOID()     , "varchar-value"),
                new OneField(DataType.TEXT.getOID()     , "1994-08-03"),
                new OneField(DataType.TEXT.getOID()     , "10:11:12"),
                new OneField(DataType.TEXT.getOID()     , "2022-06-10 11:44:33.123456"),
                new OneField(DataType.TEXT.getOID()     , "2022-06-10 11:44:55.123456"),
                new OneField(DataType.TEXT.getOID()     , "9876.54321"),
                new OneField(DataType.TEXT.getOID()     , "667b97ba-38d0-4b91-9c7d-1f8b30a75c6e"));
        builder.fillGPDBWritable(recFields);

        assertTrue(output.getBoolean(0));
        assertArrayEquals(new byte[]{0, 1}, output.getBytes(1));
        assertEquals(1L, output.getLong(2));
        assertEquals((short) 2, output.getShort(3));
        assertEquals(3, output.getInt(4));
        assertEquals("text-value\0", output.getString(5));
        assertEquals(4.5f, output.getFloat(6));
        assertEquals(6.7d, output.getDouble(7));
        assertEquals("char-value\0", output.getString(8));
        assertEquals("varchar-value\0", output.getString(9));
        assertEquals("1994-08-03\0", output.getString(10));
        assertEquals("10:11:12\0", output.getString(11));
        assertEquals("2022-06-10 11:44:33.123456\0", output.getString(12));
        assertEquals("2022-06-10 11:44:55.123456\0", output.getString(13));
        assertEquals("9876.54321\0", output.getString(14));
        assertEquals("667b97ba-38d0-4b91-9c7d-1f8b30a75c6e\0", output.getString(15));
    }

    @Test
    public void testCSVSerialization() throws Exception {
        RequestContext context = new RequestContext();
        context.setFormat("CSV");
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
        addColumn(context, 13, DataType.VARCHAR, "col13");
        addColumn(context, 14, DataType.VARCHAR, "col14");

        BridgeOutputBuilder builder = makeBuilder(context);

        List<OneField> recFields = Arrays.asList(
                new OneField(DataType.INTEGER.getOID(), 0),
                new OneField(DataType.FLOAT8.getOID(), (double) 0),
                new OneField(DataType.REAL.getOID(), (float) 0),
                new OneField(DataType.BIGINT.getOID(), (long) 0),
                new OneField(DataType.SMALLINT.getOID(), (short) 0),
                new OneField(DataType.BOOLEAN.getOID(), true),
                new OneField(DataType.BYTEA.getOID(), new byte[]{0}),
                new OneField(DataType.VARCHAR.getOID(), "value"),
                new OneField(DataType.BPCHAR.getOID(), "value"),
                new OneField(DataType.TEXT.getOID(), "va\"lue"),
                new OneField(DataType.NUMERIC.getOID(), "0"),
                new OneField(DataType.TIMESTAMP.getOID(), new Timestamp(0)),
                new OneField(DataType.DATE.getOID(), new Date(1)),
                new OneField(DataType.VARCHAR.getOID(), null),
                new OneField(DataType.VARCHAR.getOID(), TestEnum.HELLO)
        );

        List<Writable> outputQueue = builder.makeOutput(recFields);

        assertNotNull(outputQueue);
        assertEquals(1, outputQueue.size());

        String datetime = new Timestamp(0).toLocalDateTime().format(GreenplumDateTime.DATETIME_FORMATTER);
        String date = new Date(1).toString();

        outputQueue.get(0).write(dos);
        assertEquals("0,0.0,0.0,0,0,true,\\x00,value,value,\"va\"\"lue\",0," + datetime + "," + date + ",,HELLO\n",
                new String(dos.getOutput(), StandardCharsets.UTF_8));
    }

    @Test
    public void testFillOneGPDBWritableField() {
        RequestContext context = new RequestContext();
        addColumn(context, 0, DataType.INTEGER, "col0");
        BridgeOutputBuilder builder = makeBuilder(context);
        output = builder.makeGPDBWritableOutput();

        OneField unSupportedField = new OneField(UN_SUPPORTED_TYPE, (byte) 0);

        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> builder.fillOneGPDBWritableField(unSupportedField, 0),
                "Unsupported data type should throw exception");
        assertEquals("Byte is not supported for GPDB conversion", e.getMessage());
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
        Exception e = assertThrows(BadRecordException.class,
                () -> builder.fillGPDBWritable(incomplete),
                "testRecordBiggerThanSchema should have failed on - Record has 2 fields but the schema size is 4");
        assertEquals("Record has 2 fields but the schema size is 4", e.getMessage());
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
        Exception e = assertThrows(BadRecordException.class,
                () -> builder.fillGPDBWritable(complete),
                "testRecordBiggerThanSchema should have failed on - Record has 5 fields but the schema size is 4");
        assertEquals("Record has 5 fields but the schema size is 4", e.getMessage());
    }

    @Test
    public void testLineBreakInDifferentEncodings() {
        String lf = "\n";
        String cr = "\r";
        String crlf = "\r\n";

        byte[] lfBytes = {10};
        byte[] crBytes = {13};
        byte[] crlfBytes = {13, 10};

        assertArrayEquals(lfBytes, lf.getBytes());
        assertArrayEquals(crBytes, cr.getBytes());
        assertArrayEquals(crlfBytes, crlf.getBytes());

        assertArrayEquals(lfBytes, lf.getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(crBytes, cr.getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(crlfBytes, crlf.getBytes(StandardCharsets.UTF_8));

        assertArrayEquals(lfBytes, lf.getBytes(StandardCharsets.ISO_8859_1));
        assertArrayEquals(crBytes, cr.getBytes(StandardCharsets.ISO_8859_1));
        assertArrayEquals(crlfBytes, crlf.getBytes(StandardCharsets.ISO_8859_1));

        assertArrayEquals(lfBytes, lf.getBytes(Charset.forName("windows-1251")));
        assertArrayEquals(crBytes, cr.getBytes(Charset.forName("windows-1251")));
        assertArrayEquals(crlfBytes, crlf.getBytes(Charset.forName("windows-1251")));

        assertArrayEquals(lfBytes, lf.getBytes(Charset.forName("Big5")));
        assertArrayEquals(crBytes, cr.getBytes(Charset.forName("Big5")));
        assertArrayEquals(crlfBytes, crlf.getBytes(Charset.forName("Big5")));

        assertArrayEquals(lfBytes, lf.getBytes(Charset.forName("GB18030")));
        assertArrayEquals(crBytes, cr.getBytes(Charset.forName("GB18030")));
        assertArrayEquals(crlfBytes, crlf.getBytes(Charset.forName("GB18030")));

        assertArrayEquals(lfBytes, lf.getBytes(Charset.forName("MS936")));
        assertArrayEquals(crBytes, cr.getBytes(Charset.forName("MS936")));
        assertArrayEquals(crlfBytes, crlf.getBytes(Charset.forName("MS936")));

        assertArrayEquals(lfBytes, lf.getBytes(Charset.forName("Windows-932")));
        assertArrayEquals(crBytes, cr.getBytes(Charset.forName("Windows-932")));
        assertArrayEquals(crlfBytes, crlf.getBytes(Charset.forName("Windows-932")));

        assertArrayEquals(lfBytes, lf.getBytes(Charset.forName("KOI8")));
        assertArrayEquals(crBytes, cr.getBytes(Charset.forName("KOI8")));
        assertArrayEquals(crlfBytes, crlf.getBytes(Charset.forName("KOI8")));

        assertArrayEquals(lfBytes, lf.getBytes(Charset.forName("Windows-949")));
        assertArrayEquals(crBytes, cr.getBytes(Charset.forName("Windows-949")));
        assertArrayEquals(crlfBytes, crlf.getBytes(Charset.forName("Windows-949")));

        assertArrayEquals(lfBytes, lf.getBytes(Charset.forName("Cyrillic")));
        assertArrayEquals(crBytes, cr.getBytes(Charset.forName("Cyrillic")));
        assertArrayEquals(crlfBytes, crlf.getBytes(Charset.forName("Cyrillic")));
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
        Exception e = assertThrows(BadRecordException.class,
                () -> builder.fillGPDBWritable(complete),
                "testFieldTypeMismatch should have failed on - For field 3 schema requires type INTEGER but input record has type REAL");
        assertEquals("For field col3 schema requires type INTEGER but input record has type REAL", e.getMessage());
    }

    @Test
    public void convertTextDataToLines() throws Exception {

        String data = "Qué será será\n" + "Whatever will be will be\n"
                + "We are going\n" + "to Wembeley!\n";
        byte[] dataBytes = data.getBytes();
        String[] dataLines = new String[]{
                "Qué será será\n",
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

    private void addColumn(RequestContext context, int idx, DataType dataType, String name) {
        ColumnDescriptor column = new ColumnDescriptor(name, dataType.getOID(), idx, dataType.toString(), null);
        context.getTupleDescription().add(column);
    }

    private BridgeOutputBuilder makeBuilder(RequestContext context) {
        System.setProperty("greenplum.alignment", "8");

        context.setSegmentId(-44);
        context.setTotalSegments(2);
        context.setOutputFormat(OutputFormat.TEXT);
        context.setHost("my://bags");
        context.setPort(-8020);
        context.setAccessor("are");
        context.setResolver("packed");
        context.setUser("alex");
        context.addOption("I'M-STANDING-HERE", "outside-your-door");
        context.setDataSource("i'm/ready/to/go");
        context.setFragmentMetadata(new DemoFragmentMetadata("Fragment metadata information"));

        return new BridgeOutputBuilder(context);
    }

    /**
     * Test class to check the data inside BufferWritable.
     */
    private static class DataOutputToBytes implements DataOutput {

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
        public void write(byte[] b, int off, int len) {
            output = Arrays.copyOfRange(b, off, len);
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
