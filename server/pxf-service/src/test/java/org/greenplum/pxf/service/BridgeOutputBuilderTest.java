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

import org.greenplum.pxf.api.ArrayField;
import org.greenplum.pxf.api.ArrayStreamingField;
import org.greenplum.pxf.api.BadRecordException;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StreamingField;
import org.greenplum.pxf.api.io.BufferWritable;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.io.GPDBWritable;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.StreamingResolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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

@RunWith(MockitoJUnitRunner.class)
public class BridgeOutputBuilderTest {
    private static final int UN_SUPPORTED_TYPE = -1;
    private GPDBWritable output = null;
    private RequestContext context;
    private BridgeOutputBuilder.WritableIterator writableIterator;
    private List<OneField> records;
    BridgeOutputBuilder builder;
    @Mock
    private StreamingResolver resolver;

    @Before
    public void setup() {
        context = new RequestContext();
    }

    @Test
    public void testFillGPDBWritable() throws Exception {

        addColumn(0, DataType.INTEGER, "col0");
        addColumn(1, DataType.FLOAT8, "col1");
        addColumn(2, DataType.REAL, "col2");
        addColumn(3, DataType.BIGINT, "col3");
        addColumn(4, DataType.SMALLINT, "col4");
        addColumn(5, DataType.BOOLEAN, "col5");
        addColumn(6, DataType.BYTEA, "col6");
        addColumn(7, DataType.VARCHAR, "col7");
        addColumn(8, DataType.BPCHAR, "col8");
        addColumn(9, DataType.TEXT, "col9");
        addColumn(10, DataType.NUMERIC, "col10");
        addColumn(11, DataType.TIMESTAMP, "col11");
        addColumn(12, DataType.DATE, "col12");

        builder = makeBuilder(context);
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
    public void testCSVSerialization() throws Exception {

        addColumn(0, DataType.INTEGER, "col0");
        addColumn(1, DataType.FLOAT8, "col1");
        addColumn(2, DataType.REAL, "col2");
        addColumn(3, DataType.BIGINT, "col3");
        addColumn(4, DataType.SMALLINT, "col4");
        addColumn(5, DataType.BOOLEAN, "col5");
        addColumn(6, DataType.BYTEA, "col6");
        addColumn(7, DataType.VARCHAR, "col7");
        addColumn(8, DataType.BPCHAR, "col8");
        addColumn(9, DataType.TEXT, "col9");
        addColumn(10, DataType.NUMERIC, "col10");
        addColumn(11, DataType.TIMESTAMP, "col11");
        addColumn(12, DataType.DATE, "col12");
        addColumn(13, DataType.VARCHAR, "col13");

        builder = makeBuilder(context);

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
                new OneField(DataType.VARCHAR.getOID(), null)
        );

        List<Writable> outputQueue = builder.makeOutput(recFields);

        assertNotNull(outputQueue);
        assertEquals(1, outputQueue.size());

        String datetime = new Timestamp(0).toLocalDateTime().format(GreenplumDateTime.DATETIME_FORMATTER);
        String date = new Date(1).toString();

        assertOutputStream("0,0.0,0.0,0,0,true,\\x00,value,value,\"va\"\"lue\",0," + datetime + "," + date + ",\n", outputQueue.get(0));
    }

    @Test
    public void testFillOneGPDBWritableField() throws Exception {
        addColumn(0, DataType.INTEGER, "col0");
        builder = makeBuilder(context);
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
        addColumn(0, DataType.INTEGER, "col0");
        addColumn(1, DataType.INTEGER, "col1");
        addColumn(2, DataType.INTEGER, "col2");
        addColumn(3, DataType.INTEGER, "col3");

        builder = makeBuilder(context);
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

        addColumn(0, DataType.INTEGER, "col0");
        addColumn(1, DataType.INTEGER, "col1");
        addColumn(2, DataType.INTEGER, "col2");
        addColumn(3, DataType.INTEGER, "col3");

        builder = makeBuilder(context);
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

        addColumn(0, DataType.INTEGER, "col0");
        addColumn(1, DataType.INTEGER, "col1");
        addColumn(2, DataType.INTEGER, "col2");
        addColumn(3, DataType.INTEGER, "col3");

        builder = makeBuilder(context);
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

        addColumn(0, DataType.TEXT, "col0");
        // activate sampling code
        context.setStatsMaxFragments(100);
        context.setStatsSampleRatio(1f);

        builder = makeBuilder(context);
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

        addColumn(0, DataType.TEXT, "col0");
        // activate sampling code
        context.setStatsMaxFragments(100);
        context.setStatsSampleRatio(1f);

        builder = makeBuilder(context);
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

    private static void compareBufferWritable(Writable line, String expected)
            throws IOException {
        assertTrue(line instanceof BufferWritable);
        assertOutputStream(expected, line);
    }

    private void addColumn(int idx, DataType dataType, String name) {
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
        context.setFragmentMetadata(Utilities.parseBase64("U29tZXRoaW5nIGluIHRoZSB3YXk=", "Fragment metadata information"));

        return new BridgeOutputBuilder(context);
    }

    @Test
    public void testWritableIterator() throws IOException, InterruptedException {
        setStreamingResolver(new String[]{"FOO", "BAR", "BAZ"});
        records = new ArrayList<OneField>() {{
            add(new OneField(DataType.FLOAT8.getOID(), 0.123456789));
            add(new ArrayField(DataType.INT8ARRAY.getOID(), new ArrayList<Integer>() {{
                add(100);
                add(200);
                add(300);
            }}));
            add(new ArrayStreamingField(resolver));
        }};
        builder = makeBuilder(new RequestContext());
        writableIterator = builder.new WritableIterator(records);
        assertOutputStream("0.123456789,", writableIterator.next());
        assertOutputStream("\"{100,200,300}\",", writableIterator.next());
        assertOutputStream("\"{FOO,", writableIterator.next());
        assertOutputStream("BAR,", writableIterator.next());
        assertOutputStream("BAZ}\"\n", writableIterator.next());
    }

    @Test
    public void testWritableIterator_StreamingFieldFirst() throws IOException, InterruptedException {
        setStreamingResolver(new String[]{"FOO", "BAR", "BAZ"});
        records = new ArrayList<OneField>() {{
            add(new ArrayStreamingField(resolver));
            add(new OneField(DataType.BOOLEAN.getOID(), true));
            add(new ArrayField(DataType.TEXTARRAY.getOID(), new ArrayList<String>() {{
                add("foo");
                add("bar");
                add("baz");
            }}));
        }};
        builder = makeBuilder(new RequestContext());
        writableIterator = builder.new WritableIterator(records);
        assertOutputStream("\"{FOO,", writableIterator.next());
        assertOutputStream("BAR,", writableIterator.next());
        assertOutputStream("BAZ}\",", writableIterator.next());
        assertOutputStream("true,", writableIterator.next());
        assertOutputStream("\"{foo,bar,baz}\"\n", writableIterator.next());

    }

    @Test
    public void testWritableIterator_EscapeNeeded() throws IOException, InterruptedException {
        setStreamingResolver(new String[]{"FOO", "BA\"R", "BAZ"});
        records = new ArrayList<OneField>() {{
            add(new OneField(DataType.TEXT.getOID(), "just \"some text"));
            add(new ArrayStreamingField(resolver));
            add(new ArrayField(DataType.TEXTARRAY.getOID(), new ArrayList<String>() {{
                add("foo");
                add("bar");
                add("baz");
            }}));
        }};
        builder = makeBuilder(new RequestContext());
        writableIterator = builder.new WritableIterator(records);
        assertOutputStream("\"just \"\"some text\",", writableIterator.next());
        assertOutputStream("\"{FOO,", writableIterator.next());
        assertOutputStream("BA\"\"R,", writableIterator.next());
        assertOutputStream("BAZ}\",", writableIterator.next());
        assertOutputStream("\"{foo,bar,baz}\"\n", writableIterator.next());
    }

    @Test
    public void testWritableIterator_StreamingScalarField() throws IOException, InterruptedException {
        setStreamingResolver(new String[]{"FOO", "BA\"R", "BAZ"});
        records = new ArrayList<OneField>() {{
            add(new OneField(DataType.BIGINT.getOID(), 1234567890));
            add(new ArrayField(DataType.BOOLARRAY.getOID(), new ArrayList<Boolean>() {{
                add(true);
                add(false);
                add(true);
            }}));
            add(new StreamingField(DataType.TEXT.getOID(), resolver));
        }};
        builder = makeBuilder(new RequestContext());
        writableIterator = builder.new WritableIterator(records);
        assertOutputStream("1234567890,", writableIterator.next());
        assertOutputStream("\"{true,false,true}\",", writableIterator.next());
        assertOutputStream("\"FOO", writableIterator.next());
        assertOutputStream("BA\"\"R", writableIterator.next());
        assertOutputStream("BAZ\"\n", writableIterator.next());
    }

    @Test
    public void testWritableIterator_StreamingScalarField_StreamingFieldFirst() throws IOException, InterruptedException {
        setStreamingResolver(new String[]{"FOO", "BA\"R", "BAZ"});
        records = new ArrayList<OneField>() {{
            add(new StreamingField(DataType.TEXT.getOID(), resolver));
            add(new ArrayField(DataType.FLOAT8ARRAY.getOID(), new ArrayList<Double>() {{
                add(0.123456789);
                add(1.123456789);
                add(2.123456789);
            }}));
        }};
        builder = makeBuilder(new RequestContext());
        writableIterator = builder.new WritableIterator(records);
        assertOutputStream("\"FOO", writableIterator.next());
        assertOutputStream("BA\"\"R", writableIterator.next());
        assertOutputStream("BAZ\",", writableIterator.next());
        assertOutputStream("\"{0.123456789,1.123456789,2.123456789}\"\n", writableIterator.next());
    }

    private void setStreamingResolver(String[] responses) {
        resolver = new StreamingResolver() {
            @Override
            public void initialize(RequestContext requestContext) {
            }

            @Override
            public boolean isThreadSafe() {
                return false;
            }

            @Override
            public List<OneField> getFields(OneRow row) throws Exception {
                return null;
            }

            @Override
            public OneRow setFields(List<OneField> record) throws Exception {
                return null;
            }

            int cnt = 0;

            @Override
            public String next() {
                if (cnt == responses.length) {
                    return null;
                }
                return responses[cnt++];
            }

            @Override
            public boolean hasNext() {
                return cnt < responses.length;
            }
        };
    }

    private static void assertOutputStream(String correct, Writable writable) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writable.write(new DataOutputStream(out));
            assertEquals(correct, out.toString());
        }
    }
}
