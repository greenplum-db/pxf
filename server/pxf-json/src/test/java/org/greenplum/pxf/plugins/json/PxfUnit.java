package org.greenplum.pxf.plugins.json;

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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SimpleFragmentsResponse;
import org.greenplum.pxf.api.utilities.FragmentsResponseFormatter;
import org.greenplum.pxf.api.utilities.Utilities;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * This abstract class contains a number of helpful utilities in developing a PXF extension for HAWQ. Extend this class
 * and use the various <code>assert</code> methods to check given input against known output.
 */
public abstract class PxfUnit {

    private static final Log LOG = LogFactory.getLog(PxfUnit.class);
    protected static List<RequestContext> inputs = null;
    private static JsonFactory factory = new JsonFactory();
    private static ObjectMapper mapper = new ObjectMapper(factory);

    /**
     * Uses the given input directory to run through the PXF unit testing framework. Uses the lines in the file for
     * output testing.
     *
     * @param input          Input records
     * @param expectedOutput File containing output to check
     * @throws Exception
     */
    public void assertOutput(Path input, Path expectedOutput) throws Exception {

        BufferedReader rdr = new BufferedReader(new InputStreamReader(FileSystem.get(new Configuration()).open(
                expectedOutput)));

        List<String> outputLines = new ArrayList<String>();

        String line;
        while ((line = rdr.readLine()) != null) {
            outputLines.add(line);
        }

        assertOutput(input, outputLines);

        rdr.close();
    }

    /**
     * Uses the given input directory to run through the PXF unit testing framework. Uses the lines in the given
     * parameter for output testing.
     *
     * @param input          Input records
     * @param expectedOutput File containing output to check
     * @throws Exception
     */
    public void assertOutput(Path input, List<String> expectedOutput) throws Exception {

        setup(input);
        List<String> actualOutput = new ArrayList<String>();
        for (RequestContext data : inputs) {
            Accessor accessor = getReadAccessor(data);
            Resolver resolver = getReadResolver(data);

            actualOutput.addAll(getAllOutput(accessor, resolver));
        }

        Assert.assertFalse("Output did not match expected output", compareOutput(expectedOutput, actualOutput));
    }

    /**
     * Uses the given input directory to run through the PXF unit testing framework. Uses the lines in the given
     * parameter for output testing.<br>
     * <br>
     * Ignores order of records.
     *
     * @param input          Input records
     * @param expectedOutput File containing output to check
     * @throws Exception
     */
    public void assertUnorderedOutput(Path input, Path expectedOutput) throws Exception {
        BufferedReader rdr = new BufferedReader(new InputStreamReader(FileSystem.get(new Configuration()).open(
                expectedOutput)));

        List<String> outputLines = new ArrayList<String>();

        String line;
        while ((line = rdr.readLine()) != null) {
            outputLines.add(line);
        }

        assertUnorderedOutput(input, outputLines);
        rdr.close();
    }

    /**
     * Uses the given input directory to run through the PXF unit testing framework. Uses the lines in the file for
     * output testing.<br>
     * <br>
     * Ignores order of records.
     *
     * @param input          Input records
     * @param expectedOutput File containing output to check
     * @throws Exception
     */
    public void assertUnorderedOutput(Path input, List<String> expectedOutput) throws Exception {

        setup(input);

        List<String> actualOutput = new ArrayList<String>();
        for (RequestContext data : inputs) {
            Accessor accessor = getReadAccessor(data);
            Resolver resolver = getReadResolver(data);

            actualOutput.addAll(getAllOutput(accessor, resolver));
        }

        Assert.assertFalse("Output did not match expected output", compareUnorderedOutput(expectedOutput, actualOutput));
    }

    /**
     * Writes the output to the given output stream. Comma delimiter.
     *
     * @param input  The input file
     * @param output The output stream
     * @throws Exception
     */
    public void writeOutput(Path input, OutputStream output) throws Exception {

        setup(input);

        for (RequestContext data : inputs) {
            Accessor accessor = getReadAccessor(data);
            Resolver resolver = getReadResolver(data);

            for (String line : getAllOutput(accessor, resolver)) {
                output.write((line + "\n").getBytes());
            }
        }

        output.flush();
    }

    /**
     * Get the class of the implementation of Fragmenter to be tested.
     *
     * @return The class
     */
    public Class<? extends Fragmenter> getFragmenterClass() {
        return null;
    }

    /**
     * Get the class of the implementation of Accessor to be tested.
     *
     * @return The class
     */
    public Class<? extends Accessor> getAccessorClass() {
        return null;
    }

    /**
     * Get the class of the implementation of Resolver to be tested.
     *
     * @return The class
     */
    public Class<? extends Resolver> getResolverClass() {
        return null;
    }

    /**
     * Get any extra parameters that are meant to be specified for the "pxf" protocol. Note that "X-GP-OPTIONS-" is prepended to
     * each parameter name.
     *
     * @return Any extra parameters or null if none.
     */
    public List<Pair<String, String>> getExtraParams() {
        return null;
    }

    /**
     * Gets the column definition names and data types. Types are DataType objects
     *
     * @return A list of column definition name value pairs. Cannot be null.
     */
    public abstract List<Pair<String, DataType>> getColumnDefinitions();

//	protected RequestContext getInputDataForWritableTable() {
//		return getInputDataForWritableTable(null);
//	}

//    protected RequestContext getInputDataForWritableTable(Path input) {
//
//        if (getAccessorClass() == null) {
//            throw new IllegalArgumentException(
//                    "getAccessorClass() must be overwritten to return a non-null object");
//        }
//
//        if (getResolverClass() == null) {
//            throw new IllegalArgumentException(
//                    "getResolverClass() must be overwritten to return a non-null object");
//        }
//
//        RequestContext context = new RequestContext();
//
////		Map<String, String> paramsMap = new HashMap<String, String>();
//
//        System.setProperty("greenplum.alignment", "what");
//        context.setSegmentId(1);
//        context.setFilterStringValid(false);
//        context.setTotalSegments(1);
//        context.setOutputFormat(OutputFormat.GPDBWritable);
//        context.setHost("localhost");
//        context.setPort(50070);
//
//        if (input == null) {
//            context.setDataSource("/dummydata");
//        }
//
////		paramsMap.put("X-GP-ALIGNMENT", "what");
////		paramsMap.put("X-GP-SEGMENT-ID", "1");
////		paramsMap.put("X-GP-SEGMENT-COUNT", "1");
//
////		paramsMap.put("X-GP-FORMAT", "GPDBWritable");
////		paramsMap.put("X-GP-URL-HOST", "localhost");
////		paramsMap.put("X-GP-URL-PORT", "50070");
//
////		if (input == null) {
////			paramsMap.put("X-GP-DATA-DIR", "/dummydata");
////		}
//
//        List<Pair<String, DataType>> params = getColumnDefinitions();
////		paramsMap.put("X-GP-ATTRS", Integer.toString(params.size()));
//        for (int i = 0; i < params.size(); ++i) {
////			paramsMap.put("X-GP-ATTR-NAME" + i, params.get(i).first);
////			paramsMap.put("X-GP-ATTR-TYPENAME" + i, params.get(i).second.name());
////			paramsMap.put("X-GP-ATTR-TYPECODE" + i, Integer.toString(params.get(i).second.getOID()));
//
//            ColumnDescriptor column = new ColumnDescriptor(params.get(i).first, params.get(i).second.getOID(), i, params.get(i).second.name(), null);
//            context.getTupleDescription().add(column);
//        }
//
////		paramsMap.put("X-GP-OPTIONS-ACCESSOR", getAccessorClass().getName());
////		paramsMap.put("X-GP-OPTIONS-RESOLVER", getResolverClass().getName());
//        context.setAccessor(getAccessorClass().getName());
//        context.setResolver(getResolverClass().getName());
//
//        if (getExtraParams() != null) {
//            for (Pair<String, String> param : getExtraParams()) {
////				paramsMap.put("X-GP-OPTIONS-" + param.first, param.second);
//                context.addOption().put(param.first, param.second);
//            }
//        }
//
//        return context;
//    }

    /**
     * Set all necessary parameters for GPXF framework to function. Uses the given path as a single input split.
     *
     * @param input The input path, relative or absolute.
     * @throws Exception
     */
    protected void setup(Path input) throws Exception {

        if (getFragmenterClass() == null) {
            throw new IllegalArgumentException("getFragmenterClass() must be overwritten to return a non-null object");
        }

        if (getAccessorClass() == null) {
            throw new IllegalArgumentException("getAccessorClass() must be overwritten to return a non-null object");
        }

        if (getResolverClass() == null) {
            throw new IllegalArgumentException("getResolverClass() must be overwritten to return a non-null object");
        }

        RequestContext context = getContext(input);
        List<Fragment> fragments = getFragmenter(context).getFragments();

        SimpleFragmentsResponse simpleFragmentsResponse = (SimpleFragmentsResponse) FragmentsResponseFormatter.formatSimpleFragmentsResponse(fragments, input.toString());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        simpleFragmentsResponse.write(baos);

        String jsonOutput = baos.toString();

        inputs = new ArrayList<>();

        JsonNode node = decodeLineToJsonNode(jsonOutput);

        JsonNode fragmentsArray = node.get("PXFFragments");
        int i = 0;
        for (JsonNode fragNode : fragmentsArray) {
            String sourceData = fragNode.get("sourceName").textValue();
            context = getContext(input);
            context.setDataSource(sourceData);
            context.setFragmentMetadata(Utilities.parseBase64(fragNode.get("metadata").textValue(), "Fragment metadata information"));
            context.setDataFragment(i++);
            context.setProfileScheme("localfile");
            inputs.add(context);
        }
    }

    private RequestContext getContext(Path input) {
        RequestContext context = new RequestContext();

        // 2.1.0 Properties
        // HDMetaData parameters
        context.setConfig("default");
        context.setUser("who");
        System.setProperty("greenplum.alignment", "what");
        context.setSegmentId(1);
        context.setTotalSegments(1);
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setHost("localhost");
        context.setPort(50070);
        context.setDataSource(input.toString());
        context.setProfileScheme("localfile");

        List<Pair<String, DataType>> params = getColumnDefinitions();
        for (int i = 0; i < params.size(); ++i) {
            ColumnDescriptor column = new ColumnDescriptor(params.get(i).first, params.get(i).second.getOID(), i, params.get(i).second.name(), null);
            context.getTupleDescription().add(column);
        }

        // HDFSMetaData properties
        context.setFragmenter(getFragmenterClass().getName());
        context.setAccessor(getAccessorClass().getName());
        context.setResolver(getResolverClass().getName());

        if (getExtraParams() != null) {
            for (Pair<String, String> param : getExtraParams()) {
                context.addOption(param.first, param.second);
            }
        }

        return context;
    }

    private JsonNode decodeLineToJsonNode(String line) {
        try {
            return mapper.readTree(line);
        } catch (Exception e) {
            LOG.warn(e);
            return null;
        }
    }

    /**
     * Compares the expected and actual output, printing out any errors.
     *
     * @param expectedOutput The expected output
     * @param actualOutput   The actual output
     * @return True if no errors, false otherwise.
     */
    protected boolean compareOutput(List<String> expectedOutput, List<String> actualOutput) {
        return compareOutput(expectedOutput, actualOutput, false);
    }

    /**
     * Compares the expected and actual output, printing out any errors.
     *
     * @param expectedOutput The expected output
     * @param actualOutput   The actual output
     * @return True if no errors, false otherwise.
     */
    protected boolean compareUnorderedOutput(List<String> expectedOutput, List<String> actualOutput) {
        return compareOutput(expectedOutput, actualOutput, true);
    }

    private boolean compareOutput(List<String> expectedOutput, List<String> actualOutput, boolean ignoreOrder) {
        boolean error = false;
        for (int i = 0; i < expectedOutput.size(); ++i) {
            boolean match = false;
            for (int j = 0; j < actualOutput.size(); ++j) {
                if (expectedOutput.get(i).equals(actualOutput.get(j))) {
                    match = true;
                    if (!ignoreOrder && i != j) {
                        LOG.error("Expected (" + expectedOutput.get(i) + ") matched (" + actualOutput.get(j)
                                + ") but in wrong place.  " + j + " instead of " + i);
                        error = true;
                    }

                    break;
                }
            }

            if (!match) {
                LOG.error("Missing expected output: (" + expectedOutput.get(i) + ")");
                error = true;
            }
        }

        for (String anActualOutput : actualOutput) {
            boolean match = false;
            for (String anExpectedOutput : expectedOutput) {
                if (anActualOutput.equals(anExpectedOutput)) {
                    match = true;
                    break;
                }
            }

            if (!match) {
                LOG.error("Received unexpected output: (" + anActualOutput + ")");
                error = true;
            }
        }

        return error;
    }

    /**
     * Opens the accessor and reads all output, giving it to the resolver to retrieve the list of fields. These fields
     * are then added to a string, delimited by commas, and returned in a list.
     *
     * @param accessor The accessor instance to use
     * @param resolver The resolver instance to use
     * @return The list of output strings
     * @throws Exception
     */
    protected List<String> getAllOutput(Accessor accessor, Resolver resolver) throws Exception {

        Assert.assertTrue("Accessor failed to open", accessor.openForRead());

        List<String> output = new ArrayList<String>();

        OneRow row;
        while ((row = accessor.readNextObject()) != null) {

            StringBuilder bldr = new StringBuilder();
            for (OneField field : resolver.getFields(row)) {
                bldr.append((field != null && field.val != null ? field.val : "") + ",");
            }

            if (bldr.length() > 0) {
                bldr.deleteCharAt(bldr.length() - 1);
            }

            output.add(bldr.toString());
        }

        accessor.closeForRead();

        return output;
    }

    /**
     * Gets an instance of Fragmenter via reflection.
     * <p>
     * Searches for a constructor that has a single parameter of some BaseMetaData type
     *
     * @return A Fragmenter instance
     * @throws Exception If something bad happens
     */
    protected Fragmenter getFragmenter(RequestContext meta) throws Exception {

        Constructor<?> c = getFragmenterClass().getConstructor();
        Fragmenter fragmenter = (Fragmenter) c.newInstance();
        fragmenter.initialize(meta);

        /*
        Fragmenter fragmenter = null;

        for (Constructor<?> c : getFragmenterClass().getConstructors()) {
            if (c.getParameterTypes().length == 1) {
                for (Class<?> clazz : c.getParameterTypes()) {
                    if (RequestContext.class.isAssignableFrom(clazz)) {
                        fragmenter = (Fragmenter) c.newInstance(meta);
                    }
                }
            }
        }

        if (fragmenter == null) {
            throw new InvalidParameterException("Unable to find Fragmenter constructor with a BaseMetaData parameter");
        }
*/
        return fragmenter;

    }

    /**
     * Gets an instance of Accessor via reflection.
     * <p>
     * Searches for a constructor that has a single parameter of some RequestContext type
     *
     * @return An Accessor instance
     * @throws Exception If something bad happens
     */
    protected Accessor getReadAccessor(RequestContext data) throws Exception {

        Constructor<?> c = getAccessorClass().getConstructor();
        Accessor accessor = (Accessor) c.newInstance();
        accessor.initialize(data);

        /*

        for (Constructor<?> c : getAccessorClass().getConstructors()) {
            if (c.getParameterTypes().length == 1) {
                for (Class<?> clazz : c.getParameterTypes()) {
                    if (RequestContext.class.isAssignableFrom(clazz)) {
                        accessor = (Accessor) c.newInstance(data);
                    }
                }
            }
        }

        if (accessor == null) {
            throw new InvalidParameterException("Unable to find Accessor constructor with a BaseMetaData parameter");
        }
        */

        return accessor;

    }

    /**
     * Gets an instance of IFieldsResolver via reflection.
     * <p>
     * Searches for a constructor that has a single parameter of some BaseMetaData type
     *
     * @return A IFieldsResolver instance
     * @throws Exception If something bad happens
     */
    protected Resolver getReadResolver(RequestContext data) throws Exception {

        Constructor<?> c = getResolverClass().getConstructor();
        Resolver resolver = (Resolver) c.newInstance();
        resolver.initialize(data);

        /*

        // search for a constructor that has a single parameter of a type of
        // BaseMetaData to create the accessor instance
        for (Constructor<?> c : getResolverClass().getConstructors()) {
            if (c.getParameterTypes().length == 1) {
                for (Class<?> clazz : c.getParameterTypes()) {
                    if (RequestContext.class.isAssignableFrom(clazz)) {
                        resolver = (Resolver) c.newInstance(data);
                    }
                }
            }
        }

        if (resolver == null) {
            throw new InvalidParameterException("Unable to find Resolver constructor with a BaseMetaData parameter");
        }
        */

        return resolver;
    }

    public static class Pair<FIRST, SECOND> {

        public FIRST first;
        public SECOND second;

        public Pair() {
        }

        public Pair(FIRST f, SECOND s) {
            this.first = f;
            this.second = s;
        }
    }
}
