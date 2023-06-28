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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonExtensionTest {

    private static final Log LOG = LogFactory.getLog(JsonExtensionTest.class);
    private static final String IDENTIFIER = JsonAccessor.IDENTIFIER_PARAM;
    private List<Pair<String, DataType>> columnDefs = null;
    private final List<Pair<String, String>> extraParams = new ArrayList<>();
    private final List<String> output = new ArrayList<>();
    private List<RequestContext> inputs;

    @BeforeEach
    public void before() {

        columnDefs = new ArrayList<>();

        columnDefs.add(new Pair<>("created_at", DataType.TEXT));
        columnDefs.add(new Pair<>("id", DataType.BIGINT));
        columnDefs.add(new Pair<>("text", DataType.TEXT));
        columnDefs.add(new Pair<>("user.screen_name", DataType.TEXT));
        columnDefs.add(new Pair<>("entities.hashtags[0]", DataType.TEXT));
        columnDefs.add(new Pair<>("coordinates.coordinates[0]", DataType.FLOAT8));
        columnDefs.add(new Pair<>("coordinates.coordinates[1]", DataType.FLOAT8));

        output.clear();
        extraParams.clear();
    }

    @AfterEach
    public void cleanup() {
        columnDefs.clear();
    }

    @Test
    public void testCompressedMultilineJsonFile() throws Exception {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets.tar.gz"), output);
    }

    @Test
    public void testMaxRecordLength() throws Exception {

        // variable-size-objects.json contains 3 json objects but only 2 of them fit in the 27 byte length limitation

        extraParams.add(new Pair<>(IDENTIFIER, "key666"));
        extraParams.add(new Pair<>("MAXLENGTH", "27"));

        columnDefs.clear();
        columnDefs.add(new Pair<>("key666", DataType.TEXT));

        output.add("small object1");
        // skip the large object2 XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        output.add("small object3");

        assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/variable-size-objects.json"), output);
    }

    @Test
    public void testDataTypes() throws Exception {

        // TODO: The BYTEA type is not tested. The implementation (val.asText().getBytes()) returns an array reference
        // and it is not clear whether this is the desired behavior.
        //
        // For the time being avoid using BYTEA type!!!

        // This test also verifies that the order of the columns in the table definition agnostic to the order of the
        // json attributes.

        extraParams.add(new Pair<>(IDENTIFIER, "bintType"));

        columnDefs.clear();

        columnDefs.add(new Pair<>("text", DataType.TEXT));
        columnDefs.add(new Pair<>("varcharType", DataType.VARCHAR));
        columnDefs.add(new Pair<>("bpcharType", DataType.BPCHAR));
        columnDefs.add(new Pair<>("smallintType", DataType.SMALLINT));
        columnDefs.add(new Pair<>("integerType", DataType.INTEGER));
        columnDefs.add(new Pair<>("realType", DataType.REAL));
        columnDefs.add(new Pair<>("float8Type", DataType.FLOAT8));
        // The DataType.BYTEA type is left out for further validation.
        columnDefs.add(new Pair<>("booleanType", DataType.BOOLEAN));
        columnDefs.add(new Pair<>("bintType", DataType.BIGINT));

        output.add(",varcharType,bpcharType,777,999,3.15,3.14,true,666");

        assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/datatypes-test.json"), output);
    }

    @Test
    public void testMissingArrayJsonAttribute() {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        columnDefs.clear();

        columnDefs.add(new Pair<>("created_at", DataType.TEXT));
        // User is not an array! An attempt to access it should throw an exception!
        columnDefs.add(new Pair<>("user[0]", DataType.TEXT));

        assertThrows(IllegalStateException.class,
                () -> assertOutput(new Path(System.getProperty("user.dir") + File.separator
                        + "src/test/resources/tweets-with-missing-text-attribtute.json"), output));
    }

    @Test
    public void testMissingJsonAttribute() throws Exception {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        // Missing attributes are substituted by an empty field
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,,SpreadButter,tweetCongress,,");

        assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-with-missing-text-attribtute.json"), output);
    }

    @Test
    public void testMalformedJsonObject() {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> assertOutput(new Path(System.getProperty("user.dir") + File.separator
                        + "src/test/resources/tweets-broken.json"), output));
        assertTrue(e.getMessage().contains("error while parsing json record 'Unexpected character (':' (code 58)): was expecting comma to separate"));
    }

    @Test
    public void testMismatchedTypes() {

        BadRecordException e = assertThrows(BadRecordException.class,
                () -> assertOutput(new Path(System.getProperty("user.dir") + File.separator
                        + "src/test/resources/mismatched-types.json"), output));
        assertEquals("invalid BIGINT input value '\"[\"'", e.getMessage());
    }

    @Test
    public void testSmallTweets() throws Exception {

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");
        output.add("Fri Jun 07 22:45:03 +0000 2013,343136551322136576,text4,SevenStonesBuoy,,-6.1,50.103");

        assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-small.json"), output);
    }

    @Test
    public void testTweetsWithNull() throws Exception {

        output.add("Fri Jun 07 22:45:02 +0000 2013,,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,,text2,patronusdeadly,,,");

        assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/null-tweets.json"), output);
    }

    @Test
    public void testSmallTweetsWithDelete() throws Exception {

        output.add(",,,,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-small-with-delete.json"), output);
    }

    @Test
    public void testWellFormedJson() throws Exception {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-pp.json"), output);
    }

    @Test
    public void testWellFormedJsonWithDelete() throws Exception {

        extraParams.add(new Pair<>(IDENTIFIER, "created_at"));

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        assertOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-pp-with-delete.json"), output);
    }

    @Test
    public void testMultipleFiles() throws Exception {

        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");
        output.add("Fri Jun 07 22:45:03 +0000 2013,343136551322136576,text4,SevenStonesBuoy,,-6.1,50.103");
        output.add(",,,,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547115253761,text1,SpreadButter,tweetCongress,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547123646465,text2,patronusdeadly,,,");
        output.add("Fri Jun 07 22:45:02 +0000 2013,343136547136233472,text3,NoSecrets_Vagas,,,");

        assertUnorderedOutput(new Path(System.getProperty("user.dir") + File.separator
                + "src/test/resources/tweets-small*.json"), output);
    }

    /**
     * Uses the given input directory to run through the PXF unit testing framework. Uses the lines in the given
     * parameter for output testing.
     *
     * @param input          Input records
     * @param expectedOutput File containing output to check
     * @throws Exception
     */
    private void assertOutput(Path input, List<String> expectedOutput) throws Exception {
        setup(input);
        List<String> actualOutput = new ArrayList<>();
        for (RequestContext data : inputs) {
            Accessor accessor = getReadAccessor(data);
            Resolver resolver = getReadResolver(data);

            actualOutput.addAll(getAllOutput(accessor, resolver));
        }

        assertFalse(compareOutput(expectedOutput, actualOutput), "Output did not match expected output");
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
    private void assertUnorderedOutput(Path input, List<String> expectedOutput) throws Exception {

        setup(input);

        List<String> actualOutput = new ArrayList<>();
        for (RequestContext data : inputs) {
            Accessor accessor = getReadAccessor(data);
            Resolver resolver = getReadResolver(data);

            actualOutput.addAll(getAllOutput(accessor, resolver));
        }

        assertFalse(compareUnorderedOutput(expectedOutput, actualOutput), "Output did not match expected output");
    }

    /**
     * Set all necessary parameters for GPXF framework to function. Uses the given path as a single input split.
     *
     * @param input The input path, relative or absolute.
     * @throws Exception when an error occurs
     */
    private void setup(Path input) throws Exception {

        RequestContext context = getContext(input);
        List<Fragment> fragments = getFragmenter(context).getFragments();

        inputs = new ArrayList<>();

        for (int i = 0; i < fragments.size(); i++) {
            Fragment fragment = fragments.get(i);
            context = getContext(input);
            context.setDataSource(fragment.getSourceName());
            context.setFragmentMetadata(fragment.getMetadata());
            context.setFragmentIndex(i);
            inputs.add(context);
        }
    }

    private RequestContext getContext(Path input) {
        Configuration configuration = new Configuration();
        configuration.set("pxf.fs.basePath", "/");

        RequestContext context = new RequestContext();
        context.setConfiguration(configuration);

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

        for (int i = 0; i < columnDefs.size(); ++i) {
            Pair<String, DataType> columnDef = columnDefs.get(i);
            ColumnDescriptor column = new ColumnDescriptor(columnDef.first, columnDef.second.getOID(), i, columnDef.second.name(), null);
            context.getTupleDescription().add(column);
        }

        // HDFSMetaData properties
        context.setFragmenter(HdfsDataFragmenter.class.getName());
        context.setAccessor(JsonAccessor.class.getName());
        context.setResolver(JsonResolver.class.getName());

        for (Pair<String, String> param : extraParams) {
            context.addOption(param.first, param.second);
        }

        return context;
    }

    /**
     * Compares the expected and actual output, printing out any errors.
     *
     * @param expectedOutput The expected output
     * @param actualOutput   The actual output
     * @return True if no errors, false otherwise.
     */
    private boolean compareOutput(List<String> expectedOutput, List<String> actualOutput) {
        return compareOutput(expectedOutput, actualOutput, false);
    }

    /**
     * Compares the expected and actual output, printing out any errors.
     *
     * @param expectedOutput The expected output
     * @param actualOutput   The actual output
     * @return True if no errors, false otherwise.
     */
    private boolean compareUnorderedOutput(List<String> expectedOutput, List<String> actualOutput) {
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
     * @throws Exception when an error occurs
     */
    private List<String> getAllOutput(Accessor accessor, Resolver resolver) throws Exception {

        assertTrue(accessor.openForRead(), "Accessor failed to open");

        List<String> output = new ArrayList<>();

        OneRow row;
        while ((row = accessor.readNextObject()) != null) {

            StringJoiner stringJoiner = new StringJoiner(",");
            for (OneField field : resolver.getFields(row)) {
                stringJoiner.add(field != null && field.val != null ? field.val.toString() : "");
            }

            output.add(stringJoiner.toString());
        }

        accessor.closeForRead();

        return output;
    }

    private Fragmenter getFragmenter(RequestContext meta) {
        HdfsDataFragmenter hdfsDataFragmenter = new HdfsDataFragmenter();
        hdfsDataFragmenter.setRequestContext(meta);
        hdfsDataFragmenter.afterPropertiesSet();
        return hdfsDataFragmenter;

    }

    private Accessor getReadAccessor(RequestContext data)  {
        JsonUtilities jsonUtilities = new JsonUtilities();
        jsonUtilities.setPgUtilities(new PgUtilities());
        JsonAccessor jsonAccessor = new JsonAccessor(jsonUtilities, new JsonFactory());
        data.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        jsonAccessor.setRequestContext(data);
        jsonAccessor.afterPropertiesSet();
        return jsonAccessor;

    }

    private Resolver getReadResolver(RequestContext data) {
        JsonResolver jsonResolver = new JsonResolver(new PgUtilities());
        jsonResolver.setRequestContext(data);
        jsonResolver.afterPropertiesSet();
        return jsonResolver;
    }

    private static class Pair<FIRST, SECOND> {
        public FIRST first;
        public SECOND second;

        public Pair(FIRST f, SECOND s) {
            first = f;
            second = s;
        }
    }
}
