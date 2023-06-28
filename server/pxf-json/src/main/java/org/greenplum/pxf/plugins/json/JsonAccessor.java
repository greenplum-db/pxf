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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SpringContext;
import org.greenplum.pxf.plugins.hdfs.LineBreakAccessor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * This JSON accessor for PXF provides support for reading and writing Json formatted files.
 * <p>
 * For the reading use case it will read JSON data and pass it to a {@link JsonResolver}.
 * It supports a single JSON record per line, or a multi-line JSON records if the <b>IDENTIFIER</b> parameter is set.
 * When provided the <b>IDENTIFIER</b> indicates the member name to determine the encapsulating json object to return.
 * <p>
 * For the writing use case it will serialize tuple data received from {@link JsonResolver} into a JSON or JSONL format.
 * By default, the data will be written in JSONL format, where each database tuple is written on a separate line
 * and is represented by a Json object:
 * <pre>
 * {..tuple1..}
 * {..tuple2..}
 * </pre>
 * The file will have the .jsonl extension to denote that it is not a valid parsable Json by itself.
 * However, each row will be a valid Json object representing a database tuple.
 * <p>
 * Alternatively, if the <b>ROOT</b> option is provided with a non-empty value in the LOCATION URI the data will be
 * written as a valid JSON object with the root level attribute having the name given by the option value.
 * The value of the attribute will be an array of JSON objects, each one representing a database tuple.
 * For example, if the option ROOT=records is provided, the data in each file will look like:
 * <pre>
 * {"records":[
 * {..tuple1..}
 * ,{..tuple2..}
 * ...
 * ]}
 * </pre>
 * The file will have the .json extension to denote that it is a valid parsable Json by itself.
 * <p>
 * Files will be written compressed if the "COMPRESSION_CODEC" external table option is specified.
 */
public class JsonAccessor extends LineBreakAccessor {

    // parameters for read use case
    public static final String IDENTIFIER_PARAM = "IDENTIFIER";
    public static final String RECORD_MAX_LENGTH_PARAM = "MAXLENGTH";
    private static final String UNSUPPORTED_ERR_MESSAGE = "JSON accessor does not support write operation.";

    // parameters for write use case
    private static final String JSON_FILE_EXTENSION = ".json";
    private static final String JSONL_FILE_EXTENSION = ".jsonl";
    private static final String ROOT_PARAM = "ROOT";

    private static final JsonFactory jsonFactory = new JsonFactory();
    private static final String NEWLINE = "\n"; //TODO: this can be made configurable

    /**
     * If provided indicates the member name which will be used to determine the encapsulating json object to return.
     */
    private String identifier = "";

    /**
     * Optional parameter that allows to define the max length of a json record. Records that exceed the allowed length
     * are skipped. This parameter is applied only for the multi-line json records (e.g. when the IDENTIFIER is
     * provided).
     */
    private int maxRecordLength = Integer.MAX_VALUE;

    /**
     * for an object layout the name of the root element that will have a tuple array as the value
     */
    private String rootName;
    private JsonGenerator jsonGenerator;
    private ColumnDescriptor[] columnDescriptors;
    private boolean isFirstRecord;

    private final JsonUtilities jsonUtilities;

    /**
     * Constructs a new instance of the JsonAccessor
     */
    public JsonAccessor() {
        this(SpringContext.getBean(JsonUtilities.class));
    }

    JsonAccessor(JsonUtilities jsonUtilities) {
        // we do not use InputFormat for reading, set it to null.
        super(null);
        this.jsonUtilities = jsonUtilities;
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        columnDescriptors = context.getTupleDescription().toArray(new ColumnDescriptor[0]);
        // set the internal properties appropriate for either read or write use case
        if (context.getRequestType() == RequestContext.RequestType.READ_BRIDGE) {
            initReadProperties();
        } else {
            initWriteProperties();
        }
    }

    @Override
    protected String getFileExtension() {
        // use json if a root element is requested, jsonl otherwise
        return (rootName != null) ? JSON_FILE_EXTENSION : JSONL_FILE_EXTENSION;
    }

    @Override
    protected Object getReader(JobConf conf, InputSplit split) throws IOException {
        if (!isEmpty(identifier)) {
            conf.set(JsonRecordReader.RECORD_MEMBER_IDENTIFIER, identifier);
            conf.setInt(JsonRecordReader.RECORD_MAX_LENGTH, maxRecordLength);
            return new JsonRecordReader(conf, (FileSplit) split);
        } else {
            return new LineRecordReader(conf, (FileSplit) split);
        }
    }

    /**
     * Opens the resource for write and writes a header, if applicable.
     *
     * @return true if the resource is successfully opened
     * @throws Exception if opening the resource failed
     */
    @Override
    public boolean openForWrite() throws IOException {
        boolean result = super.openForWrite();
        // this should not really happen, but complying with the interface, if the operation returned false and
        // there was no exception, there is nothing else to do here, just propagate the result to caller
        if (!result) {
            return false;
        }

        // setup Json machinery, allow for use of UTF8 encoding only
        jsonGenerator = jsonFactory.createGenerator((OutputStream) dos, JsonEncoding.UTF8);
        jsonGenerator.setRootValueSeparator(null); // do not separate top level objects, we will add NEWLINE ourselves

        // write the file header, if object layout is requested
        if (rootName != null) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName(rootName);
            jsonGenerator.writeStartArray();
        }
        return true;
    }

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     * @throws Exception writing to the resource failed
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean writeNextObject(OneRow onerow) throws IOException {
        // resolver just passed List<OneField> to us without resolving anything
        List<OneField> record = (List<OneField>) onerow.getData();
        if (record == null) {
            return false;
        }
        // make sure the record and column list have the same size
        if (record.size() != columnDescriptors.length) {
            throw new PxfRuntimeException(
                    String.format("Unexpected: number of fields in the record %d is different from number of table columns %d",
                            record.size(), columnDescriptors.length));
        }
        // start each object on a new line - in object layout for all lines and in non-object for all but the first
        if (rootName != null || !isFirstRecord) {
            jsonGenerator.writeRaw(NEWLINE);
        }
        // no matter what the layout is we need to write an object out
        jsonGenerator.writeStartObject();

        // iterate over columns, use the generator to write properties and their values
        int columnIndex = 0;
        for (OneField field : record) {
            jsonUtilities.writeField(jsonGenerator, columnDescriptors[columnIndex++], field);
        }
        jsonGenerator.writeEndObject();
        isFirstRecord = false;
        return true;
    }

    /**
     * Closes the resource for write.
     *
     * @throws Exception if closing the resource failed
     */
    @Override
    public void closeForWrite() throws IOException {
        boolean caughtException = false;
        try {
            // write the file footer, if object layout is requested
            if (rootName != null) {
                jsonGenerator.writeRaw(NEWLINE);
                jsonGenerator.writeEndArray();
                jsonGenerator.writeEndObject();
            }
            jsonGenerator.flush();
            super.closeForWrite(); // to flush / close the output stream
        } catch (IOException e) {
            // remember that the exception was caught and rethrow it to propagate upwards
            caughtException = true;
            throw e;
        } finally {
            if (jsonGenerator != null) {
                try {
                    jsonGenerator.close();
                } catch (IOException e) {
                    // generator close failed, but if there was a more important exception caught before, suppress this one
                    if (caughtException) {
                        // suppress the new exception, just log its message and let the original one propagate
                        LOG.warn("Suppressing exception when closing Json generator: ", e.getMessage());
                    } else {
                        // since this is the first and only exception we see, throw it
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * Initialize the internal properties for the read use case.
     */
    private void initReadProperties() {
        if (!isEmpty(context.getOption(IDENTIFIER_PARAM))) {
            identifier = context.getOption(IDENTIFIER_PARAM);
            // If the member identifier is set then check if a record max length is defined as well.
            if (!isEmpty(context.getOption(RECORD_MAX_LENGTH_PARAM))) {
                maxRecordLength = Integer.valueOf(context.getOption(RECORD_MAX_LENGTH_PARAM));
            }
        }
    }

    /**
     * Initialize the internal properties for the write use case.
     */
    private void initWriteProperties() {
        // store the root element name, if any (for object layout)
        rootName = context.getOption(ROOT_PARAM);
        if (rootName != null && StringUtils.isBlank(rootName)) {
            throw new PxfRuntimeException("Option ROOT can not have an empty value");
        }
        validateUTF8Encoding();
        isFirstRecord = true;
    }

    /**
     * Validates that the data being written will be in UTF8 encoding. Checks for data encoding first and if it is
     * not specified, checks for the database encoding to make sure the effective encoding will be UTF8. Throws
     * a PxfRuntimeException if the effective encoding is not UTF8.
     */
    private void validateUTF8Encoding() {
        // make sure for write case Greenplum sends data in UTF8 encoding according to the Json standard
        // in the case of an external table with pxfwritable_export formatter the formatter itself will enforce that
        // the encoding is UTF8, but this check will still be relevant for FDW that is not using the formatter
        if (context.getRequestType().equals(RequestContext.RequestType.WRITE_BRIDGE)) {
            Charset encoding = context.getDataEncoding();
            encoding = (encoding != null) ? encoding : context.getDatabaseEncoding();
            if (encoding == null || !encoding.equals(StandardCharsets.UTF_8)) {
                throw new PxfRuntimeException(
                        String.format("Effective data encoding %s is not UTF8 and is not supported.", encoding),
                        "Make sure either the database is in UTF8 or the table is defined with ENCODING 'UTF8' option.");
            }
        }
    }

}
