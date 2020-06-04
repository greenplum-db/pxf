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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.greenplum.pxf.api.BadRecordException;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This JSON resolver for PXF will decode a given object from the {@link JsonAccessor} into a row for GPDB. It will
 * decode this data into a JsonNode and walk the tree for each column. It supports normal value mapping via projections
 * and JSON array indexing.
 */
@Component("JsonResolver")
@RequestScope
public class JsonResolver extends BasePlugin implements Resolver {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ArrayList<OneField> oneFieldList;
    private ColumnDescriptorCache[] columnDescriptorCache;

    @Override
    public void initialize() {
        oneFieldList = new ArrayList<>();

        // Precompute the column metadata. The metadata is used for mapping column names to json nodes.
        columnDescriptorCache = new ColumnDescriptorCache[context.getColumns()];
        for (int i = 0; i < context.getColumns(); ++i) {
            ColumnDescriptor cd = context.getColumn(i);
            columnDescriptorCache[i] = new ColumnDescriptorCache(cd);
        }
    }

    @Override
    public List<OneField> getFields(OneRow row) throws Exception {
        oneFieldList.clear();

        if (row == null || row.getData() == null) {
            throw new BadRecordException("json record is null");
        }
        String jsonRecordAsText = row.getData().toString();

        JsonNode root;
        try {
            root = MAPPER.readTree(jsonRecordAsText);
        } catch (IOException e) {
            throw new BadRecordException(
                    String.format("error while parsing json record '%s'. invalid JSON record\n%s", e.getMessage(), jsonRecordAsText), e);
        }

        // Iterate through the column definition and fetch our JSON data
        for (ColumnDescriptorCache columnMetadata : columnDescriptorCache) {

            JsonNode node = getChildJsonNode(root, columnMetadata.getNormalizedProjections());

            // If this node is null or missing, add a null value here
            if (node == null || node.isMissingNode()) {
                addNullField(columnMetadata.getColumnType());
            } else if (columnMetadata.isArray()) {
                // If this column is an array index, ex. "tweet.hashtags[0]"
                if (node.isArray()) {
                    // If the JSON node is an array, then add it to our list
                    addFieldFromJsonArray(columnMetadata.getColumnType(), node, columnMetadata.getArrayNodeIndex());
                } else {
                    throw new IllegalStateException(columnMetadata.getColumnName() + " is not an array node");
                }
            } else {
                // This column is not an array type
                // Add the value to the record
                addFieldFromJsonNode(columnMetadata.getColumnType(), node);
            }
        }

        return oneFieldList;
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     * @throws UnsupportedOperationException if constructing a row from the fields failed
     */
    @Override
    public OneRow setFields(List<OneField> record) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Iterates down the root node to the child JSON node defined by the projs path.
     *
     * @param root  node to to start the traversal from.
     * @param projs defines the path from the root to the desired child node.
     * @return Returns the child node defined by the root and projs path.
     */
    private JsonNode getChildJsonNode(JsonNode root, String[] projs) {

        // Iterate through all the tokens to the desired JSON node
        JsonNode node = root;
        for (String proj : projs) {
            node = node.path(proj);
        }

        return node;
    }

    /**
     * Iterates through the given JSON node to the proper index and adds the field of corresponding type
     *
     * @param type  The {@link DataType} type
     * @param node  The JSON array node
     * @param index The array index to iterate to
     * @throws IOException, BadRecordException
     */
    private void addFieldFromJsonArray(DataType type, JsonNode node, int index) throws IOException, BadRecordException {

        int count = 0;
        boolean added = false;
        for (Iterator<JsonNode> arrayNodes = node.elements(); arrayNodes.hasNext(); ) {
            JsonNode arrayNode = arrayNodes.next();

            if (count == index) {
                added = true;
                addFieldFromJsonNode(type, arrayNode);
                break;
            }

            ++count;
        }

        // if we reached the end of the array without adding a field, add null
        if (!added) {
            addNullField(type);
        }
    }

    /**
     * Adds a field from a given {@link JsonNode} value based on the {@link DataType} type.
     *
     * @param type The DataType type
     * @param val  The JSON node to extract the value.
     * @throws IOException, BadRecordException when there is bad data in the {@link JsonNode}
     */
    private void addFieldFromJsonNode(DataType type, JsonNode val) throws IOException, BadRecordException {
        if (val.isNull()) {
            addNullField(type);
            return;
        }

        OneField oneField = new OneField();
        oneField.type = type.getOID();

        // validate numeric types and booleans
        switch (type) {
            case BIGINT:
            case FLOAT8:
            case REAL:
            case INTEGER:
            case SMALLINT:
                validateNumber(type, val);
                break;
            case BOOLEAN:
                validateBoolean(val);
                break;
        }

        switch (type) {
            case BIGINT:
                oneField.val = val.asLong();
                break;
            case BOOLEAN:
                oneField.val = val.asBoolean();
                break;
            case BYTEA:
                oneField.val = val.asText().getBytes();
                break;
            case FLOAT8:
                oneField.val = val.asDouble();
                break;
            case REAL:
                oneField.val = (float) val.asDouble();
                break;
            case INTEGER:
                oneField.val = val.asInt();
                break;
            case SMALLINT:
                oneField.val = (short) val.asInt();
                break;
            case BPCHAR:
            case TEXT:
            case VARCHAR:
                oneField.val = val.asText();
                break;
            default:
                throw new IOException("Unsupported type " + type);
        }

        oneFieldList.add(oneField);
    }

    /**
     * Determines whether or not a {@link JsonNode} contains a valid numeric type.
     * When val is not a valid numeric type, the default is always returned
     * when calling {@link JsonNode#asLong()}. If not a numeric type, error out.
     *
     * @param type is used to report which numeric type is being validated
     * @param val  is the {@link JsonNode} that we are validating
     * @throws BadRecordException when there is a data mismatch (non-numeric data)
     */
    private static void validateNumber(DataType type, JsonNode val) throws BadRecordException {
        // to validate if val is of numeric type:
        // if val is not a number, 0 will be returned by val.asLong(0)
        // we need to check with another default in case val is actually 0
        if (val.asLong(0) == 0 && val.asLong(1) == 1) {
            throw new BadRecordException(String.format("invalid %s input value '%s'", type, val));
        }
    }

    /**
     * Determines whether or not a {@link String} contains a valid
     * {@link Boolean}, if not, error out.
     * <p>
     *
     * @param val is the {@link JsonNode} that we are validating
     * @throws BadRecordException when there is a data mismatch (non-Boolean data)
     */
    private static void validateBoolean(JsonNode val) throws BadRecordException {
        // similar approach as validateNumber()
        if (!val.asBoolean(false) && val.asBoolean(true)) {
            throw new BadRecordException(String.format("invalid BOOLEAN input value '%s'", val));
        }
    }

    /**
     * Adds a null field of the given type.
     *
     * @param type The {@link DataType} type
     */
    private void addNullField(DataType type) {
        oneFieldList.add(new OneField(type.getOID(), null));
    }
}
