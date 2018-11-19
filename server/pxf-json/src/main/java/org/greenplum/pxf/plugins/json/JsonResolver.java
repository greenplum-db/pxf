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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This JSON resolver for PXF will decode a given object from the {@link JsonAccessor} into a row for GPDB. It will
 * decode this data into a JsonNode and walk the tree for each column. It supports normal value mapping via projections
 * and JSON array indexing.
 */
public class JsonResolver extends BasePlugin implements Resolver {

	private static final Log LOG = LogFactory.getLog(JsonResolver.class);

	private ArrayList<OneField> oneFieldList;
	private ColumnDescriptorCache[] columnDescriptorCache;
	private ObjectMapper mapper;

	/**
	 * Row with empty fields. Returned in case of broken or malformed json records.
	 */
	private final List<OneField> emptyRow;

	public JsonResolver(RequestContext requestContext) throws Exception {
		initialize(requestContext);
		oneFieldList = new ArrayList<>();
		mapper = new ObjectMapper(new JsonFactory());

		// Precompute the column metadata. The metadata is used for mapping column names to json nodes.
		columnDescriptorCache = new ColumnDescriptorCache[requestContext.getColumns()];
		for (int i = 0; i < requestContext.getColumns(); ++i) {
			ColumnDescriptor cd = requestContext.getColumn(i);
			columnDescriptorCache[i] = new ColumnDescriptorCache(cd);
		}

		emptyRow = createEmptyRow();
	}

	@Override
	public List<OneField> getFields(OneRow row) throws Exception {
		oneFieldList.clear();

		String jsonRecordAsText = row.getData().toString();

		JsonNode root = decodeLineToJsonNode(jsonRecordAsText);

		if (root == null) {
			LOG.warn("Return empty-fields row due to invalid JSON: " + jsonRecordAsText);
			return emptyRow;
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
	 * @throws Exception if constructing a row from the fields failed
	 */
	@Override
	public OneRow setFields(List<OneField> record) throws Exception {
		throw new UnsupportedOperationException();
	}

	/**
	 * @return Returns a row comprised of typed, empty fields. Used as a result of broken/malformed json records.
	 */
	private List<OneField> createEmptyRow() {
		ArrayList<OneField> emptyFieldList = new ArrayList<OneField>();
		for (ColumnDescriptorCache column : columnDescriptorCache) {
			emptyFieldList.add(new OneField(column.getColumnType().getOID(), null));
		}
		return emptyFieldList;
	}

	/**
	 * Iterates down the root node to the child JSON node defined by the projs path.
	 *
	 * @param root
	 *            node to to start the traversal from.
	 * @param projs
	 *            defines the path from the root to the desired child node.
	 * @return Returns the child node defined by the root and projs path.
	 */
	private JsonNode getChildJsonNode(JsonNode root, String[] projs) {

		// Iterate through all the tokens to the desired JSON node
		JsonNode node = root;
		for (int j = 0; j < projs.length; ++j) {
			node = node.path(projs[j]);
		}

		return node;
	}

	/**
	 * Iterates through the given JSON node to the proper index and adds the field of corresponding type
	 *
	 * @param type
	 *            The {@link DataType} type
	 * @param node
	 *            The JSON array node
	 * @param index
	 *            The array index to iterate to
	 * @throws IOException
	 */
	private void addFieldFromJsonArray(DataType type, JsonNode node, int index) throws IOException {

		int count = 0;
		boolean added = false;
		for (Iterator<JsonNode> arrayNodes = node.getElements(); arrayNodes.hasNext();) {
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
	 * Adds a field from a given JSON node value based on the {@link DataType} type.
	 *
	 * @param type
	 *            The DataType type
	 * @param val
	 *            The JSON node to extract the value.
	 * @throws IOException
	 */
	private void addFieldFromJsonNode(DataType type, JsonNode val) throws IOException {
		OneField oneField = new OneField();
		oneField.type = type.getOID();

		if (val.isNull()) {
			oneField.val = null;
		} else {
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
				oneField.val = (float)val.asDouble();
				break;
			case INTEGER:
				oneField.val = val.asInt();
				break;
			case SMALLINT:
				oneField.val = (short)val.asInt();
				break;
			case BPCHAR:
			case TEXT:
			case VARCHAR:
				oneField.val = val.asText();
				break;
			default:
				throw new IOException("Unsupported type " + type);
			}
		}

		oneFieldList.add(oneField);
	}

	/**
	 * Adds a null field of the given type.
	 *
	 * @param type
	 *            The {@link DataType} type
	 */
	private void addNullField(DataType type) {
		oneFieldList.add(new OneField(type.getOID(), null));
	}

	/**
	 * Converts the input line parameter into {@link JsonNode} instance.
	 *
	 * @param line
	 *            JSON text
	 * @return Returns a {@link JsonNode} that represents the input line or null for invalid json.
	 */
	private JsonNode decodeLineToJsonNode(String line) {

		try {
			return mapper.readTree(line);
		} catch (Exception e) {
			LOG.error("Failed to parse JSON object", e);
			return null;
		}
	}
}
