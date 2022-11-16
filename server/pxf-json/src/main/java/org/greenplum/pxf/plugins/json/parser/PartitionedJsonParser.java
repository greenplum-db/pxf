package org.greenplum.pxf.plugins.json.parser;

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

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.EnumSet;

import org.greenplum.pxf.plugins.json.parser.JsonLexer.JsonLexerState;

/**
 * A simple parser that builds up a JSON object from the supplied char fed in. The parser searches for the JSON
 * object containing the member string that the user supplies.
 *
 * It tracks and builds up the JSON object as it does this search. This code assumes that the user has found the first
 * JSON begin-object "{". It will return only the object containing the member string. For example, if there is a nested
 * JSON object and the member string is in the nested object, the parser will only return the nested object.
 *
 * It is not recommended to use this with JSON text where individual JSON objects that can be large (MB's or larger).
 */
public class PartitionedJsonParser {

	private static final char START_BRACE = '{';
	private final JsonLexer lexer;
	private String memberName;

	private MemberSearchState memberState;
	private StringBuilder currentObject;
	private StringBuilder currentStringLiteral;
	private int objectCount;
	private ArrayDeque<Integer> objectStack;
	private boolean isCompletedObject;
	// track the average size of JSON objects produced to avoid extraneous buffer copies
	private int averageObjectSize;
	private int numObjectsRead;
	public PartitionedJsonParser(String memberName) {
		this.lexer = new JsonLexer();

		this.memberName = memberName;
		this.memberState = MemberSearchState.SEARCHING;

		// set the capacity for average json object size somewhat large to start
		this.averageObjectSize = 4132;
		this.numObjectsRead = 0;

		this.objectStack = new ArrayDeque<>();
	}
	private enum MemberSearchState {
		FOUND_STRING_NAME,

		SEARCHING,

		IN_MATCHING_OBJECT,

		STRING_NOT_FOUND
	}

	private static final EnumSet<JsonLexerState> inStringStates = EnumSet.of(JsonLexerState.INSIDE_STRING,
			JsonLexerState.STRING_ESCAPE);

	public void startNewJsonObject() {
		lexer.setState(JsonLexerState.BEGIN_OBJECT);
		this.memberState = MemberSearchState.SEARCHING;

		this.objectCount = 0;
		this.currentObject = new StringBuilder(averageObjectSize);
		// as this variable only holds a json key or json value, it can be relatively small.
		this.currentStringLiteral = new StringBuilder(32);
		this.objectStack = new ArrayDeque<>();
		this.isCompletedObject = false;

		currentObject.append(START_BRACE);
		objectStack.push(0);
	}
	/**
	 * @param c character to parse
	 *            Indicates the member name used to determine the encapsulating object to return.
	 * @return true  if the JSON object containing the member name has been completed or true if the
	 * 				ending bracket for the original begin bracket is found, false otherwise. This means that
	 * 			    if a nested json object contains the member name, only the nested json will be returned.
	 * 			    EX:
	 * 			    1: {
	 * 			    2: "a":1,
	 * 			    3: "b":
	 * 			    4: { "d":3
	 * 			    5: },
	 * 			    6: "c":2
	 * 			    7: }
	 *				In this example, if the member name was "d" we would return true at line 5, and currentObject would contain only lines 4-5
	 *			    If the member name was "a", we would return true at line 7, and current object would contain lines 1-7
	 *			    If the member name was "x", we would return true at line 7, but current object would be empty
	 */
	public boolean parse(char c) {
		lexer.lex(c);

		currentObject.append(c);

		switch (memberState) {
		case SEARCHING:
			if (lexer.getState() == JsonLexerState.BEGIN_STRING) {
				// we found the start of a string, so reset our string buffer
				currentStringLiteral.setLength(0);
			} else if (inStringStates.contains(lexer.getState())) {
				// we're still inside a string, so keep appending to our buffer
				currentStringLiteral.append(c);
			} else if (lexer.getState() == JsonLexerState.END_STRING && memberName.equals(currentStringLiteral.toString())) {

				if (!objectStack.isEmpty()) {
					// we hit the end of the string and it matched the member name (yay)
					memberState = MemberSearchState.FOUND_STRING_NAME;
					currentStringLiteral.setLength(0);
				}
			} else if (lexer.getState() == JsonLexerState.BEGIN_OBJECT) {
				// we are searching and found a '{', so we reset the current object string
				if (objectStack.isEmpty()) {
					currentObject.setLength(0);
					currentObject.append(START_BRACE);
				}
				objectStack.push(currentObject.length() - 1);
			} else if (lexer.getState() == JsonLexerState.END_OBJECT) {
				if (!objectStack.isEmpty()) {
					objectStack.pop();
				}
				if (objectStack.isEmpty()) {
					// we found a '}' at the same level as the first '{' and nothing was found
					currentObject.setLength(0);
					memberState = MemberSearchState.STRING_NOT_FOUND;
					isCompletedObject = true;
					numObjectsRead++;
					updateAverageJsonObjectSize();
					break;
				}
			}
			break;
		case FOUND_STRING_NAME:
			// keep popping whitespaces until we hit a different token
			if (lexer.getState() != JsonLexerState.WHITESPACE) {
				if (lexer.getState() == JsonLexerState.NAME_SEPARATOR) {
					// found our member!
					memberState = MemberSearchState.IN_MATCHING_OBJECT;
					objectCount = 0;

					if (objectStack.size() > 1) {
						currentObject.delete(0, objectStack.peek());
					}
					objectStack.clear();
				} else {
					// we didn't find a value-separator (:), so our string wasn't a member string. keep searching
					memberState = MemberSearchState.SEARCHING;
				}
			}
			break;
		case IN_MATCHING_OBJECT:
			if (lexer.getState() == JsonLexerState.BEGIN_OBJECT) {
				objectCount++;
			} else if (lexer.getState() == JsonLexerState.END_OBJECT) {
				objectCount--;
				if (objectCount < 0) {
					// we're done! we reached an "}" which is at the same level as the member we found
					isCompletedObject = true;
					numObjectsRead++;
					updateAverageJsonObjectSize();
					break;
				}
			}
			break;
		}

		return isCompletedObject;
	}

	/**
	 * If the object is complete, return it otherwise return empty string
	 * @return the completed JSON object
	 */
	public String getCompletedObject() {
		return isCompletedObject ? currentObject.toString() : "";
	}

	/**
	 * Regardless of whether the JSON object is complete, return true if a matching identifier was found.
	 * @return If the object contains the member, return true
	 */
	public boolean foundObjectWithIdentifier() {
		return memberState == MemberSearchState.FOUND_STRING_NAME || memberState == MemberSearchState.IN_MATCHING_OBJECT;
	}

	private void updateAverageJsonObjectSize() {
		int currentObjectSize = this.currentObject.toString().getBytes(StandardCharsets.UTF_8).length;
		if (numObjectsRead == 1) {
			averageObjectSize = currentObjectSize;
		} else {
			// recalculate the average: (averageObjectSize*(numObjectsRead - 1) + currentObjectSize) / numObjectsRead
			//                        = averageObjectSize + (currentObjectSize - averageObjectSize) / numObjectsRead
			averageObjectSize += (currentObjectSize - averageObjectSize)/numObjectsRead;
		}
	}
}
