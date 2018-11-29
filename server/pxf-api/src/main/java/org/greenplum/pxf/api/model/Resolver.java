package org.greenplum.pxf.api.model;

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


import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;

import java.util.List;

/**
 * Interface that defines the deserialization of one record brought from the {@link Accessor}.
 * All deserialization methods (e.g, Writable, Avro, ...) implement this interface.
 *
 * Interface that defines the serialization of data read from the DB into a OneRow object.
 * This interface is implemented by all serialization methods (e.g, Writable, Avro, ...).
 */
public interface Resolver extends Plugin {
    /**
     * Gets the {@link OneField} list of one row.
     *
     * @param row the row to get the fields from
     * @return the {@link OneField} list of one row.
     * @throws Exception if decomposing the row into fields failed
     */
    List<OneField> getFields(OneRow row) throws Exception;

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     * @throws Exception if constructing a row from the fields failed
     */
    OneRow setFields(List<OneField> record) throws Exception;
}
