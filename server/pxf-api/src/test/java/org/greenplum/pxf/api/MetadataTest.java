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

package org.greenplum.pxf.api;

import org.greenplum.pxf.api.model.Metadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MetadataTest {

    @Test
    public void createFieldEmptyNameType() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> new Metadata.Field(null, null, false, null, null),
                "Empty name, type and source type shouldn't be allowed.");
        assertEquals("Field name, type and source type cannot be empty", e.getMessage());
    }

    @Test
    public void createFieldNullType() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> new Metadata.Field("col1", null, "string"),
                "Empty name, type and source type shouldn't be allowed.");
        assertEquals("Field name, type and source type cannot be empty", e.getMessage());
    }

    @Test
    public void createItemEmptyNameType() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> new Metadata.Item(null, null),
                "Empty item name and path shouldn't be allowed.");
        assertEquals("Item or path name cannot be empty", e.getMessage());
    }
}
