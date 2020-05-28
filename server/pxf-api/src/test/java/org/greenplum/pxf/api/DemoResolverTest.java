package org.greenplum.pxf.api;

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


import org.greenplum.pxf.api.examples.DemoResolver;
import org.greenplum.pxf.api.examples.DemoTextResolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.greenplum.pxf.api.io.DataType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DemoResolverTest {

    private static final String DATA = "value1,value2";

    private DemoResolver customResolver;
    private DemoTextResolver textResolver;
    private OneRow row;
    private OneField field;

    @BeforeEach
    public void setup() {
        RequestContext context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");

        customResolver = new DemoResolver();
        textResolver = new DemoTextResolver();

        row = new OneRow("0.0", DATA);
        field = new OneField(VARCHAR.getOID(), DATA.getBytes());
    }

    @Test
    public void testGetCustomData() {
        List<OneField> output = customResolver.getFields(row);
        assertEquals("value1", output.get(0).toString());
        assertEquals("value2", output.get(1).toString());
    }

    @Test
    public void testGetTextData() {
        List<OneField> output = textResolver.getFields(row);
        assertEquals(DATA, output.get(0).toString());
    }

    @Test
    public void testSetTextData() throws Exception {
        OneRow output = textResolver.setFields(Collections.singletonList(field));
        assertArrayEquals(DATA.getBytes(), (byte[]) output.getData());
    }

    @Test
    public void testSetEmptyTextData() throws Exception {
        OneField field = new OneField(VARCHAR.getOID(), new byte[]{});
        OneRow output = textResolver.setFields(Collections.singletonList(field));
        assertNull(output);
    }

    @Test
    public void testSetTextDataNullInput() {
        assertThrows(Exception.class,
                () -> textResolver.setFields(null));
    }

    @Test
    public void testSetTextDataEmptyInput() {
        assertThrows(Exception.class,
                () -> textResolver.setFields(Collections.emptyList()));
    }

    @Test
    public void testSetTextDataManyElements() {
        assertThrows(Exception.class,
                () -> textResolver.setFields(Arrays.asList(field, field)));
    }

}
