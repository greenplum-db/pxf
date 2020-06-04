package org.greenplum.pxf.plugins.hbase;

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

import org.greenplum.pxf.api.BadRecordException;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HBaseResolverTest {
    private RequestContext context;

    /*
     * Test construction of HBaseResolver.
     *
     * HBaseResolver is created and then HBaseTupleDescription
     * creation is verified
     */
    @Test
    public void construction() {
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setFragmentMetadata(new HBaseFragmentMetadata(new byte[0], new byte[0], new HashMap<>()));

        HBaseResolver resolver = new HBaseResolver();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
    }

    /*
     * Test the convertToJavaObject method
     */
    @Test
    public void testConvertToJavaObject() throws Exception {
        Object result;

        context = new RequestContext();
        context.setConfig("default");
        context.setUser("test-user");
        context.setFragmentMetadata(new HBaseFragmentMetadata(new byte[0], new byte[0], new HashMap<>()));

        HBaseResolver resolver = new HBaseResolver();
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();

        /*
         * Supported type, No value.
         * Should successfully return Null.
         */
        result = resolver.convertToJavaObject(20, "bigint", null);
        assertNull(result);

        /*
         * Supported type, With value
         * Should successfully return a Java Object that holds original value
         */
        result = resolver.convertToJavaObject(20, "bigint", "1234".getBytes());
        assertEquals(((Long) result).longValue(), 1234L);

        /*
         * Supported type, Invalid value
         * Should throw a BadRecordException, with detailed explanation.
         */
        Exception e = assertThrows(BadRecordException.class,
                () -> resolver.convertToJavaObject(20, "bigint", "not_a_numeral".getBytes()),
                "Supported type, Invalid value should throw an exception");
        assertEquals("Error converting value 'not_a_numeral' to type bigint. (original error: For input string: \"not_a_numeral\")", e.getMessage());

        /*
         * Unsupported type
         * Should throw an Exception, indicating the name of the unsupported type
         */
        e = assertThrows(Exception.class,
                () -> resolver.convertToJavaObject(600, "point", "[1,1]".getBytes()),
                "Unsupported data type should throw exception");
        assertEquals("Unsupported data type point", e.getMessage());
    }
}
