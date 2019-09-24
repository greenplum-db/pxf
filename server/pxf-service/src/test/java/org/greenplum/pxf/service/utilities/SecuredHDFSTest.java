package org.greenplum.pxf.service.utilities;

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


import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.ServletContext;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class SecuredHDFSTest {
    RequestContext mockRequestContext;
    ServletContext mockContext;
    UserGroupInformation mockLoginUser;

    /*
     * setUp function called before each test
     */
    @Before
    public void setUp() {
        mockRequestContext = mock(RequestContext.class);
        mockContext = mock(ServletContext.class);
        mockLoginUser = mock(UserGroupInformation.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void nullToken() throws IOException {
        when(mockRequestContext.getToken()).thenReturn(null);

        SecuredHDFS.verifyToken(mockLoginUser, null, mockContext);
        verify(mockLoginUser).reloginFromKeytab();
        verify(mockLoginUser, never()).addToken(any(Token.class));
    }

    @Test
    public void invalidTokenThrows() {
        when(mockRequestContext.getToken()).thenReturn("This is odd");

        try {
            SecuredHDFS.verifyToken(mockLoginUser, "This is odd", mockContext);
            fail("invalid X-GP-TOKEN should throw");
        } catch (SecurityException e) {
            assertEquals("Failed to verify delegation token java.io.EOFException", e.getMessage());
        }
    }

    @Test
    public void loggedOutUser() throws IOException {
        when(mockRequestContext.getToken()).thenReturn("This is odd");

        try {
            SecuredHDFS.verifyToken(mockLoginUser, "This is odd", mockContext);
            fail("invalid X-GP-TOKEN should throw");
        } catch (SecurityException e) {
            verify(mockLoginUser).reloginFromKeytab();
            assertEquals("Failed to verify delegation token java.io.EOFException", e.getMessage());
        }
    }
}
