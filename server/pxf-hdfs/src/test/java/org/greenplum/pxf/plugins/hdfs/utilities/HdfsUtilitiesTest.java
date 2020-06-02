package org.greenplum.pxf.plugins.hdfs.utilities;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.CodecFactory;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HdfsUtilitiesTest {

    private CodecFactory codecFactory;
    private Configuration conf;

    @BeforeEach
    public void setup() {
        conf = new Configuration();
        codecFactory = CodecFactory.getInstance();
    }

    @Test
    public void isThreadSafe() {

        testIsThreadSafe(
                "readable compression, no compression - thread safe",
                "/some/path/without.compression",
                null,
                true);

        testIsThreadSafe(
                "readable compression, gzip compression - thread safe",
                "/some/compressed/path.gz",
                null,
                true);

        testIsThreadSafe(
                "readable compression, bzip2 compression - not thread safe",
                "/some/path/with/bzip2.bz2",
                null,
                false);

        testIsThreadSafe(
                "writable compression, no compression codec - thread safe",
                "/some/path",
                null,
                true);

        testIsThreadSafe(
                "writable compression, compression codec bzip2 - not thread safe",
                "/some/path",
                "org.apache.hadoop.io.compress.BZip2Codec",
                false);
    }

    private void testIsThreadSafe(String testDescription, String path, String codecStr, boolean expectedResult) {
        boolean result = HdfsUtilities.isThreadSafe(conf, path, codecStr);
        assertEquals(expectedResult, result, testDescription);
    }

    @Test
    public void testToString() {
        List<OneField> oneFields = Arrays.asList(new OneField(1, "uno"), new OneField(2, "dos"), new OneField(3, "tres"));

        assertEquals("uno!dos!tres", HdfsUtilities.toString(oneFields, "!"));
        assertEquals("uno", HdfsUtilities.toString(Collections.singletonList(oneFields.get(0)), "!"));
        assertEquals("", HdfsUtilities.toString(Collections.emptyList(), "!"));
    }

    @Test
    public void testParseFileSplit() {
        RequestContext context = new RequestContext();
        context.setDataSource("/abc/path/to/data/source");
        context.setFragmentMetadata(new HcfsFragmentMetadata(10, 100));
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);
        assertEquals(fileSplit.getStart(), 10);
        assertEquals(fileSplit.getLength(), 100);
        assertEquals(fileSplit.getPath().toString(), "/abc/path/to/data/source");
    }
}
