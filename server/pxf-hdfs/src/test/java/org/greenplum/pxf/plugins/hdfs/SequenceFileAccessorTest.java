package org.greenplum.pxf.plugins.hdfs;

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
import org.apache.hadoop.io.SequenceFile;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SequenceFileAccessorTest {

    private RequestContext context;
    private SequenceFileAccessor accessor;

    /*
     * setup function called before each test.
     *
     * As the focus of the test is compression codec and type behavior, and
     * since the compression methods are private to SequenceFileAccessor, we
     * test their invocation and results by calling the public openForWrite().
     * Since openForWrite does some filesystem operations on HDFS, those had
     * to be mocked (and provided good material for a new Kafka story).
     */
    @BeforeEach
    public void setup() {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();

        context = new RequestContext();
        context.setConfig("dummy");
        context.setServerName("dummy");
        context.setUser("dummy");
        context.setDataSource(path);
        context.setSegmentId(0);
        context.setProfileScheme("localfile");
        context.setConfiguration(new Configuration());
    }

    @Test
    public void compressionNotSpecified() throws Exception {
        prepareTest(null, null);
        assertEquals(SequenceFile.CompressionType.NONE, accessor.getCompressionType());
        assertNull(accessor.getCodec());
    }

    @Test
    public void compressCodec() throws Exception {
        //using BZip2 as a valid example
        prepareTest("org.apache.hadoop.io.compress.BZip2Codec", null);
        assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
    }

    @Test
    public void bogusCompressCodec() {

        final String codecName = "So I asked, who is he? He goes by the name of Wayne Rooney";
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> prepareTest(codecName, null));
        assertEquals("Invalid codec: So I asked, who is he? He goes by the name of Wayne Rooney ", e.getMessage());
    }

    @Test
    public void compressTypes() throws Exception {

        //proper value
        prepareTest("org.apache.hadoop.io.compress.BZip2Codec", "BLOCK");
        assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
        assertEquals(org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK, accessor.getCompressionType());

        //case (non) sensitivity
        prepareTest("org.apache.hadoop.io.compress.BZip2Codec", "ReCoRd");
        assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
        assertEquals(org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, accessor.getCompressionType());

        //default (RECORD)
        prepareTest("org.apache.hadoop.io.compress.BZip2Codec", null);
        assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
        assertEquals(org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, accessor.getCompressionType());
    }

    @Test
    public void illegalCompressTypeOy() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> prepareTest("org.apache.hadoop.io.compress.BZip2Codec", "Oy"));
        assertEquals("Illegal compression type 'Oy'", e.getMessage());
    }

    @Test
    public void illegalCompressTypeNone() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> prepareTest("org.apache.hadoop.io.compress.BZip2Codec", "NONE"));
        assertEquals("Illegal compression type 'NONE'. For disabling compression remove COMPRESSION_CODEC parameter.", e.getMessage());
    }

    /*
     * After each test is done, close the accessor if it was created
     */
    @AfterEach
    public void tearDown() throws Exception {
        if (accessor == null) return;

        accessor.closeForWrite();
        accessor = null;
    }

    private void prepareTest(String codec, String type) throws Exception {
        if (codec != null) {
            context.addOption("COMPRESSION_CODEC", codec);
        }
        if (type != null) {
            context.addOption("COMPRESSION_TYPE", type);
        }

        accessor = new SequenceFileAccessor();
        accessor.setRequestContext(context);
        accessor.afterPropertiesSet();
        accessor.openForWrite();
    }
}
