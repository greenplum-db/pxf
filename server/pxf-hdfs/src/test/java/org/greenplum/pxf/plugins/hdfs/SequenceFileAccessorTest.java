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
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SequenceFileAccessorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private RequestContext context;
    private SequenceFileAccessor accessor;
    private ConfigurationFactory mockConfigurationFactory;

    /*
     * setup function called before each test.
     *
     * As the focus of the test is compression codec and type behavior, and
     * since the compression methods are private to SequenceFileAccessor, we
     * test their invocation and results by calling the public openForWrite().
     * Since openForWrite does some filesystem operations on HDFS, those had
     * to be mocked (and provided good material for a new Kafka story).
     */
    @Before
    public void setup() {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();
        Configuration configuration = new Configuration();

        mockConfigurationFactory = mock(ConfigurationFactory.class);
        when(mockConfigurationFactory.initConfiguration("dummy", "dummy", null))
                .thenReturn(configuration);

        context = new RequestContext();
        context.setServerName("dummy");
        context.setUser("dummy");
        context.setDataSource(path);
        context.setSegmentId(0);
        context.setProtocol("localfile");
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
    public void bogusCompressCodec() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Invalid codec: So I asked, who is he? He goes by the name of Wayne Rooney ");

        final String codecName = "So I asked, who is he? He goes by the name of Wayne Rooney";
        prepareTest(codecName, null);
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
    public void illegalCompressTypeOy() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal compression type 'Oy'");
        prepareTest("org.apache.hadoop.io.compress.BZip2Codec", "Oy");
    }

    @Test
    public void illegalCompressTypeNone() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal compression type 'NONE'. For disabling compression remove COMPRESSION_CODEC parameter.");
        prepareTest("org.apache.hadoop.io.compress.BZip2Codec", "NONE");
    }

    /*
     * After each test is done, close the accessor if it was created
     */
    @After
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

        accessor = new SequenceFileAccessor(mockConfigurationFactory);
        accessor.initialize(context);
        accessor.openForWrite();
    }
}
