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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SequenceFileAccessor.class, HdfsSplittableDataAccessor.class, HdfsUtilities.class})
@SuppressStaticInitializationFor({"org.apache.hadoop.mapred.JobConf", "org.apache.hadoop.fs.FileContext"})
// Prevents static inits
public class SequenceFileAccessorTest {

    private RequestContext requestContext;
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
    @Before
    public void setup() throws Exception {

        Configuration hdfsConfiguration = mock(Configuration.class);
        Path file = mock(Path.class);
        FileSystem fs = mock(FileSystem.class);
        FileContext fc = mock(FileContext.class);
        requestContext = mock(RequestContext.class);

        PowerMockito.mockStatic(FileContext.class);
        PowerMockito.mockStatic(HdfsUtilities.class);
        PowerMockito.mockStatic(FileSystem.class);
        PowerMockito.whenNew(Path.class).withArguments(Mockito.anyString()).thenReturn(file);

        when(FileSystem.get(any(URI.class), any(Configuration.class))).thenReturn(fs);

//        when(file.getFileSystem(any(Configuration.class))).thenReturn(fs);
        when(requestContext.getDataSource()).thenReturn("deep.throat");
        when(requestContext.getSegmentId()).thenReturn(0);
        when(FileContext.getFileContext(hdfsConfiguration)).thenReturn(fc);
    }

    /*
     * After each test is done, close the accessor if it was created
     */
    @After
    public void tearDown() throws Exception {

        if (accessor == null) {
            return;
        }

        accessor.closeForWrite();
        accessor = null;
    }

    private void constructAccessor() throws Exception {

        accessor = new SequenceFileAccessor();
        accessor.initialize(requestContext);
        accessor.openForWrite();
    }

    private void mockCompressionOptions(String codec, String type) {
        when(requestContext.getOption("COMPRESSION_CODEC")).thenReturn(codec);
        when(requestContext.getOption("COMPRESSION_TYPE")).thenReturn(type);
    }

    @Test
    public void compressionNotSpecified() throws Exception {

        mockCompressionOptions(null, null);
        constructAccessor();
        assertEquals(SequenceFile.CompressionType.NONE, accessor.getCompressionType());
        assertNull(accessor.getCodec());
    }

    @Test
    public void compressCodec() throws Exception {

        //using BZip2 as a valid example
        when(HdfsUtilities.getCodec(Mockito.anyObject(), Mockito.anyString())).thenReturn(new BZip2Codec());
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", null);
        constructAccessor();
        assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
    }

    @Test
    public void bogusCompressCodec() {

        final String codecName = "So I asked, who is he? He goes by the name of Wayne Rooney";
        when(HdfsUtilities.getCodec(Mockito.anyObject(), Mockito.anyString())).thenThrow(new IllegalArgumentException("Compression codec " + codecName + " was not found."));
        mockCompressionOptions(codecName, null);

        try {
            constructAccessor();
            fail("should throw no codec found exception");
        } catch (Exception e) {
            assertEquals("Compression codec " + codecName + " was not found.", e.getMessage());
        }
    }

    @Test
    public void compressTypes() throws Exception {

        when(HdfsUtilities.getCodec(Mockito.anyObject(), Mockito.anyString())).thenReturn(new BZip2Codec());

        //proper value
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", "BLOCK");
        constructAccessor();
        assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
        assertEquals(org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK, accessor.getCompressionType());

        //case (non) sensitivity
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", "ReCoRd");
        constructAccessor();
        assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
        assertEquals(org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, accessor.getCompressionType());

        //default (RECORD)
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", null);
        constructAccessor();
        assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
        assertEquals(org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, accessor.getCompressionType());
    }

    @Test
    public void illegalCompressTypes() throws Exception {

        when(HdfsUtilities.getCodec(Mockito.anyObject(), Mockito.anyString())).thenReturn(new BZip2Codec());
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", "Oy");

        try {
            constructAccessor();
            fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Illegal compression type 'Oy'", e.getMessage());
        }

        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", "NONE");

        try {
            constructAccessor();
            fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Illegal compression type 'NONE'. For disabling compression remove COMPRESSION_CODEC parameter.", e.getMessage());
        }

    }
}
