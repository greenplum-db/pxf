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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * HdfsUtilities class exposes helper methods for PXF classes.
 */
public class HdfsUtilities {

    private static Logger LOG = LoggerFactory.getLogger(HdfsUtilities.class);

    /*
     * Helper routine to get a compression codec class
     */
    private static Class<? extends CompressionCodec> getCodecClass(Configuration conf, String name) {
        Class<? extends CompressionCodec> codecClass;
        try {
            codecClass = conf.getClassByName(name).asSubclass(CompressionCodec.class);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Compression codec " + name + " was not found.", e);
        }
        return codecClass;
    }

    /**
     * Helper routine to get compression codec through reflection.
     *
     * @param conf configuration used for reflection
     * @param name codec name
     * @return generated CompressionCodec
     */
    public static CompressionCodec getCodec(Configuration conf, String name) {
        return ReflectionUtils.newInstance(getCodecClass(conf, name), conf);
    }

    /**
     * Helper routine to get compression codec class by path (file suffix).
     *
     * @param path path of file to get codec for
     * @return matching codec class for the path. null if no codec is needed.
     */
    private static Class<? extends CompressionCodec> getCodecClassByPath(Configuration config, String path) {

        Class<? extends CompressionCodec> codecClass = null;
        CompressionCodecFactory factory = new CompressionCodecFactory(config);
        CompressionCodec codec = factory.getCodec(new Path(path));
        if (codec != null) {
            codecClass = codec.getClass();
        }
        String msg = (codecClass == null ? "No codec" : "Codec " + codecClass);
        LOG.debug("{} was found for file {}", msg, path);
        return codecClass;
    }

    /**
     * Returns true if the needed codec is splittable. If no codec is needed
     * returns true as well.
     *
     * @param path path of the file to be read
     * @return if the codec needed for reading the specified path is splittable.
     */
    static boolean isSplittableCodec(CompressionCodecFactory factory, Path path) {

        final CompressionCodec codec = factory.getCodec(path);
        if (null == codec) {
            return true;
        }

        return codec instanceof SplittableCompressionCodec;
    }

    /**
     * Checks if requests should be handle in a single thread or not.
     *
     * @param config    the configuration parameters object
     * @param dataDir   hdfs path to the data source
     * @param compCodec the fully qualified name of the compression codec
     * @return if the request can be run in multi-threaded mode.
     */
    public static boolean isThreadSafe(Configuration config, String dataDir, String compCodec) {

        Class<? extends CompressionCodec> codecClass = (compCodec != null) ?
                HdfsUtilities.getCodecClass(config, compCodec) :
                HdfsUtilities.getCodecClassByPath(config, dataDir);
        /* bzip2 codec is not thread safe */
        return (codecClass == null || !BZip2Codec.class.isAssignableFrom(codecClass));
    }

    /**
     * Prepares byte serialization of a file split information (start, length,
     * hosts) using {@link ObjectOutputStream}.
     *
     * @param fsp file split to be serialized
     * @return byte serialization of fsp
     * @throws IOException if I/O errors occur while writing to the underlying
     *                     stream
     */
    public static byte[] prepareFragmentMetadata(FileSplit fsp)
            throws IOException {

        return prepareFragmentMetadata(fsp.getStart(), fsp.getLength(), fsp.getLocations());
    }

    public static byte[] prepareFragmentMetadata(long start, long length, String[] locations)
            throws IOException {

        ByteArrayOutputStream byteArrayStream = writeBaseFragmentInfo(start, length, locations);
        return byteArrayStream.toByteArray();
    }


    private static ByteArrayOutputStream writeBaseFragmentInfo(long start, long length, String[] locations) throws IOException {
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
        objectStream.writeLong(start);
        objectStream.writeLong(length);
        objectStream.writeObject(locations);
        return byteArrayStream;
    }

    /**
     * Parses fragment metadata and return matching {@link FileSplit}.
     *
     * @param requestContext request input data
     * @return FileSplit with fragment metadata
     */
    public static FileSplit parseFileSplit(RequestContext requestContext) {
        try {
            FragmentMetadata fragmentMetadata = Utilities.parseFragmentMetadata(requestContext);
            return new FileSplit(new Path(requestContext.getDataSource()),
                    fragmentMetadata.getStart(), fragmentMetadata.getEnd(), fragmentMetadata.getHosts());
        }
        catch (Exception e) {
            throw new RuntimeException("Exception while reading expected fragment metadata", e);
        }
    }

    /**
     * Returns string serialization of list of fields. Fields of binary type
     * (BYTEA) are converted to octal representation to make sure they will be
     * relayed properly to the DB.
     *
     * @param complexRecord list of fields to be stringified
     * @param delimiter     delimiter between fields
     * @return string of serialized fields using delimiter
     */
    public static String toString(List<OneField> complexRecord, String delimiter) {
        StringBuilder buff = new StringBuilder();
        String delim = ""; // first iteration has no delimiter
        if (complexRecord == null)
            return "";
        for (OneField complex : complexRecord) {
            if (complex.type == DataType.BYTEA.getOID()) {
                // Serialize byte array as string
                buff.append(delim);
                Utilities.byteArrayToOctalString((byte[]) complex.val, buff);
            } else {
                buff.append(delim).append(complex.val);
            }
            delim = delimiter;
        }
        return buff.toString();
    }

}
