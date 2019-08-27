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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.codec.CompressionCodecNotSupportedException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.CodecFactory;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
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
    private static CodecFactory codecFactory = CodecFactory.getInstance();

    /**
     * Checks if requests should be handled in a single thread or not.
     *
     * @param config    the configuration parameters object
     * @param dataDir   hdfs path to the data source
     * @param compCodec the fully qualified name of the compression codec
     * @return if the request can be run in multi-threaded mode.
     */
    public static boolean isThreadSafe(Configuration config, String dataDir, String compCodec) {
        Class<? extends CompressionCodec> codecClass = null;
        if (compCodec != null) {
            CompressionCodecName compressionCodecName = null;
            try {
                //noinspection ConstantConditions
                compressionCodecName = codecFactory.getCodec(compCodec, compressionCodecName);
                return !BZip2Codec.class.isAssignableFrom(compressionCodecName.getHadoopCompressionCodecClass());
            } catch (IllegalArgumentException | CompressionCodecNotSupportedException e) {
                codecClass = codecFactory.getCodecClass(compCodec, config);
            }
        } else codecClass = codecFactory.getCodecClassByPath(config, dataDir);
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
        FragmentMetadata metadata = Utilities.parseFragmentMetadata(requestContext);
        return new FileSplit(new Path(requestContext.getDataSource()),
                metadata.getStart(), metadata.getEnd(), metadata.getHosts());
    }

    /**
     * Validates that the destination file does not exist and creates parent directory, if missing.
     *
     * @param file File handle
     * @param fs   Filesystem object
     * @throws IOException if I/O errors occur during validation
     */
    public static void validateFile(Path file, FileSystem fs)
            throws IOException {

        if (fs.exists(file)) {
            throw new IOException("File " + file.toString() + " already exists, can't write data");
        }
        Path parent = file.getParent();
        if (!fs.exists(parent)) {
            if (!fs.mkdirs(parent)) {
                throw new IOException("Creation of dir '" + parent.toString() + "' failed");
            }
            LOG.debug("Created new dir {}", parent);
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
