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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.hadoop.codec.CompressionCodecNotSupportedException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.CodecFactory;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * HdfsUtilities class exposes helper methods for PXF classes.
 */
public class HdfsUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsUtilities.class);
    private static final CodecFactory codecFactory = CodecFactory.getInstance();

    /**
     * Checks if requests should be handled in a single thread or not.
     *
     * @param config    the configuration parameters object
     * @param dataDir   hdfs path to the data source
     * @param compCodec the fully qualified name of the compression codec
     * @return if the request can be run in multi-threaded mode.
     */
    public static boolean isThreadSafe(Configuration config, String dataDir, String compCodec) {
        Class<? extends CompressionCodec> codecClass;
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
     * Parses fragment metadata and return matching {@link FileSplit}. If the
     * fragment metadata is null, a {@link FileSplit} with zero start and length
     * is returned.
     *
     * @param context request input data
     * @return FileSplit with fragment metadata
     */
    public static FileSplit parseFileSplit(RequestContext context) {
        HcfsFragmentMetadata metadata = context.getFragmentMetadata();

        long start = metadata == null ? 0 : metadata.getStart();
        long length = metadata == null ? 0 : metadata.getLength();

        LOG.debug("Parsed split: path={} start={} length={}", context.getDataSource(), start, length);

        return new FileSplit(new Path(context.getDataSource()), start, length, (String[]) null);
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
