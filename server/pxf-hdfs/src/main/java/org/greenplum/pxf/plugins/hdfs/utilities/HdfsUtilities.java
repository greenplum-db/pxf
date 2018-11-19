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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.ParquetUserData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * HdfsUtilities class exposes helper methods for PXF classes.
 */
public class HdfsUtilities {
    private static final Log LOG = LogFactory.getLog(HdfsUtilities.class);
    private static final String PROTOCOL_HDFS = "hdfs://";
    private static final String PROTOCOL_S3 = "s3a://";
    private static final String PROTOCOL_AZURE = "adl://";
    private static final String FS_DEFAULT_NAME_KEY = "fs.defaultFS";

    public enum HCFSType {
        HDFS("hdfs"),
        S3("s3"),
        ADLS("adl"),
        FILE("file");

        private String value;

        HCFSType(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    /**
     * Returns a fully resolved path include protocol
     *
     * @param input The input data parameters
     * @return an absolute data path
     */
    public static String getDataUri(Configuration configuration, RequestContext input) {
        // TODO: Move profile specific URI logic into separate classes in the profile
        String dataUri;

        switch (getHCFSType(configuration, input)) {
            case S3:
                dataUri = PROTOCOL_S3 + (StringUtils.startsWith(input.getDataSource(), "/") ?
                        input.getDataSource().substring(1) : input.getDataSource());
                break;
            case ADLS:
                dataUri = PROTOCOL_AZURE + input.getDataSource();
                break;
            case HDFS:
                dataUri = (!StringUtils.endsWith(configuration.get(FS_DEFAULT_NAME_KEY), "/") ?
                        configuration.get(FS_DEFAULT_NAME_KEY) + "/" :
                        configuration.get(FS_DEFAULT_NAME_KEY)) + input.getDataSource();

                break;
            default:
                dataUri = Utilities.absoluteDataPath(input.getDataSource());
                break;
        }
        return dataUri;
    }

    /**
     * Returns the type of filesystem being accesses
     * Profile will override the default filesystem configured
     *
     * @param input The input data parameters
     * @return an absolute data path
     */
    public static HCFSType getHCFSType(Configuration conf, RequestContext input) {
        String profile = input.getProfile();
        String protocol = (profile != null) ? profile.toLowerCase() : conf.get(FS_DEFAULT_NAME_KEY);

        if (StringUtils.startsWith(protocol, HCFSType.S3.value)) {
            return HCFSType.S3;
        } else if (StringUtils.startsWith(protocol, HCFSType.ADLS.value)) {
            return HCFSType.ADLS;
        } else if (StringUtils.startsWith(protocol, HCFSType.HDFS.value)) {
            return HCFSType.HDFS;
        }

        return HCFSType.FILE;
    }

    /*
     * Helper routine to get a compression codec class
     */
    private static Class<? extends CompressionCodec> getCodecClass(Configuration conf,
                                                                   String name) {
        Class<? extends CompressionCodec> codecClass;
        try {
            codecClass = conf.getClassByName(name).asSubclass(
                    CompressionCodec.class);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Compression codec " + name
                    + " was not found.", e);
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
        LOG.debug((codecClass == null ? "No codec" : "Codec " + codecClass)
                + " was found for file " + path);
        return codecClass;
    }

    /**
     * Returns true if the needed codec is splittable. If no codec is needed
     * returns true as well.
     *
     * @param path path of the file to be read
     * @return if the codec needed for reading the specified path is splittable.
     */
    public static boolean isSplittableCodec(CompressionCodecFactory factory, Path path) {

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

            FileSplit fileSplit = new FileSplit(new Path(requestContext.getDataSource()), fragmentMetadata.getStart(), fragmentMetadata.getEnd(), fragmentMetadata.getHosts());

            return fileSplit;

        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception while reading expected fragment metadata", e);
        }
    }

    /**
     * Accessing the Avro file through the "unsplittable" API just to get the
     * schema. The splittable API (AvroInputFormat) which is the one we will be
     * using to fetch the records, does not support getting the Avro schema yet.
     *
     * @param conf       Hadoop configuration
     * @param dataSource Avro file (i.e fileName.avro) path
     * @return the Avro schema
     * @throws IOException if I/O error occurred while accessing Avro schema file
     */
    public static Schema getAvroSchema(Configuration conf, String dataSource)
            throws IOException {
        FsInput inStream = new FsInput(new Path(dataSource), conf);
        DatumReader<GenericRecord> dummyReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dummyFileReader = new DataFileReader<>(
                inStream, dummyReader);
        Schema schema = dummyFileReader.getSchema();
        dummyFileReader.close();
        return schema;
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
                /** Serialize byte array as string */
                buff.append(delim);
                Utilities.byteArrayToOctalString((byte[]) complex.val, buff);
            } else {
                buff.append(delim).append(complex.val);
            }
            delim = delimiter;
        }
        return buff.toString();
    }

    public static byte[] makeParquetUserData(MessageType schema) throws IOException {
        ParquetUserData userData = new ParquetUserData(schema);
        return userData.toString().getBytes();
    }

    public static ParquetUserData parseParquetUserData(RequestContext input) {
        MessageType schema = MessageTypeParser.parseMessageType(new String(input.getFragmentUserData()));
        return new ParquetUserData(schema);
    }

}
