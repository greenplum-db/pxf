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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * PxfInputFormat is not intended to read a specific format, hence it implements
 * a dummy getRecordReader Instead, its purpose is to apply
 * FileInputFormat.getSplits from one point in PXF and get the splits which are
 * valid for the actual InputFormats, since all of them we use inherit
 * FileInputFormat but do not override getSplits.
 */
public class PxfInputFormat extends FileInputFormat {

    @Override
    public RecordReader getRecordReader(InputSplit split,
                                        JobConf conf,
                                        Reporter reporter) {
        throw new UnsupportedOperationException("PxfInputFormat should not be used for reading data, but only for obtaining the splits of a file");
    }

    @Override
    public FileStatus[] listStatus(JobConf job) throws IOException {
        return super.listStatus(job);
    }

    /**
     * Returns true if the needed codec is splittable. If no codec is needed
     * returns true as well.
     *
     * @param fs       the filesystem
     * @param filename the name of the file to be read
     * @return if the codec needed for reading the specified path is splittable.
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
        CompressionCodec codec = factory.getCodec(filename);

        return null == codec || codec instanceof SplittableCompressionCodec;
    }
}
