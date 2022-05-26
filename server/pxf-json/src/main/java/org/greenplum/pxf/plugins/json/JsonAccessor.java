package org.greenplum.pxf.plugins.json;

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

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

import java.io.IOException;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * This JSON accessor for PXF will read JSON data and pass it to a {@link JsonResolver}.
 * <p>
 * This accessor supports a single JSON record per line, or a multi-line JSON records if the <b>IDENTIFIER</b> parameter
 * is set.
 * <p>
 * When provided the <b>IDENTIFIER</b> indicates the member name used to determine the encapsulating json object to
 * return.
 */
public class JsonAccessor extends HdfsSplittableDataAccessor {

    public static final String IDENTIFIER_PARAM = "IDENTIFIER";
    public static final String RECORD_MAX_LENGTH_PARAM = "MAXLENGTH";
    private static final String UNSUPPORTED_ERR_MESSAGE = "JSON accessor does not support write operation.";

    /**
     * If provided indicates the member name which will be used to determine the encapsulating json object to return.
     */
    private String identifier = "";

    /**
     * Optional parameter that allows to define the max length of a json record. Records that exceed the allowed length
     * are skipped. This parameter is applied only for the multi-line json records (e.g. when the IDENTIFIER is
     * provided).
     */
    private int maxRecordLength = Integer.MAX_VALUE;

    public JsonAccessor() {
        // Because HdfsSplittableDataAccessor doesn't use the InputFormat we set it to null.
        super(null);
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();

        if (!isEmpty(context.getOption(IDENTIFIER_PARAM))) {

            identifier = context.getOption(IDENTIFIER_PARAM);

            // If the member identifier is set then check if a record max length is defined as well.
            if (!isEmpty(context.getOption(RECORD_MAX_LENGTH_PARAM))) {
                maxRecordLength = Integer.valueOf(context.getOption(RECORD_MAX_LENGTH_PARAM));
            }
        }
    }

    @Override
    protected Object getReader(JobConf conf, InputSplit split) throws IOException {
        if (!isEmpty(identifier)) {
            conf.set(JsonRecordReader.RECORD_MEMBER_IDENTIFIER, identifier);
            conf.setInt(JsonRecordReader.RECORD_MAX_LENGTH, maxRecordLength);
            return new JsonRecordReader(conf, (FileSplit) split);
        } else {
            return new LineRecordReader(conf, (FileSplit) split);
        }
    }

    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     * @throws Exception if opening the resource failed
     */
    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     * @throws Exception writing to the resource failed
     */
    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    /**
     * Closes the resource for write.
     *
     * @throws Exception if closing the resource failed
     */
    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }
}
