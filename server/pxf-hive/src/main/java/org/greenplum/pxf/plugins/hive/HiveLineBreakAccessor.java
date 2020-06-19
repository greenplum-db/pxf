package org.greenplum.pxf.plugins.hive;

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
import org.apache.hadoop.mapred.TextInputFormat;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.io.IOException;

/**
 * Specialization of HiveAccessor for a Hive table stored as Text files.
 * Use together with {@link HiveInputFormatFragmenter}/{@link HiveStringPassResolver}.
 */
@Component("HiveLineBreakAccessor")
@RequestScope
public class HiveLineBreakAccessor extends HiveAccessor {

    /**
     * Constructs a HiveLineBreakAccessor.
     */
    public HiveLineBreakAccessor() {
        super(new TextInputFormat());
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        ((TextInputFormat) inputFormat).configure(jobConf);
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split) throws IOException {
        return new LineRecordReader(jobConf, (FileSplit) split);
    }
}
