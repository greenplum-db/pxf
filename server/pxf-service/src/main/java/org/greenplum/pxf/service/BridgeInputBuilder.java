package org.greenplum.pxf.service;

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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.GPDBWritableMapper;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.io.GPDBWritable;
import org.greenplum.pxf.api.io.Text;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.DataInput;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class BridgeInputBuilder {
    private RequestContext protocolData;
    private static final Log LOG = LogFactory.getLog(BridgeInputBuilder.class);

    public BridgeInputBuilder(RequestContext protocolData) {
        this.protocolData = protocolData;
    }

    public List<OneField> makeInput(DataInput inputStream) throws Exception {
        if (protocolData.getOutputFormat() == OutputFormat.TEXT) {
            Text txt = new Text();
            txt.readFields(inputStream);
            return Collections.singletonList(new OneField(DataType.BYTEA.getOID(), txt.getBytes()));
        }

        GPDBWritable gpdbWritable = new GPDBWritable();
        gpdbWritable.readFields(inputStream);

        if (gpdbWritable.isEmpty()) {
            LOG.debug("Reached end of stream");
            return null;
        }

        GPDBWritableMapper mapper = new GPDBWritableMapper(gpdbWritable);
        int[] colTypes = gpdbWritable.getColType();
        List<OneField> record = new LinkedList<>();
        for (int i = 0; i < colTypes.length; i++) {
            mapper.setDataType(colTypes[i]);
            record.add(new OneField(colTypes[i], mapper.getData(i)));
        }

        return record;
    }
}
