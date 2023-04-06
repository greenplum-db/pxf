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


import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.InputStreamHandler;
import org.greenplum.pxf.api.model.Resolver;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import static org.greenplum.pxf.api.io.DataType.BYTEA;
import static org.greenplum.pxf.api.io.DataType.VARCHAR;

/**
 * StringPassResolver handles "deserialization" and serialization of
 * String records. StringPassResolver implements {@link Resolver}
 * interface. Returns strings as-is.
 */
@InputStreamHandler
public class StringPassResolver extends BasePlugin implements Resolver {
    // for write
    private final OneRow oneRow = new OneRow();
    private int resolveWriteCount = 0;

    /**
     * Returns a list of the fields of one record.
     * Each record field is represented by a {@link OneField} item.
     * OneField item contains two fields: an integer representing the field type and a Java
     * Object representing the field value.
     */
    @Override
    public List<OneField> getFields(OneRow onerow) {
        /*
         * This call forces a whole text line into a single varchar field and replaces
         * the proper field separation code can be found in previous revisions. The reasons
         * for doing so as this point are:
         * 1. performance
         * 2. desire to not replicate text parsing logic from the backend into java
         */
        List<OneField> record = new LinkedList<>();
        Object data = onerow.getData();
        if (data instanceof ChunkWritable) {
            record.add(new OneField(BYTEA.getOID(), ((ChunkWritable) data).box));
        } else {
            record.add(new OneField(VARCHAR.getOID(), data));
        }
        return record;
    }

    /**
     * Creates a OneRow object from the singleton list.
     */
    @Override
    public OneRow setFields(List<OneField> record) {
        OneField oneField = record.get(0);

        if ((oneField.val instanceof InputStream && resolveWriteCount > 0) ||
                (oneField.val instanceof byte[] && ((byte[]) oneField.val).length == 0)) {
            // For the inputStream case, we only consume the inputStream once
            // and then we need to return null to indicate, during the next
            // iteration, that the stream has been consumed
            return null;
        }
        oneRow.setData(oneField.val);
        resolveWriteCount++;
        return oneRow;
    }
}
