package org.greenplum.pxf.api.examples;

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
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.BasePlugin;

/**
 * Internal interface that would defined the access to a file on HDFS, but in
 * this case contains the data required.
 *
 * Demo implementation
 */
public class DemoAccessor extends BasePlugin implements Accessor {

    private static final Log LOG = LogFactory.getLog(DemoAccessor.class);
    private int rowNumber;
    private int fragmentNumber;
    private static int NUM_ROWS = 2;

    /**
     * Constructs a DemoAccessor
     *
     * @param metaData the RequestContext
     */
    public DemoAccessor(RequestContext metaData) {
        initialize(metaData);
    }

    @Override
    public boolean openForRead() throws Exception {
        /* no-op, because this plugin doesn't read a file. */
        return true;
    }

    /**
     * Read the next record
     * The record contains as many fields as defined by the DDL schema.
     *
     * @return one row which corresponds to one record
     */
    @Override
    public OneRow readNextObject() throws Exception {
        /* return next row , <key=fragmentNo.rowNo, val=rowNo,text,fragmentNo>*/
        /* check for EOF */
        if (fragmentNumber > 0)
            return null; /* signal EOF, close will be called */
        int fragment = context.getDataFragment();
        String fragmentMetadata = new String(context.getFragmentMetadata());
        int colCount = context.getColumns();

        /* generate row with (colCount) columns */
        StringBuilder colValue = new StringBuilder(fragmentMetadata + " row" + (rowNumber+1));
        for(int colIndex=1; colIndex<colCount; colIndex++) {
            colValue.append(",").append("value" + colIndex);
        }
        OneRow row = new OneRow(fragment + "." + rowNumber, colValue.toString());

        /* advance */
        rowNumber += 1;
        if (rowNumber == NUM_ROWS) {
            rowNumber = 0;
            fragmentNumber += 1;
        }

        /* return data */
        return row;
    }

    /**
     * close the reader. no action here
     *
     */
    @Override
    public void closeForRead() throws Exception {
        /* Demo close doesn't do anything */
    }

    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     * @throws Exception if opening the resource failed
     */
    @Override
    public boolean openForWrite() throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     * @throws Exception writing to the resource failed
     */
    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes the resource for write.
     *
     * @throws Exception if closing the resource failed
     */
    @Override
    public void closeForWrite() throws Exception {
        throw new UnsupportedOperationException();
    }
}
