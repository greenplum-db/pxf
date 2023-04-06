package org.greenplum.pxf.service.bridge;

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
import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.InputStreamHandler;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.serde.RecordReader;
import org.greenplum.pxf.service.serde.RecordReaderFactory;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;

import java.io.DataInputStream;
import java.nio.charset.Charset;
import java.util.List;

/**
 * WriteBridge orchestrates writing data received from GPDB into an external system. It provides methods
 * to start and stop the iteration process as well as the iterative method to read database table tuples
 * from the input stream, transform them with a resolver and store them into the external system using an accessor.
 */
public class WriteBridge extends BaseBridge {

    protected final OutputFormat outputFormat;
    protected final Charset databaseEncoding;
    protected final RecordReader recordReader;

    /**
     * Creates a new instance
     * @param pluginFactory factory for creating plugins
     * @param recordReaderFactory factory for creating a record reader to deserialize incoming data
     * @param context request context
     * @param failureHandler failure handler for GSS errors
     */
    public WriteBridge(BasePluginFactory pluginFactory, RecordReaderFactory recordReaderFactory,
                       RequestContext context, GSSFailureHandler failureHandler) {
        super(pluginFactory, context, failureHandler);
        this.outputFormat = context.getOutputFormat();
        this.databaseEncoding = context.getDatabaseEncoding();

        // create record reader for incoming data deserialization
        this.recordReader = recordReaderFactory.getRecordReader(context,
                resolver.getClass().isAnnotationPresent(InputStreamHandler.class));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean beginIteration() throws Exception {
        // using lambda and not a method reference accessor::openForRead as the accessor will be changed by the retry function
        return failureHandler.execute(context.getConfiguration(), "begin iteration", () -> accessor.openForWrite(), this::beforeRetryCallback);
    }

    /**
     * Reads a record (usually a database table tuple) from the input stream using an InputBuilder,
     * converts it into OneRow object (a representation suitable for the external system) using a Resolver,
     * and stores it into the external system using an Accessor.
     * @param inputStream input stream containing data
     * @return true if data was read and processed, false if there was no more data to read
     * @throws BadRecordException if accessor failed to write the data
     * @throws Exception if any operation failed
     */
    @Override
    public boolean setNext(DataInputStream inputStream) throws Exception {

        List<OneField> record = recordReader.readRecord(inputStream);
        if (record == null) {
            return false;
        }

        OneRow onerow = resolver.setFields(record);
        if (onerow == null) {
            return false;
        }
        if (!accessor.writeNextObject(onerow)) {
            throw new BadRecordException();
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public void endIteration() throws Exception {
        try {
            accessor.closeForWrite();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Writable getNext() {
        throw new UnsupportedOperationException("Current operation is not supported");
    }

}
