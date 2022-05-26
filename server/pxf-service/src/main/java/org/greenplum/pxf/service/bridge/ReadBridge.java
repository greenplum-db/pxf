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

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.BadRecordException;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.BridgeOutputBuilder;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;

import java.io.CharConversionException;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.charset.CharacterCodingException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.zip.ZipException;

/**
 * ReadBridge class creates appropriate accessor and resolver. It will then
 * create the correct output conversion class (e.g. Text or GPDBWritable) and
 * get records from accessor, let resolver deserialize them and serialize them
 * again using the output conversion class. <br>
 * The class handles BadRecordException and other exception type and marks the
 * record as invalid for GPDB.
 */
public class ReadBridge extends BaseBridge {

    protected BridgeOutputBuilder outputBuilder;
    protected Deque<Writable> outputQueue = new LinkedList<>();

    public ReadBridge(BasePluginFactory pluginFactory, RequestContext context, GSSFailureHandler failureHandler) {
        super(pluginFactory, context, failureHandler);
        this.outputBuilder = new BridgeOutputBuilder(context);
    }

    /**
     * Accesses the underlying data source.
     */
    @Override
    public boolean beginIteration() throws Exception {
        // using lambda and not a method reference accessor::openForRead as the accessor will be changed by the retry function
        return failureHandler.execute(context.getConfiguration(), "begin iteration", () -> accessor.openForRead(), this::beforeRetryCallback);
    }

    protected Deque<Writable> makeOutput(OneRow oneRow) throws Exception {
        return outputBuilder.makeOutput(resolver.getFields(oneRow));
    }

    /**
     * Fetches next object from the data source and turns it into a record that the GPDB
     * backend can process.
     */
    @Override
    public Writable getNext() throws Exception {
        Writable output = null;
        OneRow onerow = null;

        if (!outputQueue.isEmpty()) {
            return outputQueue.pop();
        }

        try {
            while (outputQueue.isEmpty()) {
                onerow = accessor.readNextObject();
                if (onerow == null) {
                    output = outputBuilder.getPartialLine();
                    if (output != null) {
                        LOG.warn("A partial record in the end of the fragment");
                    }
                    // if there is a partial line, return it now, otherwise it
                    // will return null
                    return output;
                }

                // we checked before that outputQueue is empty, so we can override it.
                outputQueue = makeOutput(onerow);
                if (!outputQueue.isEmpty()) {
                    output = outputQueue.pop();
                    break;
                }
            }
        } catch (IOException ex) {
            if (!isDataException(ex)) {
                throw ex;
            }
            output = outputBuilder.getErrorOutput(ex);
        } catch (BadRecordException ex) {
            String rowInfo = (onerow != null) ? onerow.toString() : "[record was not created]";
            LOG.debug("BadRecordException {}: {}", (ex.getCause() != null) ? ex.getCause() : ex, rowInfo);
            output = outputBuilder.getErrorOutput(ex);
        }

        return output;
    }

    /**
     * Close the underlying resource
     */
    public void endIteration() throws Exception {
        try {
            accessor.closeForRead();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: {}", e.getMessage());
            throw e;
        }
    }

    /*
     * There are many exceptions that inherit IOException. Some of them like
     * EOFException are generated due to a data problem, and not because of an
     * IO/connection problem as the father IOException might lead us to believe.
     * For example, an EOFException will be thrown while fetching a record from
     * a sequence file, if there is a formatting problem in the record. Fetching
     * record from the sequence-file is the responsibility of the accessor so
     * the exception will be thrown from the accessor. We identify this cases by
     * analyzing the exception type, and when we discover that the actual
     * problem was a data problem, we return the errorOutput GPDBWritable.
     */
    protected boolean isDataException(IOException ex) {
        return (ex instanceof EOFException
                || ex instanceof CharacterCodingException
                || ex instanceof CharConversionException
                || ex instanceof UTFDataFormatException || ex instanceof ZipException);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean setNext(DataInputStream inputStream) {
        throw new UnsupportedOperationException("Write operation is not supported.");
    }

}
