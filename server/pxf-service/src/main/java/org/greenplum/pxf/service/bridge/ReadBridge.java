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

import org.greenplum.pxf.api.BadRecordException;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.greenplum.pxf.service.BridgeOutputBuilder;
import org.greenplum.pxf.service.io.Writable;

import java.io.CharConversionException;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.charset.CharacterCodingException;
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

    final BridgeOutputBuilder outputBuilder;
    protected LinkedList<Writable> outputQueue = new LinkedList<>();

    /**
     * C'tor - set the implementation of the bridge.
     *
     * @param context input containing accessor and resolver names
     * @throws Exception if accessor or resolver can't be instantiated
     */
    public ReadBridge(RequestContext context) throws Exception {
        this(context, AccessorFactory.getInstance(), ResolverFactory.getInstance());
    }

    ReadBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory) throws Exception {
        super(context, accessorFactory, resolverFactory);
        outputBuilder = new BridgeOutputBuilder(context);
    }

    /**
     * Accesses the underlying data source.
     */
    @Override
    public boolean beginIteration() throws Exception {
        return accessor.openForRead();
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
                    // if there is a partial line, return it now, otherwise it will return null
                    return output;
                }

                // we checked before that outputQueue is empty, so we can override it.
                outputQueue = outputBuilder.makeOutput(resolver.getFields(onerow));
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
            String row_info = "null";
            if (onerow != null) {
                row_info = onerow.toString();
            }
            if (ex.getCause() != null) {
                LOG.debug("BadRecordException " + ex.getCause().toString()
                        + ": " + row_info);
            } else {
                LOG.debug(ex.toString() + ": " + row_info);
            }
            output = outputBuilder.getErrorOutput(ex);
        } catch (Exception ex) {
            throw ex;
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
            LOG.error("Failed to close bridge resources: %s", e.getMessage());
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

    @Override
    public boolean setNext(DataInputStream inputStream) {
        throw new UnsupportedOperationException("setNext is not implemented");
    }

}
