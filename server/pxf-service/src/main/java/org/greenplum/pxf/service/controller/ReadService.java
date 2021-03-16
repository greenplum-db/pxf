package org.greenplum.pxf.service.controller;

import org.greenplum.pxf.api.error.PxfIOException;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.OutputStream;

/**
 * Service that reads data from external systems.
 */
public interface ReadService {

    /**
     * Reads data from the external system specified by the RequestContext.
     * The data is then written to the provided OutputStream.
     *
     * @param context      request context
     * @param outputStream output stream to write data to
     * @throws PxfIOException if an error occurs
     */
    void readData(RequestContext context, OutputStream outputStream) throws PxfIOException;
}
