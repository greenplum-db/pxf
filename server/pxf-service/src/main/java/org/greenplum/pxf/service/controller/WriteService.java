package org.greenplum.pxf.service.controller;

import org.greenplum.pxf.api.model.RequestContext;

import java.io.IOException;
import java.io.InputStream;

/**
 * Service that writes data to external systems.
 */
public interface WriteService {

    /**
     * Writes data to the external system specified by the RequestContext.
     * The data is first read from the provided InputStream.
     *
     * @param context     request context
     * @param inputStream input stream to read data from
     * @return text response to send back to the client
     * @throws IOException if an error occurs
     */
    String writeData(RequestContext context, InputStream inputStream) throws IOException;
}
