package org.greenplum.pxf.service;

import org.greenplum.pxf.api.model.RequestContext;

/**
 * Parser for incoming requests responsible for extracting request parameters.
 *
 * @param <T> type of request
 */
public interface RequestParser<T> {

    /**
     * Parses the request and constructs RequestContext instance
     * @param request request data
     * @return parsed information as an instance of RequestContext
     */
    RequestContext parseRequest(T request);
}
