package org.greenplum.pxf.service.rest;

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.RequestParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

/**
 * Base abstract implementation of the resource class, provides logger and request parser
 * to the subclasses.
 */
public abstract class BaseResource {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    protected final RequestContext.RequestType requestType;

    private RequestParser<MultiValueMap<String, String>> parser;

    /**
     * Creates an instance of the resource with a given request parser.
     *
     * @param requestType the type of the request
     * @param parser      request parser
     */
    BaseResource(RequestContext.RequestType requestType, RequestParser<MultiValueMap<String, String>> parser) {
        this.requestType = requestType;
        this.parser = parser;
    }

    /**
     * Parses incoming request into request context
     *
     * @param headers the HTTP headers of incoming request
     * @return parsed request context
     */
    protected RequestContext parseRequest(MultiValueMap<String, String> headers) {
        return parser.parseRequest(headers, requestType);
    }
}
