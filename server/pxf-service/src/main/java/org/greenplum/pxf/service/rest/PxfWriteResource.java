package org.greenplum.pxf.service.rest;

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.controller.WriteService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * PXF REST endpoint for write data requests.
 */
@RestController
@RequestMapping("/pxf")
public class PxfWriteResource extends PxfBaseResource<String> {

    private final WriteService writeService;

    /**
     * Creates a new instance of the resource with Request parser and write service implementation.
     *
     * @param parser       http request parser
     * @param writeService write service implementation
     */
    public PxfWriteResource(RequestParser<MultiValueMap<String, String>> parser,
                            WriteService writeService) {
        super(RequestContext.RequestType.WRITE_BRIDGE, parser);
        this.writeService = writeService;
    }

    /**
     * REST endpoint for write data requests.
     *
     * @param headers http headers from request that carry all parameters
     * @param request the HttpServletRequest
     * @return ok response if the operation finished successfully
     */
    @PostMapping(value = "/write", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<String> stream(@RequestHeader MultiValueMap<String, String> headers,
                                         HttpServletRequest request) {
        return processRequest(headers, request);
    }

    @Override
    protected String produceResponse(RequestContext context, HttpServletRequest request) throws Exception {
        return writeService.writeData(context, request.getInputStream());
    }
}
