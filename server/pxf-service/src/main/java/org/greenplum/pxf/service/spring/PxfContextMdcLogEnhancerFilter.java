package org.greenplum.pxf.service.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.service.HttpHeaderDecoder;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * A servlet filter that enhances MDC with request headers that provide
 * PXF session/segment IDs (if available).
 */
@Slf4j
@Component
public class PxfContextMdcLogEnhancerFilter extends OncePerRequestFilter {

    private static final String MDC_SESSION_ID = "sessionId";
    private static final String MDC_SEGMENT_ID = "segmentId";

    private final HttpHeaderDecoder decoder;

    public PxfContextMdcLogEnhancerFilter(HttpHeaderDecoder decoder) {
        this.decoder = decoder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        insertIntoMDC(request);
        try {
            chain.doFilter(request, response);
        } finally {
            clearMDC();
        }
    }

    /**
     * Adds entries to MDC from the request headers
     *
     * @param request the servlet request
     */
    private void insertIntoMDC(HttpServletRequest request) {
        boolean encoded = decoder.areHeadersEncoded(request);
        String xid = decoder.getHeaderValue("X-GP-XID", request, encoded);
        if (StringUtils.isBlank(xid)) {
            return; // Not a PXF extension request
        }

        // xid : server
        String serverName = StringUtils.defaultIfBlank(decoder.getHeaderValue("X-GP-OPTIONS-SERVER", request, encoded), "default");
        String sessionId = String.format("%s:%s", xid, serverName);
        String segmentId = decoder.getHeaderValue("X-GP-SEGMENT-ID", request, encoded);
        MDC.put(MDC_SESSION_ID, sessionId);
        MDC.put(MDC_SEGMENT_ID, segmentId);
        log.debug("MDC: Added {}={}", MDC_SESSION_ID, sessionId);
        log.debug("MDC: Added {}={}", MDC_SEGMENT_ID, segmentId);
    }

    /**
     * Removes entries added to MDC
     */
    private void clearMDC() {
        // removing possibly non-existent item is OK
        MDC.remove(MDC_SEGMENT_ID);
        MDC.remove(MDC_SESSION_ID);
    }
}
