package org.greenplum.pxf.service;

import org.greenplum.pxf.service.spring.PxfContextMdcLogEnhancerFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.spi.MDCAdapter;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.servlet.ServletException;
import java.io.IOException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class PxfContextMdcLogEnhancerFilterTest {

    PxfContextMdcLogEnhancerFilter filter;
    MockHttpServletRequest mockRequest;
    MockHttpServletResponse mockResponse;
    MockFilterChain mockFilterChain;

    @Mock
    MDCAdapter mdcMock;

    @BeforeEach
    void setup() {
        mockRequest = new MockHttpServletRequest();
        mockResponse = new MockHttpServletResponse();
        mockFilterChain = new MockFilterChain();
        filter = new PxfContextMdcLogEnhancerFilter(new HttpHeaderDecoder());
    }

    @Test
    void testNonPxfContextRequest() throws ServletException, IOException {
        filter.doFilter(mockRequest, mockResponse, mockFilterChain);
        // always removes
        verify(mdcMock).remove("segmentId");
        verify(mdcMock).remove("sessionId");
        verifyNoMoreInteractions(mdcMock);
    }

    @Test
    void testPxfContextRequest() throws ServletException, IOException {

        mockRequest.addHeader("X-GP-XID", "transaction:id");
        mockRequest.addHeader("X-GP-SEGMENT-ID", "5");
        filter.doFilter(mockRequest, mockResponse, mockFilterChain);

        verify(mdcMock).put("sessionId", "transaction:id:default");
        verify(mdcMock).put("segmentId", "5");

        verify(mdcMock).remove("segmentId");
        verify(mdcMock).remove("sessionId");
        verifyNoMoreInteractions(mdcMock);
    }

    @Test
    void testPxfContextRequestWithServerName() throws ServletException, IOException {

        mockRequest.addHeader("X-GP-XID", "transaction:id");
        mockRequest.addHeader("X-GP-OPTIONS-SERVER", "s3");
        mockRequest.addHeader("X-GP-SEGMENT-ID", "5");
        filter.doFilter(mockRequest, mockResponse, mockFilterChain);

        verify(mdcMock).put("sessionId", "transaction:id:s3");
        verify(mdcMock).put("segmentId", "5");

        verify(mdcMock).remove("segmentId");
        verify(mdcMock).remove("sessionId");
        verifyNoMoreInteractions(mdcMock);
    }

    @Test
    void testPxfContextEncodedRequest() throws ServletException, IOException {

        mockRequest.addHeader("X-GP-ENCODED-HEADER-VALUES", "true");
        mockRequest.addHeader("X-GP-XID", "transaction%3Aid");
        mockRequest.addHeader("X-GP-SEGMENT-ID", "5");
        filter.doFilter(mockRequest, mockResponse, mockFilterChain);

        verify(mdcMock).put("sessionId", "transaction:id:default");
        verify(mdcMock).put("segmentId", "5");

        verify(mdcMock).remove("segmentId");
        verify(mdcMock).remove("sessionId");
        verifyNoMoreInteractions(mdcMock);
    }
}