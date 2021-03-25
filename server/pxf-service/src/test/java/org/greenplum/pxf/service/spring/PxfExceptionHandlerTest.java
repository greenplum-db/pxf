package org.greenplum.pxf.service.spring;

import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
public class PxfExceptionHandlerTest {

    PxfExceptionHandler handler;
    @Mock
    HttpServletResponse mockResponse;

    @BeforeEach
    public void setup() {
        handler = new PxfExceptionHandler();
    }

    @Test
    public void testHandlePxfRuntimeException() throws IOException {
        handler.handlePxfRuntimeException(new PxfRuntimeException("foo"), mockResponse);
        verify(mockResponse).sendError(500);
        verifyNoMoreInteractions(mockResponse);
    }
}
