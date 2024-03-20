package org.greenplum.pxf.service.controller;

import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.service.utilities.ThrowingSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PxfErrorReporterTest {
    class TestReporter extends PxfErrorReporter<String> {}
    private TestReporter reporter = new TestReporter();

    @Mock
    private ThrowingSupplier<String, Exception> mockAction;

    @Test
    public void testNoException() throws Exception {
        when(mockAction.get()).thenReturn("ok");
        String result = reporter.invokeWithErrorHandling(mockAction);
        assertEquals("ok", result);
    }

    @Test
    public void testIOException_NotClientAbort() throws Exception {
        assertWrapped(new IOException());
    }

    @Test
    public void testIOException_ClientAbort() throws Exception {
        assertWrapped(new ClientAbortException());
    }

    @Test
    public void testIOException_ClientAbort_Chained() throws Exception {
        assertWrapped(new IOException(new ClientAbortException()));
    }

    @Test
    public void testOtherException() throws Exception {
        assertWrapped(new Exception());
    }

    @Test
    public void testError() throws Exception {
        assertNotWrapped(new Error());
    }

    @Test
    public void testPxfRuntimeException() throws Exception {
        assertNotWrapped(new PxfRuntimeException());
    }

    private void assertWrapped(Throwable e) throws Exception {
        when(mockAction.get()).thenThrow(e);
        Exception thrownException = assertThrows(PxfRuntimeException.class, () -> reporter.invokeWithErrorHandling(mockAction));
        assertSame(e, thrownException.getCause());
    }

    private void assertNotWrapped(Throwable e) throws Exception {
        when(mockAction.get()).thenThrow(e);
        Throwable thrown = assertThrows(e.getClass(), () -> reporter.invokeWithErrorHandling(mockAction));
        assertSame(e, thrown);
    }
}
