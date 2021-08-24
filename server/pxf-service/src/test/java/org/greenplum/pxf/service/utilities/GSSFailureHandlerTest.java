package org.greenplum.pxf.service.utilities;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GSSFailureHandlerTest {

    private GSSFailureHandler handler;
    private Object result;
    private Configuration configuration;
    @Mock
    private Callable<Object> mockCallable;
    @Mock
    private Runnable mockRunnable;

    public Exception expectedException;

    @BeforeEach
    public void setup () {
        handler = new GSSFailureHandler();
        result = new Object();
        configuration = new Configuration(); // using real configuration instead of mock
    }

//    @Test
//    public void testGetInstance() {
//        assertSame(handler, GSSFailureHandler.getInstance());
//    }

    // ---------- NON-SECURE TESTS ----------
    @Test
    public void testNonSecureSuccess() throws Exception {
        expectNonSecure();
        expectOperationSuccess();
        Object operationResult = execute();
        assertSame(result, operationResult);
    }

    @Test
    public void testNonSecureExceptionFailure() throws Exception {
        expectNonSecure();
        expectOperationExceptionReported();
    }

    @Test
    public void testNonSecureIOExceptionFailure() throws Exception {
        expectNonSecure();
        expectOperationIOExceptionReported();
    }

    @Test
    public void testNonSecureGSSExceptionFailure() throws Exception {
        expectNonSecure();
        expectOperationGSSExceptionReported();
    }

    // ---------- SECURE TESTS - NO RETRIES ----------
    @Test
    public void testSecureSuccess() throws Exception {
        expectSecure();
        expectOperationSuccess();
        Object operationResult = execute();
        assertSame(result, operationResult);
    }

    @Test
    public void testSecureExceptionFailure() throws Exception {
        expectSecure();
        expectOperationExceptionReported();
    }

    @Test
    public void testSecureIOExceptionFailure() throws Exception {
        expectSecure();
        expectOperationIOExceptionReported();
    }

    // ---------- SECURE TESTS - WITH RETRIES ----------
    @Test
    public void testSecureGSSExceptionFailure() throws Exception {
        expectSecure();
        expectOperationGSSExceptionReported(6); // with 5 retries the handler will call the callable 6 times
    }

    @Test
    public void testSecureGSSExceptionFailureCustomRetries() throws Exception {
        expectSecure();
        configuration.set("pxf.sasl.connection.retries", "2");
        expectOperationGSSExceptionReported(3); // with 2 retries the handler will call the callable 3 times
    }

    @Test
    public void testSecureGSSExceptionFailureZeroCustomRetries() throws Exception {
        expectSecure();
        configuration.set("pxf.sasl.connection.retries", "0");
        expectOperationGSSExceptionReported(1); // with 0 retries the handler will call the callable 1 time
    }

    @Test
    public void testSecureGSSExceptionFailureNegativeCustomRetries() throws Exception {
        expectSecure();
        configuration.set("pxf.sasl.connection.retries", "-1");

        expectedException = assertThrows(RuntimeException.class,
                () -> execute(0)); // will not get to execute the callable
        assertEquals("Property pxf.sasl.connection.retries can not be set to a negative value -1", expectedException.getMessage());
    }

    @Test
    public void testSecureGSSExceptionOvercomeAfterOneRetry() throws Exception {
        expectSecure();
        when(mockCallable.call()).thenThrow(new IOException("GSS initiate failed")).thenReturn(result);
        execute(2); // 2 attempts total
    }

    @Test
    public void testSecureGSSExceptionOvercomeAfterFiveRetries() throws Exception {
        expectSecure();
        when(mockCallable.call())
                .thenThrow(new IOException("oopsGSS initiate failed"))
                .thenThrow(new IOException("oops GSS initiate failedoops there"))
                .thenThrow(new IOException("GSS initiate failedoops"))
                .thenThrow(new IOException("GSS initiate failed oops"))
                .thenThrow(new IOException("GSS initiate failed"))
                .thenReturn(result);
        execute(6); // 6 attempts total, default limit
    }

    @Test
    public void testSecureGSSExceptionFailedAfterAllAllowedRetries() throws Exception {
        expectSecure();
        when(mockCallable.call())
                .thenThrow(new IOException("oopsGSS initiate failed"))
                .thenThrow(new IOException("oops GSS initiate failedoops there"))
                .thenThrow(new IOException("GSS initiate failedoops"))
                .thenThrow(new IOException("GSS initiate failed oops"))
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate failed"));

        // 6 attempts total, default limit
        expectedException = assertThrows(IOException.class,
                () -> execute(6));
        assertEquals("GSS initiate failed", expectedException.getMessage());
    }

    @Test
    public void testSecureGSSExceptionFailureOvercomeButErroredOtherwise() throws Exception {
        expectSecure();
        when(mockCallable.call())
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate oops failed")); // treated as another error

        // with 2 retries the handler will call the callable 3 times
        expectedException = assertThrows(IOException.class,
                () -> execute(3));
        assertEquals("GSS initiate oops failed", expectedException.getMessage());
    }

    // ---------- SECURE TESTS - WITH RETRIES AND CALLBACKS ----------
    @Test
    public void testSecureGSSExceptionOvercomeAfterTwoRetriesWithCallbacks() throws Exception {
        expectSecure();
        when(mockCallable.call())
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate failed"))
                .thenReturn(result);
        execute(3, 2); // 3 attempts total, 2 callback
    }

    @Test
    public void testSecureGSSExceptionOvercomeTwiceButFailsOnThirdCallback() throws Exception {
        expectSecure();
        when(mockCallable.call())
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate failed"));
        doNothing().doNothing().doThrow(new RuntimeException("dont call me")).when(mockRunnable).run();

        // 3 attempts total, 3 callbacks as well
        expectedException = assertThrows(RuntimeException.class,
                () -> execute(3, 3));
        assertEquals("dont call me", expectedException.getMessage());
    }

    private Object execute() throws Exception {
        return execute(1);
    }

    private Object execute(int expectedNumberOfCalls) throws Exception {
        return execute(expectedNumberOfCalls, 0);
    }

    private Object execute(int expectedNumberOfCalls, int expectedNumberOfCallbacks) throws Exception {
        try {
            if (expectedNumberOfCallbacks > 0) {
                return handler.execute(configuration, "foo", mockCallable, mockRunnable);
            } else {
                return handler.execute(configuration, "foo", mockCallable);
            }
        } finally {
            verify(mockCallable, times(expectedNumberOfCalls)).call();
            verify(mockRunnable, times(expectedNumberOfCallbacks)).run();
        }
    }

    private void expectNonSecure() {
        //when(mockConfiguration.get("hadoop.security.authentication","simple")).thenReturn("simple");
    }

    private void expectSecure() {
        configuration.set("hadoop.security.authentication","kerberos");
    }

    private void expectOperationSuccess() throws Exception {
        when(mockCallable.call()).thenReturn(result);
    }

    private void expectOperationExceptionReported() throws Exception {
        when(mockCallable.call()).thenThrow(new Exception("oops"));
        expectedException = assertThrows(Exception.class,
                () -> execute());
        assertEquals("oops", expectedException.getMessage());
    }

    private void expectOperationIOExceptionReported() throws Exception {
        when(mockCallable.call()).thenThrow(new IOException("oops"));
        expectedException = assertThrows(IOException.class,
                () -> execute());
        assertEquals("oops", expectedException.getMessage());
    }

    private void expectOperationGSSExceptionReported() throws Exception {
        expectOperationGSSExceptionReported(1);
    }
    private void expectOperationGSSExceptionReported(int expectedNumberOfCalls) throws Exception {
        when(mockCallable.call()).thenThrow(new IOException("GSS initiate failed"));
        expectedException = assertThrows(IOException.class,
                () -> execute(expectedNumberOfCalls));
        assertEquals("GSS initiate failed", expectedException.getMessage());
    }
}
