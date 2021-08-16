package org.greenplum.pxf.service.utilities;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GSSFailureHandlerTest {

    private GSSFailureHandler handler;
    private Object result;
    private Configuration configuration;
    @Mock
    private Callable<Object> mockCallable;
    @Mock
    private Runnable mockRunnable;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup () {
        handler = GSSFailureHandler.getInstance();
        result = new Object();
        configuration = new Configuration(); // using real configuration instead of mock

        //when(mockConfiguration.getInt("pxf.sasl.connection.retries",5)).thenReturn(5); // default
    }

    @Test
    public void testGetInstance() {
        assertSame(handler, GSSFailureHandler.getInstance());
    }

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
        execute();
    }

    @Test
    public void testNonSecureIOExceptionFailure() throws Exception {
        expectNonSecure();
        expectOperationIOExceptionReported();
        execute();
    }

    @Test
    public void testNonSecureGSSExceptionFailure() throws Exception {
        expectNonSecure();
        expectOperationGSSExceptionReported();
        execute();
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
        execute();
    }

    @Test
    public void testSecureIOExceptionFailure() throws Exception {
        expectSecure();
        expectOperationIOExceptionReported();
        execute();
    }

    // ---------- SECURE TESTS - WITH RETRIES ----------
    @Test
    public void testSecureGSSExceptionFailure() throws Exception {
        expectSecure();
        expectOperationGSSExceptionReported();
        execute(6); // with 5 retries the handler will call the callable 6 times
    }

    @Test
    public void testSecureGSSExceptionFailureCustomRetries() throws Exception {
        expectSecure();
        expectOperationGSSExceptionReported();
        configuration.set("pxf.sasl.connection.retries", "2");
        execute(3); // with 2 retries the handler will call the callable 3 times
    }

    @Test
    public void testSecureGSSExceptionFailureZeroCustomRetries() throws Exception {
        expectSecure();
        expectOperationGSSExceptionReported();
        configuration.set("pxf.sasl.connection.retries", "0");
        execute(1); // with 0 retries the handler will call the callable 1 time
    }

    @Test
    public void testSecureGSSExceptionFailureNegativeCustomRetries() throws Exception {
        expectSecure();
        when(mockCallable.call()).thenThrow(new IOException("GSS initiate failed"));
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Property pxf.sasl.connection.retries can not be set to a negative value -1");

        configuration.set("pxf.sasl.connection.retries", "-1");
        execute(0); // will not get to execute the callable
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
        expectedException.expect(IOException.class);
        expectedException.expectMessage("GSS initiate failed");
        when(mockCallable.call())
                .thenThrow(new IOException("oopsGSS initiate failed"))
                .thenThrow(new IOException("oops GSS initiate failedoops there"))
                .thenThrow(new IOException("GSS initiate failedoops"))
                .thenThrow(new IOException("GSS initiate failed oops"))
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate failed"));
        execute(6); // 6 attempts total, default limit
    }

    @Test
    public void testSecureGSSExceptionFailureOvercomeButErroredOtherwise() throws Exception {
        expectSecure();
        expectedException.expect(IOException.class);
        expectedException.expectMessage("GSS initiate oops failed");
        when(mockCallable.call())
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate oops failed")); // treated as another error
        execute(3); // with 2 retries the handler will call the callable 3 times
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
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("dont call me");
        when(mockCallable.call())
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate failed"))
                .thenThrow(new IOException("GSS initiate failed"));
        doNothing().doNothing().doThrow(new RuntimeException("dont call me")).when(mockRunnable).run();
        execute(3, 3); // 3 attempts total, 3 callbacks as well
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
        expectedException.expect(Exception.class);
        expectedException.expectMessage("oops");
    }

    private void expectOperationIOExceptionReported() throws Exception {
        when(mockCallable.call()).thenThrow(new IOException("oops"));
        expectedException.expect(IOException.class);
        expectedException.expectMessage("oops");
    }

    private void expectOperationGSSExceptionReported() throws Exception {
        when(mockCallable.call()).thenThrow(new IOException("GSS initiate failed"));
        expectedException.expect(IOException.class);
        expectedException.expectMessage("GSS initiate failed");
    }
}
