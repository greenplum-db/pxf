package org.greenplum.pxf.api.error;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PxfRuntimeExceptionTest {

    @Test
    public void testExplicitErrorMessage() {
        PxfRuntimeException e = new PxfRuntimeException("message", new NullPointerException("dummy"));
        assertEquals("message", e.getMessage());
    }

    @Test
    public void testErrorMessageFromCause() {
        PxfRuntimeException e = new PxfRuntimeException(new NullPointerException("message"));
        assertEquals("message", e.getMessage());
    }

    @Test
    public void testDefaultErrorMessageWhenCauseDoesNotHaveIt() {
        PxfRuntimeException e = new PxfRuntimeException(new NullPointerException());
        assertEquals("java.lang.NullPointerException", e.getMessage());
    }

}
