package org.greenplum.pxf.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PxfApiVersionCheckerTest {

    @Test
    public void testApiVersionsMatch() {
        PxfApiVersionChecker apiVersionChecker = new PxfApiVersionChecker();
        assertTrue(apiVersionChecker.isCompatible("1.0.0", "1.0.0"));
        assertTrue(apiVersionChecker.isCompatible(null, null));

        assertFalse(apiVersionChecker.isCompatible("1.1.0", "1.0.0"));
        assertFalse(apiVersionChecker.isCompatible("1.0.0", "1.1.0"));
        assertFalse(apiVersionChecker.isCompatible("1.0.0", "1.0.1"));
        assertFalse(apiVersionChecker.isCompatible(null, "1.0.0"));
        assertFalse(apiVersionChecker.isCompatible("1.0.0", null));

    }
}
