package org.greenplum.pxf.service;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.mock.web.MockHttpServletRequest;

class PxfConfigurationTest {

    private PxfConfiguration configuration;

    private MockHttpServletRequest mockRequest;

    @BeforeEach
    public void setup() {
        configuration = new PxfConfiguration(null);
        mockRequest = new MockHttpServletRequest();
    }
}
