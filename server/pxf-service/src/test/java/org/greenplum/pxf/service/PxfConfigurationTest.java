package org.greenplum.pxf.service;

import org.greenplum.pxf.service.servlet.SecurityServletFilter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(classes = PxfConfiguration.class)
@TestPropertySource("classpath:server-config-test.properties")
class PxfConfigurationTest {

    @Test
    void defaultFilterConfiguration() {
        PxfConfiguration pxfConfiguration = new PxfConfiguration();

        FilterRegistrationBean<SecurityServletFilter> bean = pxfConfiguration.registerSecurityServletFilter();
        assertNotNull(bean);
        assertNotNull(bean.getFilter());
        assertNotNull(bean.getUrlPatterns());
        assertEquals(1, bean.getUrlPatterns().size());
        assertEquals("/pxf/v15/*", bean.getUrlPatterns().toArray()[0]);
    }
}