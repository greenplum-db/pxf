package org.greenplum.pxf.service;

import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.greenplum.pxf.service.servlet.SecurityServletFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@EnableConfigurationProperties(value = PxfServerProperties.class)
@TestPropertySource("classpath:server-config-test.properties")
class PxfConfigurationTest {

    @MockBean
    @Autowired
    SecurityServletFilter securityServletFilter;

    @Test
    void defaultFilterConfiguration() {
        PxfConfiguration pxfConfiguration = new PxfConfiguration(securityServletFilter);

        FilterRegistrationBean<SecurityServletFilter> bean = pxfConfiguration.registerSecurityServletFilter();
        assertNotNull(bean);
        assertNotNull(bean.getFilter());
        assertNotNull(bean.getUrlPatterns());
        assertEquals(1, bean.getUrlPatterns().size());
        assertEquals("/pxf/v15/*", bean.getUrlPatterns().toArray()[0]);
    }
}