package org.greenplum.pxf.service;

import org.greenplum.pxf.service.servlet.SecurityServletFilter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = PxfConfiguration.class)
@TestPropertySource("classpath:server-config-test.properties")
class PxfConfigurationTest {

    @MockBean
    private BuildProperties buildProperties;

    @Test
    void defaultFilterConfiguration() {
        when(buildProperties.get(anyString())).thenReturn("v15");

        PxfConfiguration pxfConfiguration = new PxfConfiguration(buildProperties);

        FilterRegistrationBean<SecurityServletFilter> bean = pxfConfiguration.registerSecurityServletFilter();
        assertNotNull(bean);
        assertNotNull(bean.getFilter());
        assertNotNull(bean.getUrlPatterns());
        assertEquals(1, bean.getUrlPatterns().size());
        assertEquals("/pxf/v15/*", bean.getUrlPatterns().toArray()[0]);
    }
}