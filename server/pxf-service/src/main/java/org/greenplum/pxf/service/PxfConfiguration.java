package org.greenplum.pxf.service;

import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.greenplum.pxf.service.servlet.SecurityServletFilter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares the registerSecurityServletFilter bean method to be processed by
 * the Spring container
 */
@Configuration
@EnableConfigurationProperties(PxfServerProperties.class)
public class PxfConfiguration {

    private final BuildProperties buildProperties;

    /**
     * Constructs a new PxfConfiguration class
     *
     * @param buildProperties the build property information
     */
    public PxfConfiguration(BuildProperties buildProperties) {
        this.buildProperties = buildProperties;
    }

    /**
     * Returns a {@link FilterRegistrationBean} that registers the
     * {@link SecurityServletFilter} for URL patterns that match
     * /pxf/{protocol_version}/*
     *
     * @return the {@link FilterRegistrationBean} for the {@link SecurityServletFilter}
     */
    @Bean
    public FilterRegistrationBean<SecurityServletFilter> registerSecurityServletFilter() {
        FilterRegistrationBean<SecurityServletFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new SecurityServletFilter());
        registrationBean.addUrlPatterns("/pxf/" + buildProperties.get("protocol_version") + "/*");
        return registrationBean;
    }
}
