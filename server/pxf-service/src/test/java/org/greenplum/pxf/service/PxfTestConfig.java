package org.greenplum.pxf.service;

import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan({"org.greenplum.pxf.api"})
@EnableConfigurationProperties(PxfServerProperties.class)
public class PxfTestConfig {
}
