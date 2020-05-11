package org.greenplum.pxf.api.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@EnableConfigurationProperties(value = PxfServerProperties.class)
@TestPropertySource("classpath:server-config-test.properties")
class PxfServerPropertiesTest {

    @Autowired
    PxfServerProperties properties;

    @Test
    public void testPxfConfIsSet() {
        assertNotNull(properties.getConf());
        assertEquals("/path/to/pxf/conf", properties.getConf());

        assertNotNull(properties.getTomcat());
        assertEquals(50, properties.getTomcat().getMaxHeaderCount());
        assertFalse(properties.isMetadataCache());
    }
}