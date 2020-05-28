package org.greenplum.pxf.api.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PxfServerPropertiesTest {

    private final PxfServerProperties properties = new PxfServerProperties();

    @Test
    public void testDefaults() {
        assertThat(this.properties.getConf()).isNull();
        assertThat(this.properties.isMetadataCacheEnabled()).isEqualTo(true);
        assertThat(this.properties.getTomcat()).isNotNull();
        assertThat(this.properties.getTomcat().getMaxHeaderCount()).isEqualTo(30000);
    }

    @Test
    public void testPxfConfBinding() {
        bind("pxf.conf", "/path/to/pxf/conf");
        assertThat(this.properties.getConf()).isEqualTo("/path/to/pxf/conf");
    }

    @Test
    public void testMetadataCacheEnabledBinding() {
        bind("pxf.metadata-cache-enabled", "false");
        assertThat(this.properties.isMetadataCacheEnabled()).isEqualTo(false);

        bind("pxf.metadata-cache-enabled", "true");
        assertThat(this.properties.isMetadataCacheEnabled()).isEqualTo(true);
    }

    @Test
    public void testTomcatMaxHeaderCountBinding() {
        bind("pxf.tomcat.max-header-count", "50");
        assertThat(this.properties.getTomcat().getMaxHeaderCount()).isEqualTo(50);
    }

    private void bind(String name, String value) {
        bind(Collections.singletonMap(name, value));
    }

    private void bind(Map<String, String> map) {
        ConfigurationPropertySource source = new MapConfigurationPropertySource(map);
        new Binder(source).bind("pxf", Bindable.ofInstance(this.properties));
    }
}