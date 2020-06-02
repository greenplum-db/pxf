package org.greenplum.pxf.api.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
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

    @Test
    public void testTaskExecutionThreadNamePrefixBinding() {
        bind("pxf.task.thread-name-prefix", "foo-bar");
        assertThat(this.properties.getTask().getThreadNamePrefix()).isEqualTo("foo-bar");
    }

    @Test
    public void testTaskExecutionPoolCoreSizeBinding() {
        bind("pxf.task.pool.core-size", "50");
        assertThat(this.properties.getTask().getPool().getCoreSize()).isEqualTo(50);
    }

    @Test
    public void testTaskExecutionPoolKeepAliveBinding() {
        bind("pxf.task.pool.keep-alive", "120s");
        assertThat(this.properties.getTask().getPool().getKeepAlive()).isEqualTo(Duration.ofSeconds(120));
    }

    @Test
    public void testTaskExecutionPoolMaxSizeBinding() {
        bind("pxf.task.pool.max-size", "200");
        assertThat(this.properties.getTask().getPool().getMaxSize()).isEqualTo(200);
    }

    @Test
    public void testTaskExecutionPoolQueueCapacityBinding() {
        bind("pxf.task.pool.queue-capacity", "5");
        assertThat(this.properties.getTask().getPool().getQueueCapacity()).isEqualTo(5);
    }

    @Test
    public void testTaskExecutionPoolAllowCoreThreadTimeoutBinding() {
        bind("pxf.task.pool.allow-core-thread-timeout", "false");
        assertThat(this.properties.getTask().getPool().isAllowCoreThreadTimeout()).isEqualTo(false);
    }

    @Test
    public void testTaskExecutionShutdownAwaitTerminationPeriodBinding() {
        bind("pxf.task.shutdown.await-termination-period", "20s");
        assertThat(this.properties.getTask().getShutdown().getAwaitTerminationPeriod()).isEqualTo(Duration.ofSeconds(20));
    }

    @Test
    public void testTaskExecutionShutdownBinding() {
        bind("pxf.task.shutdown.await-termination", "true");
        assertThat(this.properties.getTask().getShutdown().isAwaitTermination()).isEqualTo(true);
    }

    private void bind(String name, String value) {
        bind(Collections.singletonMap(name, value));
    }

    private void bind(Map<String, String> map) {
        ConfigurationPropertySource source = new MapConfigurationPropertySource(map);
        new Binder(source).bind("pxf", Bindable.ofInstance(this.properties));
    }
}