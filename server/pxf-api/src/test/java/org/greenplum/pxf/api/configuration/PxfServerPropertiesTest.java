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
        bind("pxf.task-execution.thread-name-prefix", "foo-bar");
        assertThat(this.properties.getTaskExecution().getThreadNamePrefix()).isEqualTo("foo-bar");
    }

    @Test
    public void testTaskExecutionPoolCoreSizeBinding() {
        bind("pxf.task-execution.pool.core-size", "50");
        assertThat(this.properties.getTaskExecution().getPool().getCoreSize()).isEqualTo(50);
    }

    @Test
    public void testTaskExecutionPoolKeepAliveBinding() {
        bind("pxf.task-execution.pool.keep-alive", "120s");
        assertThat(this.properties.getTaskExecution().getPool().getKeepAlive()).isEqualTo(Duration.ofSeconds(120));
    }

    @Test
    public void testTaskExecutionPoolMaxSizeBinding() {
        bind("pxf.task-execution.pool.max-size", "200");
        assertThat(this.properties.getTaskExecution().getPool().getMaxSize()).isEqualTo(200);
    }

    @Test
    public void testTaskExecutionPoolQueueCapacityBinding() {
        bind("pxf.task-execution.pool.queue-capacity", "5");
        assertThat(this.properties.getTaskExecution().getPool().getQueueCapacity()).isEqualTo(5);
    }

    @Test
    public void testTaskExecutionPoolAllowCoreThreadTimeoutBinding() {
        bind("pxf.task-execution.pool.allow-core-thread-timeout", "false");
        assertThat(this.properties.getTaskExecution().getPool().isAllowCoreThreadTimeout()).isEqualTo(false);
    }

    @Test
    public void testTaskExecutionShutdownAwaitTerminationPeriodBinding() {
        bind("pxf.task-execution.shutdown.await-termination-period", "20s");
        assertThat(this.properties.getTaskExecution().getShutdown().getAwaitTerminationPeriod()).isEqualTo(Duration.ofSeconds(20));
    }

    @Test
    public void testTaskExecutionShutdownBinding() {
        bind("pxf.task-execution.shutdown.await-termination", "true");
        assertThat(this.properties.getTaskExecution().getShutdown().isAwaitTermination()).isEqualTo(true);
    }

    private void bind(String name, String value) {
        bind(Collections.singletonMap(name, value));
    }

    private void bind(Map<String, String> map) {
        ConfigurationPropertySource source = new MapConfigurationPropertySource(map);
        new Binder(source).bind("pxf", Bindable.ofInstance(this.properties));
    }
}