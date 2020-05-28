package org.greenplum.pxf.plugins.jdbc;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class PxfJdbcPropertiesTest {

    private final PxfJdbcProperties properties = new PxfJdbcProperties();

    @Test
    void testDefaults() {
        assertNotNull(properties.getConnection());
        assertThat(this.properties.getConnection().getCleanupSleepInterval()).isEqualTo(Duration.ofMinutes(5));
        assertThat(this.properties.getConnection().getCleanupTimeout()).isEqualTo(Duration.ofHours(24));
        assertThat(this.properties.getConnection().getPoolExpirationTimeout()).isEqualTo(Duration.ofHours(6));
    }

    @Test
    void testConnectionCleanupSleepIntervalBinding() {
        bind("pxf.jdbc.connection.cleanup-sleep-interval", "10m");
        assertThat(this.properties.getConnection().getCleanupSleepInterval()).isEqualTo(Duration.ofMinutes(10));

        bind("pxf.jdbc.connection.cleanup-sleep-interval", "2h");
        assertThat(this.properties.getConnection().getCleanupSleepInterval()).isEqualTo(Duration.ofHours(2));
    }

    @Test
    void testConnectionCleanupTimeoutBinding() {
        bind("pxf.jdbc.connection.cleanup-timeout", "5h");
        assertThat(this.properties.getConnection().getCleanupTimeout()).isEqualTo(Duration.ofHours(5));

        bind("pxf.jdbc.connection.cleanup-timeout", "5s");
        assertThat(this.properties.getConnection().getCleanupTimeout()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void testConnectionPoolExpirationTimeoutBinding() {
        bind("pxf.jdbc.connection.pool-expiration-timeout", "3h");
        assertThat(this.properties.getConnection().getPoolExpirationTimeout()).isEqualTo(Duration.ofHours(3));

        bind("pxf.jdbc.connection.pool-expiration-timeout", "5m");
        assertThat(this.properties.getConnection().getPoolExpirationTimeout()).isEqualTo(Duration.ofMinutes(5));
    }

    private void bind(String name, String value) {
        bind(Collections.singletonMap(name, value));
    }

    private void bind(Map<String, String> map) {
        ConfigurationPropertySource source = new MapConfigurationPropertySource(map);
        new Binder(source).bind("pxf.jdbc", Bindable.ofInstance(this.properties));
    }
}