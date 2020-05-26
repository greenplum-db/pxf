package org.greenplum.pxf.plugins.jdbc;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Configuration
@ConfigurationProperties(prefix = PxfJdbcProperties.PROPERTY_PREFIX)
@Getter
@Setter
public class PxfJdbcProperties {

    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "pxf.jdbc";

    /**
     * Customizable settings for the connection pool through PXF
     */
    private Connection connection = new Connection();

    @Getter
    @Setter
    @Validated
    public static class Connection {

        /**
         * Defines the amount of time to sleep before checking for active
         * connections in the pool
         */
        @DurationUnit(ChronoUnit.MINUTES)
        private Duration cleanupSleepInterval = Duration.ofMinutes(5);

        /**
         * Defines the maximum amount of time to wait until all connections
         * finish execution and become idle. If connections are active for over
         * the specified execution time, the connection pool will be closed and
         * destroyed.
         */
        @DurationUnit(ChronoUnit.HOURS)
        private Duration cleanupTimeout = Duration.ofHours(24);

        /**
         * Defines the expiration timeout of the pool after the last time the
         * pool was accessed.
         */
        @DurationUnit(ChronoUnit.HOURS)
        private Duration poolExpirationTimeout = Duration.ofHours(6);
    }
}
