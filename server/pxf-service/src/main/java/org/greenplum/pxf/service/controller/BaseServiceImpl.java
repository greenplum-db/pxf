package org.greenplum.pxf.service.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.MetricsReporter;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.security.SecurityService;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.time.Instant;

/**
 * Base abstract implementation of the Service class, provides means to execute an operation
 * using provided request context and the identity determined by the security service.
 */
@Slf4j
public abstract class BaseServiceImpl {

    protected final MetricsReporter metricsReporter;
    private final String serviceName;
    private final ConfigurationFactory configurationFactory;
    private final BridgeFactory bridgeFactory;
    private final SecurityService securityService;

    /**
     * Creates a new instance of the service with auto-wired dependencies.
     *
     * @param serviceName          name of the service
     * @param configurationFactory configuration factory
     * @param bridgeFactory        bridge factory
     * @param securityService      security service
     * @param metricsReporter      metrics reporter service
     */
    protected BaseServiceImpl(String serviceName,
                              ConfigurationFactory configurationFactory,
                              BridgeFactory bridgeFactory,
                              SecurityService securityService,
                              MetricsReporter metricsReporter) {
        this.serviceName = serviceName;
        this.configurationFactory = configurationFactory;
        this.bridgeFactory = bridgeFactory;
        this.securityService = securityService;
        this.metricsReporter = metricsReporter;
    }

    /**
     * Executes an action with the identity determined by the PXF security service.
     *
     * @param context request context
     * @param action  action to execute
     * @return operation statistics
     * @throws IOException if an error occurs during the operation
     */
    protected OperationStats processData(RequestContext context, PrivilegedExceptionAction<OperationStats> action) throws IOException {
        log.debug("{} {} service is called for resource {} using profile {}",
                context.getId(), serviceName, context.getDataSource(), context.getProfile());

        // Initialize the configuration for this request
        Configuration configuration = configurationFactory.
                initConfiguration(
                        context.getConfig(),
                        context.getServerName(),
                        context.getUser(),
                        context.getAdditionalConfigProps());
        context.setConfiguration(configuration);

        Instant startTime = Instant.now();
        OperationStats stats = securityService.doAs(context, action);
        Long recordCount = stats.getRecordCount();
        if (recordCount > 0) {
            long durationMs = Duration.between(startTime, Instant.now()).toMillis();
            double rate = durationMs == 0 ? 0 : (1000.0 * recordCount / durationMs);
            log.info("{} completed {} operation for {} tuple{} in {} ms. rate = {} tuples/sec",
                    context.getId(),
                    stats.getOperation(),
                    recordCount,
                    recordCount == 1 ? "" : "s",
                    durationMs,
                    String.format("%.2f", rate));
        } else {
            log.info("{} completed", context.getId());
        }
        return stats;
    }

    /**
     * Returns a new Bridge instance based on the current context.
     *
     * @param context request context
     * @return an instance of the bridge to use
     */
    protected Bridge getBridge(RequestContext context) {
        return bridgeFactory.getBridge(context);
    }
}
