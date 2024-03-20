package org.greenplum.pxf.service.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.MetricsReporter;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.security.SecurityService;

import java.security.PrivilegedAction;
import java.time.Duration;
import java.time.Instant;

/**
 * Base abstract implementation of the Service class, provides means to execute an operation
 * using provided request context and the identity determined by the security service.
 */
@Slf4j
public abstract class BaseServiceImpl<T> extends PxfErrorReporter<T> {

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
     */
    protected OperationStats processData(RequestContext context, PrivilegedAction<OperationResult> action) throws Exception {
        log.debug("{} service is called for resource {} using profile {}",
                serviceName, context.getDataSource(), context.getProfile());

        // initialize the configuration for this request
        Configuration configuration = configurationFactory.
                initConfiguration(
                        context.getConfig(),
                        context.getServerName(),
                        context.getUser(),
                        context.getAdditionalConfigProps());
        context.setConfiguration(configuration);

        Instant startTime = Instant.now();

        // execute processing action with a proper identity
        OperationResult result = securityService.doAs(context, action);

        // obtain results after executing the action
        OperationStats stats = result.getStats();
        Exception exception = result.getException();
        String status = (exception == null) ? "Completed" :
                (Utilities.isClientDisconnectException(exception)) ? "Aborted" : "Failed";

        // log action status and stats
        long recordCount = stats.getRecordCount();
        long byteCount = stats.getByteCount();
        long durationMs = Duration.between(startTime, Instant.now()).toMillis();
        double rate = durationMs == 0 ? 0 : (1000.0 * recordCount / durationMs);
        double byteRate = durationMs == 0 ? 0 : (1000.0 * byteCount / durationMs);

        log.info("{} {} operation [{} ms, {} record{}, {} records/sec, {} bytes, {} bytes/sec]{}",
                status,
                stats.getOperation().name().toLowerCase(),
                durationMs,
                recordCount,
                recordCount == 1 ? "" : "s",
                String.format("%.2f", rate),
                byteCount,
                String.format("%.2f", byteRate),
                (exception == null) ? "" : " for " + result.getSourceName());

        // re-throw the exception if the operation failed
        if (exception != null) {
            throw exception;
        }

        // return operation stats
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
