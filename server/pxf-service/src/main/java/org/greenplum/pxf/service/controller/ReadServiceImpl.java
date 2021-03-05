package org.greenplum.pxf.service.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.PluginConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.FragmenterService;
import org.greenplum.pxf.service.MetricsReporter;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.security.SecurityService;
import org.springframework.stereotype.Service;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the ReadService.
 */
@Service
@Slf4j
public class ReadServiceImpl extends BaseServiceImpl implements ReadService {

    private final FragmenterService fragmenterService;

    /**
     * Creates a new instance.
     *
     * @param configurationFactory configuration factory
     * @param bridgeFactory        bridge factory
     * @param securityService      security service
     * @param fragmenterService    fragmenter service
     * @param metricsReporter      metrics reporter service
     */
    public ReadServiceImpl(ConfigurationFactory configurationFactory,
                           BridgeFactory bridgeFactory,
                           SecurityService securityService,
                           FragmenterService fragmenterService,
                           MetricsReporter metricsReporter) {
        super("Read", configurationFactory, bridgeFactory, securityService, metricsReporter);
        this.fragmenterService = fragmenterService;
    }

    @Override
    public void readData(RequestContext context, OutputStream outputStream) throws IOException {
        processData(context, () -> writeStream(context, outputStream));
    }

    /**
     * Calls Fragmenter service to get a list of fragments for the resource, then reads records for each fragment
     * and writes them to the output stream.
     *
     * @param context      request context
     * @param outputStream output stream
     * @return operation statistics
     * @throws IOException if error occurs when reading data
     */
    private OperationStats writeStream(RequestContext context, OutputStream outputStream) throws IOException {
        long recordCount = 0;
        boolean restoreOriginalValues;

        String originalProfile = context.getProfile();
        String originalAccessor = context.getAccessor();
        String originalResolver = context.getResolver();
        String originalProfileScheme = context.getProfileScheme();

        int recordReportFrequency = metricsReporter.getReportFrequency(MetricsReporter.PxfMetric.RECORDS_SENT);

        try {
            List<Fragment> fragments = fragmenterService.getFragmentsForSegment(context);
            DataOutputStream dos = new DataOutputStream(outputStream);
            for (int i = 0; i < fragments.size(); i++) {
                Fragment fragment = fragments.get(i);
                String profile = fragment.getProfile();
                restoreOriginalValues = false;
                if (StringUtils.isNotBlank(profile) &&
                        !StringUtils.equalsIgnoreCase(profile, context.getProfile())) {
                    restoreOriginalValues = true;
                    log.debug("{} Fragment {} of resource {} will be using profile: {}",
                            context.getId(), fragment.getIndex(), fragment.getSourceName(), profile);
                    updateProfile(context, profile);
                }
                context.setDataSource(fragment.getSourceName());
                context.setFragmentIndex(fragment.getIndex());
                context.setFragmentMetadata(fragment.getMetadata());

                recordCount += processFragment(dos, context, fragment, recordReportFrequency);

                // In cases where we have hundreds of thousands of fragments,
                // we want to release the fragment reference as soon as we are
                // done processing the fragment. This allows the GC to reclaim
                // any memory, under memory stress situations, if needed.
                fragments.set(i, null);

                if (restoreOriginalValues) {
                    // Restore the original values so that the next
                    // fragment will use the default profile settings
                    context.setProfile(originalProfile);
                    context.setAccessor(originalAccessor);
                    context.setResolver(originalResolver);
                    context.setProfileScheme(originalProfileScheme);
                }
            }
        } catch (ClientAbortException e) {
            // Occurs whenever client (GPDB) decides to end the connection
            if (log.isDebugEnabled()) {
                // Stacktrace in debug
                log.warn(String.format("%s Remote connection closed by GPDB (segment %s)",
                        context.getId(), context.getSegmentId()), e);
            } else {
                log.warn("{} Remote connection closed by GPDB (segment {}) (Enable debug for stacktrace)",
                        context.getId(), context.getSegmentId());
            }
            // Re-throw the exception so Spring MVC is aware that an IO error has occurred
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
        return OperationStats.builder().operation("read").recordCount(recordCount).build();
    }

    private int processFragment(DataOutputStream dos, RequestContext context, Fragment fragment, int recordReportFrequency) throws Exception {
        Writable record;
        int recordCount = 0;
        boolean success = false;
        Instant startTime = Instant.now();
        Bridge bridge = null;
        try {
            bridge = getBridge(context);
            if (!bridge.beginIteration()) {
                log.debug("{} Skipping streaming fragment {} of resource {}",
                        context.getId(), fragment.getIndex(), context.getDataSource());
            } else {
                log.debug("{} Starting streaming fragment {} of resource {}",
                        context.getId(), fragment.getIndex(), context.getDataSource());
                while ((record = bridge.getNext()) != null) {
                    record.write(dos);
                    ++recordCount;
                    // report records based off the recordReportFrequency
                    if (recordCount % recordReportFrequency == 0) {
                        metricsReporter.reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, recordReportFrequency, context);
                    }
                }
                // report the remaining records that have yet to be reported
                int remainder = recordCount % recordReportFrequency;
                if (remainder != 0) {
                    metricsReporter.reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, remainder, context);
                }
            }
            success = true;
        } finally {
            if (bridge != null) {
                try {
                    bridge.endIteration();
                } catch (Exception e) {
                    log.warn("{} Ignoring error encountered during bridge.endIteration()", context.getId(), e);
                }

            }
            Duration duration = Duration.between(startTime, Instant.now());
            log.debug("{} Finished processing fragment {} of resource {}, {} records in {} ms.",
                    context.getId(), fragment.getIndex(), context.getDataSource(), recordCount, duration.toMillis());
            metricsReporter.reportTimer(MetricsReporter.PxfMetric.FRAGMENTS_SENT, duration, context, success);
        }
        return recordCount;
    }

    private void updateProfile(RequestContext context, String profile) {
        context.setProfile(profile);
        PluginConf pluginConf = context.getPluginConf();
        Map<String, String> pluginMap = pluginConf.getPlugins(profile);
        context.setAccessor(pluginMap.get("ACCESSOR"));
        context.setResolver(pluginMap.get("RESOLVER"));

        String handlerClassName = pluginConf.getHandler(profile);
        Utilities.updatePlugins(context, handlerClassName);
        context.setProfileScheme(pluginConf.getProtocol(profile));
    }

}
