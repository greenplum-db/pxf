package org.greenplum.pxf.service.rest;

import lombok.SneakyThrows;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.PluginConf;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.FragmenterService;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.security.SecurityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class BridgeResponse implements StreamingResponseBody {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final BridgeFactory bridgeFactory;
    private final SecurityService securityService;
    private final FragmenterService fragmenterService;
    private final RequestContext context;
    private final String dataDir;

    public BridgeResponse(BridgeFactory bridgeFactory,
                          SecurityService securityService,
                          FragmenterService fragmenterService,
                          RequestContext context) {
        this.securityService = securityService;
        this.bridgeFactory = bridgeFactory;
        this.fragmenterService = fragmenterService;
        this.context = context;
        this.dataDir = context.getDataSource();
    }

    @SneakyThrows
    @Override
    public void writeTo(OutputStream out) {
        PrivilegedExceptionAction<Void> action = () -> writeToInternal(out);
        securityService.doAs(context, action);
    }

    private Void writeToInternal(OutputStream out) throws IOException {
        Instant startTime = Instant.now();
        long recordCount = 0;
        boolean restoreOriginalValues;

        String originalProfile = context.getProfile();
        String originalAccessor = context.getAccessor();
        String originalResolver = context.getResolver();
        String originalProfileScheme = context.getProfileScheme();

        try {
            List<Fragment> fragments = fragmenterService.getFragmentsForSegment(context);
            DataOutputStream dos = new DataOutputStream(out);
            RequestContext context = this.context;
            for (int i = 0; i < fragments.size(); i++) {
                Fragment fragment = fragments.get(i);
                String profile = fragment.getProfile();
                restoreOriginalValues = false;
                if (StringUtils.isNotBlank(profile) &&
                        !StringUtils.equalsIgnoreCase(profile, context.getProfile())) {
                    restoreOriginalValues = true;
                    LOG.debug("{} Fragment {} of resource {} using profile: {}", context.getId(), fragment.getIndex(), dataDir, profile);
                    updateProfile(context, profile);
                }
                context.setDataSource(fragment.getSourceName());
                context.setFragmentIndex(fragment.getIndex());
                context.setFragmentMetadata(fragment.getMetadata());

                recordCount += processFragment(dos, context, fragment);

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

            if (recordCount > 0) {
                long durationMs = Duration.between(startTime, Instant.now()).toMillis();
                double rate = durationMs == 0 ? 0 : (1000.0 * recordCount / durationMs);
                LOG.info("{} completed streaming {} tuple{} in {}ms. {} tuples/sec",
                        context.getId(),
                        recordCount,
                        recordCount == 1 ? "" : "s",
                        durationMs,
                        String.format("%.2f", rate));
            } else {
                LOG.info("{} completed", context.getId());
            }

        } catch (ClientAbortException e) {
            // Occurs whenever client (GPDB) decides to end the connection
            if (LOG.isDebugEnabled()) {
                // Stacktrace in debug
                LOG.warn(String.format("Remote connection closed by GPDB (segment %s)", context.getSegmentId()), e);
            } else {
                LOG.warn("Remote connection closed by GPDB (segment {}) (Enable debug for stacktrace)", context.getSegmentId());
            }
            // Re-throw the exception so Spring MVC is aware that an IO error has occurred
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
        return null;
    }

    private int processFragment(DataOutputStream dos, RequestContext context, Fragment fragment) throws Exception {
        Writable record;
        int recordCount = 0;
        Bridge bridge = bridgeFactory.getBridge(context);
        try {
            if (!bridge.beginIteration()) return 0;

            LOG.debug("{} Starting streaming fragment {} of resource {}", context.getId(), fragment.getIndex(), dataDir);
            while ((record = bridge.getNext()) != null) {
                record.write(dos);
                ++recordCount;
            }
            LOG.debug("{} Finished streaming fragment {} of resource {}, {} records.", context.getId(), fragment.getIndex(), dataDir, recordCount);
        } finally {
            LOG.debug("{} Stopped streaming fragment {} of resource {}, {} records.", context.getId(), fragment.getIndex(), dataDir, recordCount);
            try {
                bridge.endIteration();
            } catch (Exception e) {
                LOG.warn("{} Ignoring error encountered during bridge.endIteration()", context.getId(), e);
            }
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
