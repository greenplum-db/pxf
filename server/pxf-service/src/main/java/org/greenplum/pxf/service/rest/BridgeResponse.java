package org.greenplum.pxf.service.rest;

import lombok.SneakyThrows;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
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
import java.util.List;

public class BridgeResponse implements StreamingResponseBody {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final BridgeFactory bridgeFactory;
    private final RequestContext context;
    private final List<Fragment> fragments;
    private final SecurityService securityService;

    public BridgeResponse(SecurityService securityService, BridgeFactory bridgeFactory,
                          RequestContext context, List<Fragment> fragments) {
        this.securityService = securityService;
        this.bridgeFactory = bridgeFactory;
        this.context = context;
        this.fragments = fragments;
    }

    @SneakyThrows
    @Override
    public void writeTo(OutputStream out) {
        PrivilegedExceptionAction<Void> action = () -> writeToInternal(out);
        securityService.doAs(context, true, action);
    }

    private Void writeToInternal(OutputStream out) throws IOException {
        int fragmentIndex = 0;
        final String dataDir = context.getDataSource();
        long recordCount = 0;

        try {
            for (Fragment fragment : fragments) {
                context.setDataSource(fragment.getSourceName());
                context.setFragmentIndex(fragmentIndex);
                context.setFragmentMetadata(fragment.getMetadata());

                if (StringUtils.isNotBlank(fragment.getProfile())) {
                    context.setProfile(fragment.getProfile());
                }

                Bridge bridge = bridgeFactory.getBridge(context);

                try {
                    if (!bridge.beginIteration()) {
                        return null;
                    }
                    Writable record;
                    DataOutputStream dos = new DataOutputStream(out);

                    LOG.debug("Starting streaming fragment {} of resource {}", fragmentIndex, dataDir);
                    while ((record = bridge.getNext()) != null) {
                        record.write(dos);
                        ++recordCount;
                    }
                    LOG.debug("Finished streaming fragment {} of resource {}, {} records.", fragmentIndex, dataDir, recordCount);
                } finally {
                    LOG.debug("Stopped streaming fragment {} of resource {}, {} records.", fragmentIndex, dataDir, recordCount);
                    try {
                        bridge.endIteration();
                    } catch (Exception e) {
                        LOG.warn("Ignoring error encountered during bridge.endIteration()", e);
                    }
                }
                fragmentIndex++;
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
}
