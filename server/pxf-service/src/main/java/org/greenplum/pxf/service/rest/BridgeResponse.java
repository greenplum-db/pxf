package org.greenplum.pxf.service.rest;

import lombok.SneakyThrows;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.security.SecurityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.locks.ReentrantLock;

public class BridgeResponse implements StreamingResponseBody {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Bridge bridge;
    private final RequestContext context;
    private final SecurityService securityService;

    public BridgeResponse(SecurityService securityService, Bridge bridge, RequestContext context) {
        this.securityService = securityService;
        this.bridge = bridge;
        this.context = context;
    }

    @SneakyThrows
    @Override
    public void writeTo(OutputStream out) {
        PrivilegedExceptionAction<Void> action = () -> writeToInternal(out);
        securityService.doAs(context, context.isLastFragment(), action);
    }

    private Void writeToInternal(OutputStream out) throws IOException {
        final int fragment = context.getDataFragment();
        final String dataDir = context.getDataSource();
        long recordCount = 0;

        try {
            if (!bridge.beginIteration()) {
                return null;
            }
            Writable record;
            DataOutputStream dos = new DataOutputStream(out);

            LOG.debug("Starting streaming fragment {} of resource {}", fragment, dataDir);
            while ((record = bridge.getNext()) != null) {
                record.write(dos);
                ++recordCount;
            }
            LOG.debug("Finished streaming fragment {} of resource {}, {} records.", fragment, dataDir, recordCount);
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
        } finally {
            LOG.debug("Stopped streaming fragment {} of resource {}, {} records.", fragment, dataDir, recordCount);
            try {
                bridge.endIteration();
            } catch (Exception e) {
                LOG.warn("Ignoring error encountered during bridge.endIteration()", e);
            }
        }
        return null;
    }
}
