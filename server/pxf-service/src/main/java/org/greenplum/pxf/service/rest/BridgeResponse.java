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

    /**
     * Lock is needed here in the case of a non-thread-safe plugin. Using
     * synchronized methods is not enough because the bridge work is called by
     * {@link StreamingResponseBody}, after we are getting out of this class's
     * context.
     * <p/>
     * BRIDGE_LOCK is accessed through lock() and unlock() functions, based on
     * the isThreadSafe parameter that is determined by the bridge.
     */
    // TODO: fine-grained locking to prevent other requests from locking when it's not necessary
    private static final ReentrantLock BRIDGE_LOCK = new ReentrantLock();

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final boolean threadSafe;
    private final Bridge bridge;
    private final RequestContext context;
    private final SecurityService securityService;

    public BridgeResponse(SecurityService securityService, Bridge bridge, RequestContext context, boolean threadSafe) {
        this.securityService = securityService;
        this.bridge = bridge;
        this.context = context;
        this.threadSafe = threadSafe;
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

        if (!threadSafe) {
            lock(dataDir);
        }

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
                LOG.debug("Remote connection closed by GPDB", e);
            } else {
                LOG.warn("Remote connection closed by GPDB (Enable debug for stacktrace)");
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
            if (!threadSafe) {
                unlock(dataDir);
            }
        }
        return null;
    }

    /**
     * Locks BRIDGE_LOCK
     *
     * @param path path for the request, used for logging.
     */
    private void lock(String path) {
        LOG.trace("Locking BridgeResource for {}", path);
        BRIDGE_LOCK.lock();
        LOG.trace("Locked BridgeResource for {}", path);
    }

    /**
     * Unlocks BRIDGE_LOCK
     *
     * @param path path for the request, used for logging.
     */
    private void unlock(String path) {
        LOG.trace("Unlocking BridgeResource for {}", path);
        BRIDGE_LOCK.unlock();
        LOG.trace("Unlocked BridgeResource for {}", path);
    }
}
