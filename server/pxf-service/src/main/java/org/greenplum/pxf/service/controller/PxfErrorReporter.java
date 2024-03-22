package org.greenplum.pxf.service.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.utilities.ThrowingSupplier;

import java.io.IOException;

/**
 * Base class that allows executing an action that returns a result or throws an exception. The exception
 * is logged into a log file and wrapped into a PxfRuntimeException that can be later handled
 * by the PxfExceptionHandler. This is the only place where any exception thrown within PXF should be logged.
 * <p>
 * In the future, this logic should be implemented by an Spring AOP annotation-driven aspect and the corresponding
 * annotation can be specified for the endpoint methods that must have any exceptions they throw be reported before
 * propagating them to the container.
 *
 * @param <T> type of data the action returns
 */
@Slf4j
public abstract class PxfErrorReporter<T> {

    protected T invokeWithErrorHandling(ThrowingSupplier<T, Exception> action) {
        try {
            // call the action and return the value if there are no errors
            return action.get();
        } catch (IOException e) {
            // if the exception is due to client disconnecting prematurely, log the appropriate message
            if (Utilities.isClientDisconnectException(e)) {
                // this occurs when a client (GPDB) ends the connection which is common for LIMIT queries
                // (ex: SELECT * FROM table LIMIT 1) so we want to log just a warning message,
                // not an error with the full stacktrace (unless in debug mode)
                if (log.isDebugEnabled()) {
                    // Stacktrace in debug
                    log.warn("Remote connection closed by the client.", e);
                } else {
                    log.warn("Remote connection closed by the client (enable debug for the stacktrace).");
                }
            } else {
                // some other IO error, log it as usual
                log.error(StringUtils.defaultIfBlank(e.getMessage(), e.getClass().getName()), e);
            }
            // wrap into PxfRuntimeException and throw back so that it can be handled by the PxfExceptionHandler
            throw new PxfRuntimeException(e);
        } catch (PxfRuntimeException | Error e) {
            // let PxfRuntimeException and Error propagate themselves
            log.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error(StringUtils.defaultIfBlank(e.getMessage(), e.getClass().getName()), e);
            // wrap into PxfRuntimeException and throw back so that it can be handled by the PxfExceptionHandler
            throw new PxfRuntimeException(e);
        }
    }
}
