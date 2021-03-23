package org.greenplum.pxf.service.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.service.utilities.ThrowingSupplier;

@Slf4j
public abstract class PxfErrorReporter<T> {

    protected T invokeWithErrorHandling(ThrowingSupplier<T, Exception> action) {
        try {
            // call the action and return the value if there are no errors
            return action.get();
        } catch (ClientAbortException e) {
            // the ClientAbortException occurs whenever a client (GPDB) decides to end the connection
            // which is common for LIMIT queries (ex: SELECT * FROM table LIMIT 1)
            // so we want to log just a warning message, not an error with the full stacktrace (unless in debug mode)
            if (log.isDebugEnabled()) {
                // Stacktrace in debug
                log.warn("Remote connection closed by the client.", e);
            } else {
                log.warn("Remote connection closed by the client (enable debug for the stacktrace).");
            }
            // wrap into PxfRuntimeException so that it can be handled by the PxfExceptionHandler
            throw new PxfRuntimeException(e);
        } catch (PxfRuntimeException | Error e) {
            // let PxfRuntimeException and Error propagate themselves
            log.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            // wrap into PxfRuntimeException so that it can be handled by the PxfExceptionHandler
            throw new PxfRuntimeException(e);
        }
    }
}

