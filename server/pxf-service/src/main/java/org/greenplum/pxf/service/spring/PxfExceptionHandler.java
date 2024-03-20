package org.greenplum.pxf.service.spring;

import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Handler for PXF specific exceptions that just reports the request error status. The actual message body with
 * all proper error attributes is created by the Spring MVC BasicErrorController.
 * <p>
 * This handler prevents the PXF specific exception from being thrown to the container, where it would've gotten
 * logged without an MDC context, since by that time the MDC context is cleaned up.
 * <p>
 * Instead, it is assumed that the PXF specific exception has been seen by the PXF resource
 * or the processing logic and was logged there, where the MDC context is still available.
 */
@ControllerAdvice
public class PxfExceptionHandler {

    /**
     * Handles PxfRuntimeException that PXF controller methods can throw. If the response has already been committed,
     * it re-throws the exception to signal Tomcat to abort the connection without properly terminating it
     * with a 0-length chunk. If the response has not yet been committed, it sets the response status to 500 that will
     * cause the exception to follow the error flow and be serialized as JSON by Spring Boot to the response message body.
     * @param e exception to process
     * @param response http response object
     * @throws IOException if any operation fails
     */
    @ExceptionHandler({PxfRuntimeException.class})
    public void handlePxfRuntimeException(PxfRuntimeException e, HttpServletResponse response) throws IOException {
        if (response.isCommitted()) {
            // streaming has already started, it's too late to send an error
            // re-throw the exception so that Tomcat can write the error response and
            // terminate the connection immediately without writing the end 0-length chunk
            // causing the client to recognize an error occurred
            // if the exception is not re-thrown, Tomcat thinks it has been handled and does not terminate the connection
            // abnormally, which results in client not realizing there was an error in PXF server
            throw e;
        } else {
            // do not re-throw the error, otherwise it will be logged 2 more times by Tomcat, just set the error status
            response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }
}
