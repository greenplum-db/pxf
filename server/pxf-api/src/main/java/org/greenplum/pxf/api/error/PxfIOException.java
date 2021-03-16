package org.greenplum.pxf.api.error;

import java.io.IOException;

/**
 * An extension of IOException that indicates that the exception has been generated and processed by PXF
 */
public class PxfIOException extends IOException {

    /**
     * Creates a new instance from an existing IOException
     *
     * @param cause original exception
     */
    public PxfIOException(Exception cause) {
        super(cause.getMessage(), cause);
    }
}
