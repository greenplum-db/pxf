package org.greenplum.pxf.api.error;

import lombok.Getter;

public class PxfRuntimeException extends RuntimeException {

    @Getter
    private final String hint;

    public PxfRuntimeException() {
        this(null, null, null);
    }

    public PxfRuntimeException(String message) {
        this(message, null, null);
    }

    public PxfRuntimeException(String message, String hint) {
        this(message, hint, null);
    }

    public PxfRuntimeException(String message, Throwable cause) {
        this(message, null, cause);
    }

    public PxfRuntimeException(String message, String hint, Throwable cause) {
        super(message, cause);
        this.hint = hint;
    }

}
