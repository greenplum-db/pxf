package org.greenplum.pxf.api.model;

/**
 * Defines methods to dynamically determine processing components for a given protocol.
 */
public interface ProtocolHandler {

    /**
     * Returns the class name of a fragmenter to use for reading / writing data
     * @param context request context
     * @return class name of the fragmenter
     */
    String getFragmenterClassName(RequestContext context);

    /**
     * Returns the class name of an accessor to use for reading / writing data
     * @param context request context
     * @return class name of the accessor
     */
    String getAccessorClassName(RequestContext context);

    /**
     * Returns the class name of a resolver to use for converting data
     * @param context request context
     * @return class name of the accessor
     */
    String getResolverClassName(RequestContext context);
}
