package org.greenplum.pxf.api.model;

/**
 * Base interface for all plugin types that provides information on plugin thread safety
 */
public interface Plugin {

    /**
     * Sets the context for the current request
     *
     * @param context the context for the current request
     */
    void setRequestContext(RequestContext context);

    /**
     * Invoked after the {@code RequestContext} has been bound
     */
    void afterPropertiesSet();
}
