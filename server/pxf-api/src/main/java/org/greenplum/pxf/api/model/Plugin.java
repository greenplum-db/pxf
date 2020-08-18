package org.greenplum.pxf.api.model;

/**
 * Base interface for all plugin types that manages initialization and provides
 * information on plugin thread safety
 */
public interface Plugin {

    /**
     * Initialize the plugin for the incoming request
     *
     * @param requestContext data provided in the request
     */
    void initialize(RequestContext requestContext);
}
