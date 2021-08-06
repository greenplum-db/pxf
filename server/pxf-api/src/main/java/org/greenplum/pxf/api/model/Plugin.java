package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

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

    /**
     * Returns Configuration that the plugin has access to, if any.
     *
     * @return the instance of Configuration
     */
    Configuration getConfiguration();
}
