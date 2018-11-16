package org.greenplum.pxf.api.model;

/**
 * Base interface for all plugin types that manages initialization and provides
 * information on plugin thread safety
 */
public interface Plugin {

    /**
     * Initialize the plugin for the incoming request
     *
     * @param inputData data provided in the request
     */
    void initialize(InputData inputData);

    /**
     * Checks if the plugin is thread safe
     *
     * @return true if plugin is thread safe, false otherwise
     */
    boolean isThreadSafe();
}
