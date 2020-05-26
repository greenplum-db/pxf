package org.greenplum.pxf.api.model;

/**
 * Base interface for all plugin types that provides information on plugin thread safety
 */
public interface Plugin {

    /**
     * Checks if the plugin is thread safe
     *
     * @return true if plugin is thread safe, false otherwise
     */
    default boolean isThreadSafe() {
        return true;
    }
}
