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
     * Method called after the {@link RequestContext} and 
     * {@link org.apache.hadoop.conf.Configuration} have been bound to the
     * BasePlugin and is ready to be consumed by implementing classes
     */
    void initialize();

    /**
     * Checks if the plugin is thread safe
     *
     * @return true if plugin is thread safe, false otherwise
     */
    default boolean isThreadSafe() {
        return true;
    }
}
