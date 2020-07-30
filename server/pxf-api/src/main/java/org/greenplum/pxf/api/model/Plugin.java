package org.greenplum.pxf.api.model;

import org.springframework.beans.factory.InitializingBean;

/**
 * Base interface for all plugin types that provides information on plugin thread safety
 */
public interface Plugin extends InitializingBean {

    /**
     * Sets the context for the current request
     *
     * @param context the context for the current request
     */
    void setRequestContext(RequestContext context);
}
