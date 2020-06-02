package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Base class for all plugin types (Accessor, Resolver, Fragmenter, ...).
 * Manages the meta data.
 */
public class BasePlugin implements Plugin {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected Configuration configuration;
    protected RequestContext context;

    /**
     * {@inheritDoc}
     */
    @Autowired
    public void setRequestContext(RequestContext context) {
        this.context = context;
        this.configuration = context.getConfiguration();
    }

    /**
     * Method called after the {@link RequestContext} and {@link Configuration}
     * have been bound to the BasePlugin and is ready to be consumed by
     * implementing classes
     */
    public void initialize() {
    }
}
