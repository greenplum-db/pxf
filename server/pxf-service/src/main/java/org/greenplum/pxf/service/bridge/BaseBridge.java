package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * Abstract class representing the bridge that provides to subclasses logger and accessor and
 * resolver instances obtained from the factories.
 */
public abstract class BaseBridge implements Bridge {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected Accessor accessor;
    protected Resolver resolver;

    public BaseBridge(ApplicationContext applicationContext, RequestContext context) {
        this.accessor = applicationContext.getBean(Utilities.getShortClassName(context.getAccessor()), Accessor.class);
        this.resolver = applicationContext.getBean(Utilities.getShortClassName(context.getResolver()), Resolver.class);
    }

    /**
     * {@inheritDoc}
     */
    public void initialize() {
        this.accessor.initialize();
        this.resolver.initialize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isThreadSafe() {
        boolean result = accessor.isThreadSafe() && resolver.isThreadSafe();
        LOG.debug("Bridge is {}thread safe", (result ? "" : "not "));
        return result;
    }
}
