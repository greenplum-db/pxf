package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
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
        this.accessor = applicationContext.getBean(context.getAccessor().substring(context.getAccessor().lastIndexOf(".") + 1), Accessor.class);
        this.resolver = applicationContext.getBean(context.getResolver().substring(context.getResolver().lastIndexOf(".") + 1), Resolver.class);
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
