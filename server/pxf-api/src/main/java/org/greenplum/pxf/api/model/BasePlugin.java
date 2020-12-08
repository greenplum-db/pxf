package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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
    public void setRequestContext(RequestContext context) {
        this.context = context;
        this.configuration = context.getConfiguration();
    }

    /**
     * Method called after the {@link RequestContext} and {@link Configuration}
     * have been bound to the BasePlugin and is ready to be consumed by
     * implementing classes
     */
    @Override
    public void afterPropertiesSet() {
    }

    /**
     * When DEBUG mode is enabled, logs the total number of rows read, the
     * amount of time it took to read the file, and the average read speed
     * in nanoseconds
     *
     * @param totalRowsRead        the total number of rows read
     * @param totalReadTimeInNanos the total nanoseconds it took to read the file
     */
    protected void logReadStats(long totalRowsRead, long totalReadTimeInNanos) {
        if (!LOG.isDebugEnabled())
            return;

        final long millis = TimeUnit.NANOSECONDS.toMillis(totalReadTimeInNanos);
        long tuplesPerMillis = millis == 0 ? 0 : totalRowsRead / millis;
        LOG.debug("{}-{}: Read TOTAL of {} rows from file {} on server {} in {} ms. Average speed: {} tuples/ms",
                context.getTransactionId(),
                context.getSegmentId(),
                totalRowsRead,
                context.getDataSource(),
                context.getServerName(),
                millis,
                tuplesPerMillis);
    }
}
