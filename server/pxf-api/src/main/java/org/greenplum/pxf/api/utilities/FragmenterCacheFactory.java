package org.greenplum.pxf.api.utilities;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.greenplum.pxf.api.model.Fragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Factory class for creation of {@link com.google.common.cache.Cache} objects.
 */
@Component
public class FragmenterCacheFactory {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Cache<String, List<Fragment>> fragmenterCache;

    /**
     * Constructs the FragmenterCacheFactory class
     */
    public FragmenterCacheFactory() {
        fragmenterCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.SECONDS)
                .removalListener((RemovalListener<String, List<Fragment>>) notification ->
                        LOG.debug("Removed fragmenterCache entry for transactionId {} with {} fragments with cause {}",
                                notification.getKey(),
                                (notification.getValue() != null ? notification.getValue().size() : 0),
                                notification.getCause().toString()))
                .build();
    }

    /**
     * @return the cache for the fragmenter
     */
    public Cache<String, List<Fragment>> getCache() {
        return fragmenterCache;
    }
}
