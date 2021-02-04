package org.greenplum.pxf.service;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmenterCacheFactory;
import org.greenplum.pxf.service.utilities.AnalyzeUtils;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * The {@link FragmenterService} returns fragments for a given segment. It
 * performs caching of Fragment for a unique query. The first segment to
 * request the list of fragments will populate it, while the rest of the
 * segments will wait until the list of fragments is populated.
 */
@Component
public class FragmenterService {

    private static final Logger LOG = LoggerFactory.getLogger(FragmenterService.class);

    private final BasePluginFactory pluginFactory;

    private final FragmenterCacheFactory fragmenterCacheFactory;

    public FragmenterService(FragmenterCacheFactory fragmenterCacheFactory,
                             BasePluginFactory pluginFactory) {
        this.fragmenterCacheFactory = fragmenterCacheFactory;
        this.pluginFactory = pluginFactory;
    }

    public List<Fragment> getFragmentsForSegment(RequestContext context) throws IOException {

        LOG.trace("{} Received FRAGMENTER call", context.getId());
        Instant startTime = Instant.now();
        final String path = context.getDataSource();

        if (LOG.isDebugEnabled()) {
            LOG.debug("fragmentCache size={}, stats={}",
                    fragmenterCacheFactory.getCache().size(),
                    fragmenterCacheFactory.getCache().stats().toString());
        }

        LOG.debug("{} FRAGMENTER started for path \"{}\"", context.getId(), path);

        List<Fragment> fragments = getFragmentsFromCache(context, startTime);

        List<Fragment> filteredFragments = filterFragments(fragments,
                context.getSegmentId(),
                context.getTotalSegments(),
                context.getGpSessionId(),
                context.getGpCommandCount());

        if (LOG.isDebugEnabled()) {
            int numberOfFragments = filteredFragments.size();
            long elapsedMillis = Duration.between(startTime, Instant.now()).toMillis();

            LOG.debug("{} returns {}/{} fragment{} for path {} in {} ms for {} [profile {} predicate is{} available]",
                    context.getId(), numberOfFragments, fragments.size(), numberOfFragments == 1 ? "" : "s",
                    context.getDataSource(), elapsedMillis, context.getId(), context.getProfile(), context.hasFilter() ? "" : " not");
        }

        return filteredFragments;
    }

    /**
     * Returns the list of fragments from the fragmenter cache. If the cache is
     * empty, it populates the cache with the list of fragments. When
     * concurrent requests are made to the cache with the same key, the first
     * request will populate the cache, while the other requests will wait
     * until the cache entry is populated.
     *
     * @param context   the request context
     * @param startTime the start time of the request
     * @return the list of fragments for the request
     * @throws IOException when an exception occurs
     */
    private List<Fragment> getFragmentsFromCache(RequestContext context, Instant startTime) throws IOException {
        final String fragmenterCacheKey = getFragmenterCacheKey(context);
        try {
            return fragmenterCacheFactory.getCache()
                    .get(fragmenterCacheKey, () -> {
                        LOG.debug("{} caching fragments from segmentId={} with key={}",
                                context.getId(), context.getSegmentId(), fragmenterCacheKey);
                        List<Fragment> fragmentList = getFragmenter(context).getFragments();

                        /* Create a fragmenter instance with API level parameters */
                        fragmentList = AnalyzeUtils.getSampleFragments(fragmentList, context);
                        updateFragmentIndex(fragmentList);

                        int numberOfFragments = fragmentList.size();
                        long elapsedMillis = Duration.between(startTime, Instant.now()).toMillis();
                        LOG.info("{} returns {} fragment{} for path {} in {} ms [fragmenter {} profile {} predicate is{} available]",
                                context.getId(), numberOfFragments, numberOfFragments == 1 ? "" : "s",
                                context.getDataSource(), elapsedMillis, context.getFragmenter(),
                                context.getProfile(), context.hasFilter() ? "" : " not");

                        return fragmentList;
                    });
        } catch (UncheckedExecutionException | ExecutionException e) {
            // Unwrap the error
            Exception exception = e.getCause() != null ? (Exception) e.getCause() : e;
            if (exception instanceof IOException)
                throw (IOException) exception;
            throw new IOException(exception);
        }
    }

    /**
     * Filters the {@code fragments} for the given segment. To determine which
     * segment S should process an element at a given index i, use a randomized
     * MOD function
     * <p>
     * S = MOD(I + MOD(gp_session_id, N) + gp_command_count, N)
     * <p>
     * which ensures more fair work distribution for small lists of just a few
     * elements across N segments global session ID and command count are used
     * as a randomizer, as it is different for every query, while being the
     * same across all segments for a given query.
     *
     * @param fragments      the list of fragments
     * @param segmentId      the identifier for the segment processing the request
     * @param totalSegments  the total number of segments
     * @param gpSessionId    the Greenplum session ID
     * @param gpCommandCount the command number for this Greenplum Session ID
     * @return the filtered list of fragments for the given segment
     */
    private List<Fragment> filterFragments(List<Fragment> fragments, int segmentId, int totalSegments, int gpSessionId, int gpCommandCount) {
        int shift = gpSessionId % totalSegments;
        int fragmentCount = fragments.size();

        List<Fragment> filteredFragments = new ArrayList<>((int) Math.ceil(fragmentCount / totalSegments));
        for (int i = 0; i < fragmentCount; i++) {
            if (segmentId == ((i + shift + gpCommandCount) % totalSegments)) {
                filteredFragments.add(fragments.get(i));
            }
        }
        return filteredFragments;
    }

    /**
     * Returns the fragmenter initialized with the request context
     *
     * @param context the request context
     * @return the fragmenter initialized with the request context
     */
    private Fragmenter getFragmenter(RequestContext context) {
        return pluginFactory.getPlugin(context, context.getFragmenter());
    }

    /**
     * Returns a key for the fragmenter cache. TransactionID is not sufficient to key
     * the cache. For the case where we have multiple slices (i.e select a, b from c
     * where a = 'part1' union all select a, b from c where a = 'part2'), the list of
     * fragments for each slice in the query will be different, but the transactionID
     * will be the same. For that reason we must include the server name, data source
     * and the filter string as part of the fragmenter cache.
     *
     * @param context the request context
     * @return the key for the fragmenter cache
     */
    private String getFragmenterCacheKey(RequestContext context) {
        return String.format("%s:%s:%s:%s",
                context.getServerName(),
                context.getTransactionId(),
                context.getDataSource(),
                context.getFilterString());
    }

    /**
     * Updates the fragments' indexes so that it is incremented by sourceName.
     * (E.g.: {"a", 0}, {"a", 1}, {"b", 0} ... )
     *
     * @param fragments fragments to be updated
     */
    private void updateFragmentIndex(List<Fragment> fragments) {
        int index = 0;
        String sourceName = null;
        for (Fragment fragment : fragments) {

            String currentSourceName = fragment.getSourceName();
            if (!currentSourceName.equals(sourceName)) {
                index = 0;
                sourceName = currentSourceName;
            }
            fragment.setIndex(index++);
        }
    }
}
