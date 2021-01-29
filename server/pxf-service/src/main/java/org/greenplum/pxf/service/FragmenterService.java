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
import org.apache.logging.log4j.Level;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmenterCacheFactory;
import org.greenplum.pxf.service.SessionId;
import org.greenplum.pxf.service.security.SecurityService;
import org.greenplum.pxf.service.utilities.AnalyzeUtils;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Class enhances the API of the WEBHDFS REST server. Returns the data fragments
 * that a data resource is made of, enabling parallel processing of the data
 * resource. Example for querying API FRAGMENTER from a web client
 * {@code curl -i "http://localhost:51200/pxf/{version}/Fragmenter/getFragments"}
 * <code>/pxf/</code> is made part of the path when there is a webapp by that
 * name in tomcat.
 */
@Component
public class FragmenterService {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final BasePluginFactory pluginFactory;

    private final SecurityService securityService;

    private final FragmenterCacheFactory fragmenterCacheFactory;

    // Records the startTime of the fragmenter call
    private long startTime;

    // this flag is set to true when the thread processes the fragment call
    private boolean didThreadProcessFragmentCall;

    public FragmenterService(FragmenterCacheFactory fragmenterCacheFactory,
                             BasePluginFactory pluginFactory,
                             SecurityService securityService) {
        this.fragmenterCacheFactory = fragmenterCacheFactory;
        this.pluginFactory = pluginFactory;
        this.securityService = securityService;
    }

    public List<Fragment> getFragmentsForSegment(RequestContext context) throws Throwable {

        LOG.debug("Received FRAGMENTER call");
        startTime = System.currentTimeMillis();
        final String path = context.getDataSource();
        final String fragmenterCacheKey = getFragmenterCacheKey(context);

        if (LOG.isDebugEnabled()) {
            LOG.debug("fragmentCache size={}, stats={}",
                    fragmenterCacheFactory.getCache().size(),
                    fragmenterCacheFactory.getCache().stats().toString());
        }

        LOG.debug("FRAGMENTER started for path \"{}\"", path);

        List<Fragment> fragments;

        try {
            // We can't support lambdas here because asm version doesn't support it
            fragments = fragmenterCacheFactory.getCache()
                    .get(fragmenterCacheKey, () -> {
                        didThreadProcessFragmentCall = true;
                        LOG.debug("Caching fragments for transactionId={} from segmentId={} with key={}",
                                context.getTransactionId(), context.getSegmentId(), fragmenterCacheKey);
                        PrivilegedExceptionAction<List<Fragment>> action = () ->
                                getFragmenter(context).getFragments();

                        List<Fragment> fragmentList = securityService.doAs(context, false, action);

                        /* Create a fragmenter instance with API level parameters */
                        fragmentList = AnalyzeUtils.getSampleFragments(fragmentList, context);
                        updateFragmentIndex(fragmentList);
                        return fragmentList;
                    });
        } catch (UncheckedExecutionException | ExecutionException e) {
            // Unwrap the error
            if (e.getCause() != null)
                throw e.getCause();
            throw e;
        }

        logFragmentStatistics(didThreadProcessFragmentCall ? Level.INFO : Level.DEBUG,
                context, fragments);

        return filterFragments(fragments, context);
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
     * @param fragments the list of fragments
     * @param context   the request context
     * @return the filtered list of fragments for the given segment
     */
    private List<Fragment> filterFragments(List<Fragment> fragments, RequestContext context) {

        if (context.getGpSessionId() == null || context.getGpCommandCount() == null) {
            throw new PxfRuntimeException("Using an incompatible PXF extension with this server.",
                    "Please make sure the correct PXF extension is installed on your Greenplum cluster");
        }

        int gpSessionId = context.getGpSessionId();
        int gpCommandCount = context.getGpCommandCount();
        int totalSegments = context.getTotalSegments();
        int shift = gpSessionId % totalSegments;
        int fragmentCount = fragments.size();

        List<Fragment> filteredFragments = new ArrayList<>((int) Math.ceil(fragmentCount / totalSegments));
        for (int i = 0; i < fragmentCount; i++) {
            if (context.getSegmentId() == ((i + shift + gpCommandCount) % totalSegments)) {
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

    private void logFragmentStatistics(Level level, RequestContext context, List<Fragment> fragments) {

        int numberOfFragments = fragments.size();
        SessionId session = new SessionId(context.getSegmentId(), context.getTransactionId(), context.getUser(), context.getServerName());
        long elapsedMillis = System.currentTimeMillis() - startTime;

        if (level == Level.INFO) {
            LOG.info("{} returns {} fragment{} for path {} in {} ms for {} [profile {} filter is{} available]",
                    context.getFragmenter(), numberOfFragments, numberOfFragments == 1 ? "" : "s",
                    context.getDataSource(), elapsedMillis, session, context.getProfile(), context.hasFilter() ? "" : " not");
        } else if (level == Level.DEBUG) {
            LOG.debug("{} returns {} fragment{} for path {} in {} ms for {} [profile {} filter is{} available]",
                    context.getFragmenter(), numberOfFragments, numberOfFragments == 1 ? "" : "s",
                    context.getDataSource(), elapsedMillis, session, context.getProfile(), context.hasFilter() ? "" : " not");
        }
    }

    /**
     * Updates the fragments' indexes so that it is incremented by sourceName.
     * (E.g.: {"a", 0}, {"a", 1}, {"b", 0} ... )
     *
     * @param fragments fragments to be updated
     */
    private void updateFragmentIndex(List<Fragment> fragments) {

        String sourceName = null;
        int index = 0;
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
