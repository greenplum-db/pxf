package org.greenplum.pxf.service.rest;

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
import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmenterCacheFactory;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.SessionId;
import org.greenplum.pxf.service.security.SecurityService;
import org.greenplum.pxf.service.utilities.AnalyzeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.greenplum.pxf.api.model.RequestContext.RequestType;

/**
 * Class enhances the API of the WEBHDFS REST server. Returns the data fragments
 * that a data resource is made of, enabling parallel processing of the data
 * resource. Example for querying API FRAGMENTER from a web client
 * {@code curl -i "http://localhost:51200/pxf/{version}/Fragmenter/getFragments"}
 * <code>/pxf/</code> is made part of the path when there is a webapp by that
 * name in tomcat.
 */
@RestController
@RequestMapping("/pxf/" + Version.PXF_PROTOCOL_VERSION + "/Fragmenter/")
public class FragmenterResource extends BaseResource {

    private ApplicationContext applicationContext;

    private FragmenterCacheFactory fragmenterCacheFactory;

    private PxfServerProperties pxfServerProperties;

    private SecurityService securityService;

    // Records the startTime of the fragmenter call
    private long startTime;

    // this flag is set to true when the thread processes the fragment call
    private boolean didThreadProcessFragmentCall;

    public FragmenterResource() {
        super(RequestType.FRAGMENTER);
    }

    @Autowired
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Autowired
    public void setFragmenterCacheFactory(FragmenterCacheFactory fragmenterCacheFactory) {
        this.fragmenterCacheFactory = fragmenterCacheFactory;
    }

    @Autowired
    public void setPxfServerProperties(PxfServerProperties pxfServerProperties) {
        this.pxfServerProperties = pxfServerProperties;
    }

    @Autowired
    public void setSecurityService(SecurityService securityService) {
        this.securityService = securityService;
    }

    /**
     * The function is called when
     * {@code http://host:port/pxf/{version}/Fragmenter/getFragments} is used.
     *
     * @param headers Holds HTTP headers from request
     * @return response object with JSON serialized fragments metadata
     * @throws Exception if getting fragments info failed
     */
    @GetMapping(value = "getFragments", produces = "application/json")
    public ResponseEntity<FragmentsResponse> getFragments(
            @RequestHeader MultiValueMap<String, String> headers) throws Throwable {

        LOG.debug("Received FRAGMENTER call");
        startTime = System.currentTimeMillis();
        final RequestContext context = parseRequest(headers);
        final String path = context.getDataSource();
        final String fragmenterCacheKey = getFragmenterCacheKey(context);

        if (LOG.isDebugEnabled() && pxfServerProperties.isMetadataCacheEnabled()) {
            LOG.debug("fragmentCache size={}, stats={}",
                    fragmenterCacheFactory.getCache().size(),
                    fragmenterCacheFactory.getCache().stats().toString());
        }

        LOG.debug("FRAGMENTER started for path \"{}\"", path);

        List<Fragment> fragments;

        if (pxfServerProperties.isMetadataCacheEnabled()) {
            try {
                // We can't support lambdas here because asm version doesn't support it
                fragments = fragmenterCacheFactory.getCache()
                        .get(fragmenterCacheKey, () -> {
                            didThreadProcessFragmentCall = true;
                            LOG.debug("Caching fragments for transactionId={} from segmentId={} with key={}",
                                    context.getTransactionId(), context.getSegmentId(), fragmenterCacheKey);
                            return getFragments(context);
                        });
            } catch (UncheckedExecutionException | ExecutionException e) {
                // Unwrap the error
                if (e.getCause() != null)
                    throw e.getCause();
                throw e;
            }

            if (!didThreadProcessFragmentCall) {
                logFragmentStatistics(Level.DEBUG, context, fragments);
            }
        } else {
            LOG.debug("Fragmenter cache is disabled");
            fragments = getFragments(context);
        }

        FragmentsResponse fragmentsResponse = FragmentsResponseFormatter.formatResponse(fragments, path);
        return new ResponseEntity<>(fragmentsResponse, HttpStatus.OK);
    }

    /**
     * The function is called when
     * {@code http://nn:port/pxf/{version}/Fragmenter/getFragmentsStats?path=...} is
     * used.
     *
     * @param headers Holds HTTP headers from request
     * @return response object with JSON serialized fragments statistics
     * @throws Exception if getting fragments info failed
     */
    @GetMapping(value = "getFragmentsStats", produces = "application/json")
    public ResponseEntity<String> getFragmentsStats(
            @RequestHeader MultiValueMap<String, String> headers) throws Exception {

        RequestContext context = parseRequest(headers);

        /* Create a fragmenter instance with API level parameters */
        final Fragmenter fragmenter = getFragmenter(context);

        FragmentStats fragmentStats = fragmenter.getFragmentStats();
        String response = FragmentStats.dataToJSON(fragmentStats);
        if (LOG.isDebugEnabled()) {
            LOG.debug(FragmentStats.dataToString(fragmentStats, context.getDataSource()));
        }

        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    private List<Fragment> getFragments(RequestContext context) throws Exception {
        PrivilegedExceptionAction<List<Fragment>> action = () ->
                getFragmenter(context).getFragments();

        List<Fragment> fragments = securityService.doAs(context, false, action);

        /* Create a fragmenter instance with API level parameters */
        fragments = AnalyzeUtils.getSampleFragments(fragments, context);
        logFragmentStatistics(Level.INFO, context, fragments);
        return fragments;
    }

    /**
     * Returns the fragmenter initialized with the request context
     *
     * @param context the request context
     * @return the fragmenter initialized with the request context
     */
    private Fragmenter getFragmenter(RequestContext context) {
        return applicationContext.getBean(Utilities.getShortClassName(context.getFragmenter()), Fragmenter.class);
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

}
