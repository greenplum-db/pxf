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
import org.apache.log4j.Level;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.StreamingFragmenter;
import org.greenplum.pxf.api.utilities.FragmenterCacheFactory;
import org.greenplum.pxf.api.utilities.FragmenterFactory;
import org.greenplum.pxf.api.utilities.FragmentsResponseFormatter;
import org.greenplum.pxf.api.utilities.SimpleFragmentsResponse;
import org.greenplum.pxf.api.utilities.StreamingFragmentsResponse;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.HttpRequestParser;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.SessionId;
import org.greenplum.pxf.service.utilities.AnalyzeUtils;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.Callable;
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
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Fragmenter/")
public class FragmenterResource extends BaseResource {

    private FragmenterFactory fragmenterFactory;

    private FragmenterCacheFactory fragmenterCacheFactory;

    // Records the startTime of the fragmenter call
    private long startTime;

    // this flag is set to true when the thread processes the fragment call
    private boolean didThreadProcessFragmentCall;

    public FragmenterResource() {
        this(HttpRequestParser.getInstance(), FragmenterFactory.getInstance(), FragmenterCacheFactory.getInstance());
    }

    FragmenterResource(RequestParser<HttpHeaders> parser,
                       FragmenterFactory fragmenterFactory,
                       FragmenterCacheFactory fragmenterCacheFactory) {
        super(RequestType.FRAGMENTER, parser);
        this.fragmenterFactory = fragmenterFactory;
        this.fragmenterCacheFactory = fragmenterCacheFactory;
        if (LOG.isDebugEnabled() && Utilities.isFragmenterCacheEnabled()) {
            LOG.debug("fragmentCache size={}, stats={}",
                    fragmenterCacheFactory.getCache().size(),
                    fragmenterCacheFactory.getCache().stats().toString());
        }
    }

    /**
     * The function is called when
     * {@code http://host:port/pxf/{version}/Fragmenter/getFragments} is used.
     *
     * @param servletContext Servlet context contains attributes required by
     *                       SecuredHDFS
     * @param headers        Holds HTTP headers from request
     * @return response object with JSON serialized fragments metadata
     * @throws Exception if getting fragments info failed
     */
    @GET
    @Path("getFragments")
    @Produces("application/json")
    public Response getFragments(@Context final ServletContext servletContext,
                                 @Context final HttpHeaders headers)
            throws Throwable {

        LOG.debug("Received FRAGMENTER call");
        startTime = System.currentTimeMillis();
        final RequestContext context = parseRequest(headers);
        final String path = context.getDataSource();
        final String fragmenterCacheKey = getFragmenterCacheKey(context);

        LOG.debug("FRAGMENTER started for path \"{}\"", path);

        List<Fragment> fragments;

        Fragmenter fragmenter = fragmenterFactory.getPlugin(context);
        if (fragmenter != null && StreamingFragmenter.class.isAssignableFrom(fragmenter.getClass())) {
            return Response.ok(new StreamingFragmentsResponse((StreamingFragmenter) fragmenter),
                    MediaType.APPLICATION_JSON_TYPE).build();
        } else if (Utilities.isFragmenterCacheEnabled()) {
            try {
                // We can't support lambdas here because asm version doesn't support it
                fragments = fragmenterCacheFactory.getCache()
                        .get(fragmenterCacheKey, new Callable<List<Fragment>>() {
                            @Override
                            public List<Fragment> call() throws Exception {
                                didThreadProcessFragmentCall = true;
                                LOG.debug("Caching fragments for transactionId={} from segmentId={} with key={}",
                                        context.getTransactionId(), context.getSegmentId(), fragmenterCacheKey);
                                return getFragments(context);
                            }
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

        SimpleFragmentsResponse simpleFragmentsResponse = (SimpleFragmentsResponse) FragmentsResponseFormatter.formatSimpleFragmentsResponse(fragments, path);
        return Response.ok(simpleFragmentsResponse, MediaType.APPLICATION_JSON_TYPE).build();
    }

    /**
     * The function is called when
     * {@code http://nn:port/pxf/{version}/Fragmenter/getFragmentsStats?path=...} is
     * used.
     *
     * @param servletContext Servlet context contains attributes required by
     *                       SecuredHDFS
     * @param headers        Holds HTTP headers from request
     * @param path           Holds URI path option used in this request
     * @return response object with JSON serialized fragments statistics
     * @throws Exception if getting fragments info failed
     */
    @GET
    @Path("getFragmentsStats")
    @Produces("application/json")
    public Response getFragmentsStats(@Context final ServletContext servletContext,
                                      @Context final HttpHeaders headers,
                                      @QueryParam("path") final String path)
            throws Exception {

        RequestContext context = parseRequest(headers);

        /* Create a fragmenter instance with API level parameters */
        final Fragmenter fragmenter = fragmenterFactory.getPlugin(context);

        FragmentStats fragmentStats = fragmenter.getFragmentStats();
        String response = FragmentStats.dataToJSON(fragmentStats);
        if (LOG.isDebugEnabled()) {
            LOG.debug(FragmentStats.dataToString(fragmentStats, path));
        }

        return Response.ok(response, MediaType.APPLICATION_JSON_TYPE).build();
    }

    private List<Fragment> getFragments(RequestContext context) throws Exception {
        /* Create a fragmenter instance with API level parameters */
        List<Fragment> fragments = AnalyzeUtils.getSampleFragments(fragmenterFactory.getPlugin(context).getFragments(), context);

        logFragmentStatistics(Level.INFO, context, fragments);
        return fragments;
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
