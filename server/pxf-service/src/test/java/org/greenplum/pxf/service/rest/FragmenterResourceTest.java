package org.greenplum.pxf.service.rest;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmenterCacheFactory;
import org.greenplum.pxf.api.utilities.FragmenterFactory;
import org.greenplum.pxf.api.utilities.FragmentsResponse;
import org.greenplum.pxf.service.FakeTicker;
import org.greenplum.pxf.service.RequestParser;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.ServletContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class FragmenterResourceTest {

    private RequestParser parser;
    private FragmenterFactory fragmenterFactory;
    private FragmenterCacheFactory fragmenterCacheFactory;
    private ServletContext servletContext;
    private HttpHeaders headersFromRequest1;
    private HttpHeaders headersFromRequest2;
    private Fragmenter fragmenter1;
    private Fragmenter fragmenter2;
    private Cache<String, List<Fragment>> fragmentCache;
    private FakeTicker fakeTicker;

    private String PROPERTY_KEY_FRAGMENTER_CACHE = "pxf.service.fragmenter.cache.enabled";

    @Before
    public void setup() {
        parser = mock(RequestParser.class);
        fragmenterFactory = mock(FragmenterFactory.class);
        fragmenterCacheFactory = mock(FragmenterCacheFactory.class);
        servletContext = mock(ServletContext.class);
        headersFromRequest1 = mock(HttpHeaders.class);
        headersFromRequest2 = mock(HttpHeaders.class);
        fragmenter1 = mock(Fragmenter.class);
        fragmenter2 = mock(Fragmenter.class);

        fakeTicker = new FakeTicker();
        fragmentCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.SECONDS)
                .ticker(fakeTicker)
                .build();

        when(fragmenterCacheFactory.getCache()).thenReturn(fragmentCache);
        System.clearProperty(PROPERTY_KEY_FRAGMENTER_CACHE);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void getFragmentsResponseFromEmptyCache() throws Throwable {
        RequestContext context = new RequestContext();
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(0);

        when(parser.parseRequest(headersFromRequest1)).thenReturn(context);
        when(fragmenterFactory.getPlugin(context)).thenReturn(fragmenter1);

        new FragmenterResource(parser, fragmenterFactory, fragmenterCacheFactory)
                .getFragments(servletContext, headersFromRequest1, "/foo/bar");
        verify(fragmenter1).getFragments();
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentTransactions() throws Throwable {
        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-654321");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentDataSources() throws Throwable {
        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setDataSource("foo.bar");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setDataSource("bar.foo");
        context2.setFilterString("a3c25s10d2016-01-03o6");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentFilters() throws Throwable {
        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setFilterString("a3c25s10d2016-01-03o2");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedWhenCacheIsDisabled() throws Throwable {
        // Disable Fragmenter Cache
        System.setProperty(PROPERTY_KEY_FRAGMENTER_CACHE, "false");

        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setDataSource("foo.bar");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setDataSource("foo.bar");
        context2.setFilterString("a3c25s10d2016-01-03o6");

        testContextsAreNotCached(context1, context2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void getSameFragmenterCallTwiceUsesCache() throws Throwable {
        List<Fragment> fragmentList = new ArrayList<>();

        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSegmentId(0);

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setSegmentId(1);

        when(parser.parseRequest(headersFromRequest1)).thenReturn(context1);
        when(parser.parseRequest(headersFromRequest2)).thenReturn(context2);
        when(fragmenterFactory.getPlugin(context1)).thenReturn(fragmenter1);
        when(fragmenterFactory.getPlugin(context2)).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList);

        Response response1 = new FragmenterResource(parser, fragmenterFactory, fragmenterCacheFactory)
                .getFragments(servletContext, headersFromRequest1, "/foo/bar");
        Response response2 = new FragmenterResource(parser, fragmenterFactory, fragmenterCacheFactory)
                .getFragments(servletContext, headersFromRequest2, "/foo/bar");

        verify(fragmenter1, times(1)).getFragments();
        verifyZeroInteractions(fragmenter2);
        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response1.getEntity());
        assertNotNull(response2.getEntity());
        assertTrue(response1.getEntity() instanceof FragmentsResponse);
        assertTrue(response2.getEntity() instanceof FragmentsResponse);

        assertSame(fragmentList, ((FragmentsResponse) response1.getEntity()).getFragments());
        assertSame(fragmentList, ((FragmentsResponse) response2.getEntity()).getFragments());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFragmenterCallExpiresAfterTimeout() throws Throwable {
        List<Fragment> fragmentList1 = new ArrayList<>();
        List<Fragment> fragmentList2 = new ArrayList<>();

        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSegmentId(0);

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setSegmentId(1);

        when(parser.parseRequest(headersFromRequest1)).thenReturn(context1);
        when(parser.parseRequest(headersFromRequest2)).thenReturn(context2);
        when(fragmenterFactory.getPlugin(context1)).thenReturn(fragmenter1);
        when(fragmenterFactory.getPlugin(context2)).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList1);
        when(fragmenter2.getFragments()).thenReturn(fragmentList2);

        Response response1 = new FragmenterResource(parser, fragmenterFactory, fragmenterCacheFactory)
                .getFragments(servletContext, headersFromRequest1, "/foo/bar");
        fakeTicker.advanceTime(11 * 1000);
        Response response2 = new FragmenterResource(parser, fragmenterFactory, fragmenterCacheFactory)
                .getFragments(servletContext, headersFromRequest2, "/foo/bar");

        verify(fragmenter1, times(1)).getFragments();
        verify(fragmenter2, times(1)).getFragments();
        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response1.getEntity());
        assertNotNull(response2.getEntity());
        assertTrue(response1.getEntity() instanceof FragmentsResponse);
        assertTrue(response2.getEntity() instanceof FragmentsResponse);

        // Checks for reference
        assertSame(fragmentList1, ((FragmentsResponse) response1.getEntity()).getFragments());
        assertSame(fragmentList2, ((FragmentsResponse) response2.getEntity()).getFragments());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiThreadedAccessToFragments() throws Throwable {
        final AtomicInteger finishedCount = new AtomicInteger();

        int threadCount = 100;
        Thread[] threads = new Thread[threadCount];
        final Fragmenter fragmenter = mock(Fragmenter.class);

        for (int i = 0; i < threads.length; i++) {
            int index = i;
            threads[i] = new Thread(() -> {

                RequestParser requestParser = mock(RequestParser.class);
                HttpHeaders httpHeaders = mock(HttpHeaders.class);
                FragmenterFactory factory = mock(FragmenterFactory.class);
                FragmenterCacheFactory cacheFactory = mock(FragmenterCacheFactory.class);

                final RequestContext context = new RequestContext();
                context.setTransactionId("XID-MULTI_THREADED-123456");
                context.setSegmentId(index % 10);

                when(cacheFactory.getCache()).thenReturn(fragmentCache);
                when(requestParser.parseRequest(httpHeaders)).thenReturn(context);
                when(factory.getPlugin(context)).thenReturn(fragmenter);

                try {
                    new FragmenterResource(requestParser, factory, cacheFactory)
                            .getFragments(servletContext, httpHeaders, "/foo/bar/" + index);

                    finishedCount.incrementAndGet();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        verify(fragmenter, times(1)).getFragments();
        assertEquals(threadCount, finishedCount.intValue());
        fakeTicker.advanceTime(1000 * 1000);

        // From the CacheBuilder documentation:
        // Expired entries may be counted in {@link Cache#size}, but will never be visible to read or
        // write operations. Expired entries are cleaned up as part of the routine maintenance described
        // in the class javadoc
        assertTrue(fragmentCache.size() <= 1);
    }

    @SuppressWarnings("unchecked")
    private void testContextsAreNotCached(RequestContext context1, RequestContext context2)
            throws Throwable {

        List<Fragment> fragmentList1 = new ArrayList<>();
        List<Fragment> fragmentList2 = new ArrayList<>();

        when(parser.parseRequest(headersFromRequest1)).thenReturn(context1);
        when(parser.parseRequest(headersFromRequest2)).thenReturn(context2);
        when(fragmenterFactory.getPlugin(context1)).thenReturn(fragmenter1);
        when(fragmenterFactory.getPlugin(context2)).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList1);
        when(fragmenter2.getFragments()).thenReturn(fragmentList2);

        Response response1 = new FragmenterResource(parser, fragmenterFactory, fragmenterCacheFactory)
                .getFragments(servletContext, headersFromRequest1, "/foo/bar");
        Response response2 = new FragmenterResource(parser, fragmenterFactory, fragmenterCacheFactory)
                .getFragments(servletContext, headersFromRequest2, "/bar/foo");

        verify(fragmenter1, times(1)).getFragments();
        verify(fragmenter2, times(1)).getFragments();

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response1.getEntity());
        assertNotNull(response2.getEntity());
        assertTrue(response1.getEntity() instanceof FragmentsResponse);
        assertTrue(response2.getEntity() instanceof FragmentsResponse);

        assertSame(fragmentList1, ((FragmentsResponse) response1.getEntity()).getFragments());
        assertSame(fragmentList2, ((FragmentsResponse) response2.getEntity()).getFragments());
    }
}