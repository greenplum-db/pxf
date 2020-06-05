package org.greenplum.pxf.service.rest;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.RequestContext.RequestType;
import org.greenplum.pxf.api.utilities.FragmenterCacheFactory;
import org.greenplum.pxf.service.FakeTicker;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.security.SecurityService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FragmenterResourceTest {

    private ApplicationContext mockApplicationContext;
    private MultiValueMap<String, String> mockRequestHeaders1;
    private MultiValueMap<String, String> mockRequestHeaders2;
    private Fragmenter fragmenter1;
    private Fragmenter fragmenter2;
    private Cache<String, List<Fragment>> fragmentCache;
    private FakeTicker fakeTicker;
    private RequestParser<MultiValueMap<String, String>> mockParser;
    private PxfServerProperties mockPxfServerProperties;
    private FragmenterResource fragmenterResource;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {

        mockParser = mock(RequestParser.class);
        mockApplicationContext = mock(ApplicationContext.class);
        FragmenterCacheFactory fragmenterCacheFactory = mock(FragmenterCacheFactory.class);
        mockRequestHeaders1 = mock(MultiValueMap.class);
        mockRequestHeaders2 = mock(MultiValueMap.class);
        fragmenter1 = mock(Fragmenter.class);
        fragmenter2 = mock(Fragmenter.class);
        mockPxfServerProperties = mock(PxfServerProperties.class);
        ConfigurationFactory mockConfigurationFactory = mock(ConfigurationFactory.class);

        when(mockConfigurationFactory.initConfiguration(any(), any(), any(), any())).thenReturn(new Configuration());

        fakeTicker = new FakeTicker();
        fragmentCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.SECONDS)
                .ticker(fakeTicker)
                .build();

        when(fragmenterCacheFactory.getCache()).thenReturn(fragmentCache);
        when(mockPxfServerProperties.isMetadataCacheEnabled()).thenReturn(true);

        FakeSecurityService fakeSecurityService = new FakeSecurityService();

        fragmenterResource = new FragmenterResource();
        fragmenterResource.setApplicationContext(mockApplicationContext);
        fragmenterResource.setFragmenterCacheFactory(fragmenterCacheFactory);
        fragmenterResource.setPxfServerProperties(mockPxfServerProperties);
        fragmenterResource.setConfigurationFactory(mockConfigurationFactory);
        fragmenterResource.setRequestParser(mockParser);
        fragmenterResource.setSecurityService(fakeSecurityService);
    }

    @Test
    public void getFragmentsResponseFromEmptyCache() throws Throwable {
        RequestContext context = new RequestContext();
        context.setTransactionId("XID-XYZ-123456");
        context.setSegmentId(0);
        context.setFragmenter(Fragmenter.class.getName());

        when(mockParser.parseRequest(mockRequestHeaders1, RequestType.FRAGMENTER)).thenReturn(context);
        when(mockApplicationContext.getBean("Fragmenter", Fragmenter.class)).thenReturn(fragmenter1);

        fragmenterResource.getFragments(mockRequestHeaders1);
        verify(fragmenter1, times(1)).getFragments();
    }


    @Test
    public void testFragmenterCallIsNotCachedForDifferentTransactions() throws Throwable {
        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setFragmenter("org.greenplum.pxf.api.model.Fragmenter1");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-654321");
        context2.setFragmenter("org.greenplum.pxf.api.model.Fragmenter2");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentDataSources() throws Throwable {
        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setDataSource("foo.bar");
        context1.setFilterString("a3c25s10d2016-01-03o6");
        context1.setFragmenter("org.greenplum.pxf.api.model.Fragmenter1");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setDataSource("bar.foo");
        context2.setFilterString("a3c25s10d2016-01-03o6");
        context2.setFragmenter("org.greenplum.pxf.api.model.Fragmenter2");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentFilters() throws Throwable {
        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setFilterString("a3c25s10d2016-01-03o6");
        context1.setFragmenter("org.greenplum.pxf.api.model.Fragmenter1");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setFilterString("a3c25s10d2016-01-03o2");
        context2.setFragmenter("org.greenplum.pxf.api.model.Fragmenter2");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedWhenCacheIsDisabled() throws Throwable {
        // Disable Fragmenter Cache
        when(mockPxfServerProperties.isMetadataCacheEnabled()).thenReturn(false);

        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setDataSource("foo.bar");
        context1.setFilterString("a3c25s10d2016-01-03o6");
        context1.setFragmenter("org.greenplum.pxf.api.model.Fragmenter1");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setDataSource("foo.bar");
        context2.setFilterString("a3c25s10d2016-01-03o6");
        context2.setFragmenter("org.greenplum.pxf.api.model.Fragmenter2");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void getSameFragmenterCallTwiceUsesCache() throws Throwable {
        List<Fragment> fragmentList = new ArrayList<>();

        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSegmentId(0);
        context1.setFragmenter("org.greenplum.pxf.api.model.Fragmenter1");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setSegmentId(1);
        context2.setFragmenter("org.greenplum.pxf.api.model.Fragmenter2");

        when(mockParser.parseRequest(mockRequestHeaders1, RequestType.FRAGMENTER)).thenReturn(context1);
        when(mockParser.parseRequest(mockRequestHeaders2, RequestType.FRAGMENTER)).thenReturn(context2);
        when(mockApplicationContext.getBean("Fragmenter1", Fragmenter.class)).thenReturn(fragmenter1);
        when(mockApplicationContext.getBean("Fragmenter2", Fragmenter.class)).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList);

        ResponseEntity<FragmentsResponse> response1 = fragmenterResource.getFragments(mockRequestHeaders1);
        ResponseEntity<FragmentsResponse> response2 = fragmenterResource.getFragments(mockRequestHeaders2);

        verify(fragmenter1, times(1)).getFragments();

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response1.getBody());
        assertNotNull(response2.getBody());

        assertSame(fragmentList, response1.getBody().getFragments());
        assertSame(fragmentList, response2.getBody().getFragments());
    }

    @Test
    public void testFragmenterCallExpiresAfterTimeout() throws Throwable {
        List<Fragment> fragmentList1 = new ArrayList<>();
        List<Fragment> fragmentList2 = new ArrayList<>();

        RequestContext context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSegmentId(0);
        context1.setFragmenter("org.greenplum.pxf.api.model.Fragmenter1");

        RequestContext context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-123456");
        context2.setSegmentId(1);
        context2.setFragmenter("org.greenplum.pxf.api.model.Fragmenter2");

        when(mockParser.parseRequest(mockRequestHeaders1, RequestType.FRAGMENTER)).thenReturn(context1);
        when(mockParser.parseRequest(mockRequestHeaders2, RequestType.FRAGMENTER)).thenReturn(context2);
        when(mockApplicationContext.getBean("Fragmenter1", Fragmenter.class)).thenReturn(fragmenter1);
        when(mockApplicationContext.getBean("Fragmenter2", Fragmenter.class)).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList1);
        when(fragmenter2.getFragments()).thenReturn(fragmentList2);

        ResponseEntity<FragmentsResponse> response1 = fragmenterResource.getFragments(mockRequestHeaders1);
        fakeTicker.advanceTime(11 * 1000);
        ResponseEntity<FragmentsResponse> response2 = fragmenterResource.getFragments(mockRequestHeaders2);

        verify(fragmenter1, times(1)).getFragments();
        verify(fragmenter2, times(1)).getFragments();
        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response1.getBody());
        assertNotNull(response2.getBody());

        // Checks for reference
        assertSame(fragmentList1, response1.getBody().getFragments());
        assertSame(fragmentList2, response2.getBody().getFragments());
    }

    @Test
    public void testMultiThreadedAccessToFragments() throws Throwable {
        final AtomicInteger finishedCount = new AtomicInteger();

        int threadCount = 100;
        Thread[] threads = new Thread[threadCount];
        final Fragmenter fragmenter = mock(Fragmenter.class);
        when(mockApplicationContext.getBean("Fragmenter", Fragmenter.class)).thenReturn(fragmenter);

        for (int i = 0; i < threads.length; i++) {
            int index = i;
            threads[i] = new Thread(() -> {

                MultiValueMap<String, String> httpHeaders = new LinkedMultiValueMap<>();

                final RequestContext context = new RequestContext();
                context.setTransactionId("XID-MULTI_THREADED-123456");
                context.setSegmentId(index % 10);
                context.setFragmenter("org.greenplum.pxf.api.model.Fragmenter");

                when(mockParser.parseRequest(httpHeaders, RequestType.FRAGMENTER)).thenReturn(context);

                try {
                    fragmenterResource.getFragments(httpHeaders);

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

        // From the CacheBuilder documentation:
        // Expired entries may be counted in {@link Cache#size}, but will never be visible to read or
        // write operations. Expired entries are cleaned up as part of the routine maintenance described
        // in the class javadoc
        assertEquals(1, fragmentCache.size());
        // advance time one second force a cache clean up.
        // Cache retains the entry
        fakeTicker.advanceTime(1000);
        fragmentCache.cleanUp();
        assertEquals(1, fragmentCache.size());
        // advance 10 seconds and force a clean up
        // cache should be clean now
        fakeTicker.advanceTime(10 * 1000);
        fragmentCache.cleanUp();
        assertEquals(0, fragmentCache.size());
    }

    private void testContextsAreNotCached(RequestContext context1, RequestContext context2)
            throws Throwable {

        List<Fragment> fragmentList1 = new ArrayList<>();
        List<Fragment> fragmentList2 = new ArrayList<>();

        when(mockParser.parseRequest(mockRequestHeaders1, RequestType.FRAGMENTER)).thenReturn(context1);
        when(mockParser.parseRequest(mockRequestHeaders2, RequestType.FRAGMENTER)).thenReturn(context2);
        when(mockApplicationContext.getBean("Fragmenter1", Fragmenter.class)).thenReturn(fragmenter1);
        when(mockApplicationContext.getBean("Fragmenter2", Fragmenter.class)).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList1);
        when(fragmenter2.getFragments()).thenReturn(fragmentList2);

        ResponseEntity<FragmentsResponse> response1 = fragmenterResource.getFragments(mockRequestHeaders1);
        ResponseEntity<FragmentsResponse> response2 = fragmenterResource.getFragments(mockRequestHeaders2);

        verify(fragmenter1, times(1)).getFragments();
        verify(fragmenter2, times(1)).getFragments();

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response1.getBody());
        assertNotNull(response2.getBody());

        assertSame(fragmentList1, response1.getBody().getFragments());
        assertSame(fragmentList2, response2.getBody().getFragments());

        if (mockPxfServerProperties.isMetadataCacheEnabled()) {
            assertEquals(2, fragmentCache.size());
            // advance time one second force a cache clean up.
            // Cache retains the entry
            fakeTicker.advanceTime(1000);
            fragmentCache.cleanUp();
            assertEquals(2, fragmentCache.size());
            // advance 10 seconds and force a clean up
            // cache should be clean now
            fakeTicker.advanceTime(10 * 1000);
            fragmentCache.cleanUp();
        }
        assertEquals(0, fragmentCache.size());
    }

    private static class FakeSecurityService implements SecurityService {

        @Override
        public <T> T doAs(RequestContext context, boolean lastCallForSegment, PrivilegedExceptionAction<T> action) throws IOException, InterruptedException {
            return UserGroupInformation.getCurrentUser().doAs(action);
        }
    }
}
