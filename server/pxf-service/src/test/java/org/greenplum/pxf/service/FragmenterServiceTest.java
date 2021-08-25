package org.greenplum.pxf.service;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.examples.DemoFragmentMetadata;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmenterCacheFactory;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FragmenterServiceTest {

    private BasePluginFactory mockPluginFactory;
    private Fragmenter fragmenter1;
    private Fragmenter fragmenter2;
    private Cache<String, List<Fragment>> fragmentCache;
    private FakeTicker fakeTicker;
    private FragmenterService fragmenterService;
    private Configuration configuration;

    private RequestContext context1;
    private RequestContext context2;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
        context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setFragmenter("org.greenplum.pxf.api.model.Fragmenter1");
        context1.setSegmentId(0);
        context1.setGpCommandCount(1);
        context1.setGpSessionId(1);
        context1.setTotalSegments(1);
        context1.setDataSource("path.A");
        context1.setConfiguration(configuration);

        context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-654321");
        context2.setFragmenter("org.greenplum.pxf.api.model.Fragmenter2");
        context2.setSegmentId(0);
        context2.setGpCommandCount(1);
        context2.setGpSessionId(1);
        context2.setTotalSegments(1);
        context2.setDataSource("path.A");
        context2.setConfiguration(configuration);

        mockPluginFactory = mock(BasePluginFactory.class);
        FragmenterCacheFactory fragmenterCacheFactory = mock(FragmenterCacheFactory.class);
        fragmenter1 = mock(Fragmenter.class);
        fragmenter2 = mock(Fragmenter.class);

        fakeTicker = new FakeTicker();
        fragmentCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.SECONDS)
                .ticker(fakeTicker)
                .build();

        when(fragmenterCacheFactory.getCache()).thenReturn(fragmentCache);

        // use a real handler to ensure pass-through calls on default configuration
        fragmenterService = new FragmenterService(fragmenterCacheFactory,
                mockPluginFactory, new GSSFailureHandler());
    }

    @Test
    public void getFragmentsResponseFromEmptyCache() throws Throwable {
        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);

        fragmenterService.getFragmentsForSegment(context1);
        verify(fragmenter1, times(1)).getFragments();
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentTransactions() throws Throwable {
        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentSchemas() throws Throwable {
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSchemaName("foo1");
        context1.setTableName("bar");
        context1.setDataSource("path");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        context2.setTransactionId("XID-XYZ-123456");
        context2.setSchemaName("foo2");
        context2.setTableName("bar");
        context2.setDataSource("path");
        context2.setFilterString("a3c25s10d2016-01-03o6");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentTables() throws Throwable {
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSchemaName("foo");
        context1.setTableName("bar1");
        context1.setDataSource("path");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        context2.setTransactionId("XID-XYZ-123456");
        context2.setSchemaName("foo");
        context2.setTableName("bar2");
        context2.setDataSource("path");
        context2.setFilterString("a3c25s10d2016-01-03o6");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentDataSources() throws Throwable {
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSchemaName("foo");
        context1.setTableName("bar");
        context1.setDataSource("path1");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        context2.setTransactionId("XID-XYZ-123456");
        context2.setSchemaName("foo");
        context2.setTableName("bar");
        context2.setDataSource("path2");
        context2.setFilterString("a3c25s10d2016-01-03o6");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentFilters() throws Throwable {
        context1.setTransactionId("XID-XYZ-123456");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        context2.setTransactionId("XID-XYZ-123456");
        context2.setFilterString("a3c25s10d2016-01-03o2");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallForTwoSegments() throws Throwable {

        List<Fragment> fragmentList = Arrays.asList(
                new Fragment("foo.bar", new DemoFragmentMetadata()),
                new Fragment("bar.foo", new DemoFragmentMetadata()),
                new Fragment("foobar", new DemoFragmentMetadata()),
                new Fragment("barfoo", new DemoFragmentMetadata())
        );

        context1.setTransactionId("XID-XYZ-123456");
        context1.setSegmentId(0);
        context1.setTotalSegments(2);

        context2.setTransactionId("XID-XYZ-123456");
        context2.setSegmentId(1);
        context2.setTotalSegments(2);

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);
        when(mockPluginFactory.getPlugin(context2, context2.getFragmenter())).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList);

        List<Fragment> response1 = fragmenterService.getFragmentsForSegment(context1);
        List<Fragment> response2 = fragmenterService.getFragmentsForSegment(context2);

        verify(fragmenter1, times(1)).getFragments();

        assertEquals(2, response1.size());
        assertEquals("foo.bar", response1.get(0).getSourceName());
        assertEquals("foobar", response1.get(1).getSourceName());

        assertEquals(2, response2.size());
        assertEquals("bar.foo", response2.get(0).getSourceName());
        assertEquals("barfoo", response2.get(1).getSourceName());
    }

    @Test
    public void getSameFragmenterCallTwiceUsesCache() throws Throwable {
        List<Fragment> fragmentList = new ArrayList<>();

        context1.setTransactionId("XID-XYZ-123456");
        context2.setTransactionId("XID-XYZ-123456");

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);
        when(mockPluginFactory.getPlugin(context2, context2.getFragmenter())).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList);

        List<Fragment> response1 = fragmenterService.getFragmentsForSegment(context1);
        List<Fragment> response2 = fragmenterService.getFragmentsForSegment(context2);

        verify(fragmenter1, times(1)).getFragments();

        assertEquals(fragmentList, response1);
        assertEquals(fragmentList, response2);
    }

    @Test
    public void testFragmenterCallExpiresAfterTimeout() throws Throwable {
        List<Fragment> fragmentList1 = new ArrayList<>();
        List<Fragment> fragmentList2 = new ArrayList<>();

        context1.setTransactionId("XID-XYZ-123456");
        context2.setTransactionId("XID-XYZ-123456");

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);
        when(mockPluginFactory.getPlugin(context2, context2.getFragmenter())).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList1);
        when(fragmenter2.getFragments()).thenReturn(fragmentList2);

        List<Fragment> response1 = fragmenterService.getFragmentsForSegment(context1);
        fakeTicker.advanceTime(11 * 1000);
        List<Fragment> response2 = fragmenterService.getFragmentsForSegment(context2);

        verify(fragmenter1, times(1)).getFragments();
        verify(fragmenter2, times(1)).getFragments();
        // Checks for reference
        assertEquals(fragmentList1, response1);
        assertEquals(fragmentList2, response2);
    }

    @Test
    public void testMultiThreadedAccessToFragments() throws Throwable {
        final AtomicInteger finishedCount = new AtomicInteger();

        int threadCount = 100;
        Thread[] threads = new Thread[threadCount];
        final Fragmenter fragmenter = mock(Fragmenter.class);
        when(mockPluginFactory.getPlugin(any(), any())).thenReturn(fragmenter);

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {

                try {
                    fragmenterService.getFragmentsForSegment(context1);
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

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);
        when(mockPluginFactory.getPlugin(context2, context2.getFragmenter())).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList1);
        when(fragmenter2.getFragments()).thenReturn(fragmentList2);

        List<Fragment> response1 = fragmenterService.getFragmentsForSegment(context1);
        List<Fragment> response2 = fragmenterService.getFragmentsForSegment(context2);

        verify(fragmenter1, times(1)).getFragments();
        verify(fragmenter2, times(1)).getFragments();

        assertEquals(fragmentList1, response1);
        assertEquals(fragmentList2, response2);

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
        assertEquals(0, fragmentCache.size());
    }

    private static class FakeTicker extends Ticker {

        static final int NANOS_PER_MILLIS = 1000000;
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        public void advanceTime(long milliseconds) {
            nanos.addAndGet(milliseconds * NANOS_PER_MILLIS);
        }
    }
}