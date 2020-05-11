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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UGICacheTest {
    private static final long MINUTES = 60 * 1000L;
    private UGIProvider provider = null;
    private SessionId session = null;
    private UGICache cache = null;
    private FakeTicker fakeTicker;

    @BeforeEach
    public void setUp() throws Exception {
        provider = mock(UGIProvider.class);
        session = new SessionId(0, "txn-id", "the-user", "default", new Configuration(), UserGroupInformation.getLoginUser());
        fakeTicker = new FakeTicker();
        cache = new UGICache(provider, fakeTicker);

        when(provider.createProxyUGI(any(String.class), any(UserGroupInformation.class)))
                .thenAnswer((Answer<UserGroupInformation>) invocation -> mock(UserGroupInformation.class));

        when(provider.createRemoteUser(any(String.class), any(SessionId.class)))
                .thenAnswer((Answer<UserGroupInformation>) invocation -> mock(UserGroupInformation.class));
    }

    @Test
    public void getUGIFromEmptyCache() throws Exception {
        UserGroupInformation ugi = cache.getUserGroupInformation(session, false);
        assertNotNull(ugi);
        verify(provider).createRemoteUser("the-user", session);
    }

    @Test
    public void getProxyUGIFromEmptyCache() throws Exception {
        UserGroupInformation ugi = cache.getUserGroupInformation(session, true);
        assertNotNull(ugi);
        verify(provider).createProxyUGI("the-user", UserGroupInformation.getLoginUser());
    }

    @Test
    public void getSameUGITwiceUsesCache() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, false);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session, false);
        assertEquals(ugi1, ugi2);
        verify(provider, times(1)).createRemoteUser("the-user", session);
        assertCacheSize(1);
    }

    @Test
    public void getSameProxyUGITwiceUsesCache() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session, true);
        assertEquals(ugi1, ugi2);
        verify(provider, times(1)).createProxyUGI("the-user", UserGroupInformation.getLoginUser());
        assertCacheSize(1);
    }

    @Test
    public void getUGIWithEquivalentSessionsReturnsTheSameInstance() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user", "default");
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, false);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session2, false);
        assertEquals(ugi1, ugi2);
    }

    @Test
    public void getProxyUGIWithEquivalentSessionsReturnsTheSameInstance() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user", "default");
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session2, true);
        assertEquals(ugi1, ugi2);
    }

    @Test
    public void getTwoUGIsWithDifferentTransactionsForSameUser() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id-2", "the-user", "default", new Configuration(), UserGroupInformation.getLoginUser());
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, false);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(otherSession, false);
        assertNotEquals(ugi1, ugi2);
        verify(provider, times(2)).createRemoteUser(eq("the-user"), any(SessionId.class));
        verify(provider, times(0)).createProxyUGI(eq("the-user"), any(UserGroupInformation.class));
        assertCacheSize(2);
    }

    @Test
    public void getTwoProxyUGIsWithDifferentTransactionsForSameUser() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id-2", "the-user", "default", new Configuration(), UserGroupInformation.getLoginUser());
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session, true);
        UserGroupInformation proxyUGI2 = cache.getUserGroupInformation(otherSession, true);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(2)).createProxyUGI("the-user", UserGroupInformation.getLoginUser());
        assertCacheSize(2);
        // getting a new UGI instance for each transaction ID is not strictly necessary, but allows
        // us to expire UGIs for transactions that have finished. If we reused one UGI per user,
        // it might never get to expire from the cache, and eventually Kerberos might invalidate
        // the UGI on its end.
    }

    @Test
    public void getTwoUGIsWithDifferentUsers() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id", "different-user", "default", new Configuration(), UserGroupInformation.getLoginUser());
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, false);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(otherSession, false);
        assertNotEquals(ugi1, ugi2);
        verify(provider, times(1)).createRemoteUser(eq("the-user"), any(SessionId.class));
        verify(provider, times(1)).createRemoteUser(eq("different-user"), any(SessionId.class));
        assertCacheSize(2);
        assertStillInCache(session, ugi1);
        assertStillInCache(otherSession, ugi2);
    }

    @Test
    public void getTwoProxyUGIsWithDifferentUsers() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id", "different-user", "default", new Configuration(), UserGroupInformation.getLoginUser());
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session, true);
        UserGroupInformation proxyUGI2 = cache.getUserGroupInformation(otherSession, true);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(1)).createProxyUGI("the-user", UserGroupInformation.getLoginUser());
        verify(provider, times(1)).createProxyUGI("different-user", UserGroupInformation.getLoginUser());
        assertCacheSize(2);
        assertStillInCache(session, proxyUGI1);
        assertStillInCache(otherSession, proxyUGI2);
    }

    @Test
    public void anySegmentIdIsValid() throws Exception {

        UserGroupInformation mockProxyUGI = mock(UserGroupInformation.class);
        when(provider.createProxyUGI("the-user", null)).thenReturn(mockProxyUGI);

        int crazySegId = Integer.MAX_VALUE;
        session = new SessionId(crazySegId, "txn-id", "the-user", "default");
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session, true);
        assertSame(mockProxyUGI, proxyUGI1);
        assertStillInCache(session, proxyUGI1);
    }

    @Test
    public void ensureCleanUpAfterExpiration() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);
        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        SessionId session2 = new SessionId(0, "txn-id", "the-user-2", "default");
        cache.getUserGroupInformation(session2, true); // this triggers cleanup of ugi1
        assertNoLongerInCache(session, ugi1);
        cache.release(session2, true);
        assertCacheSize(0);
    }

    @Test
    public void ensureExpiredUGIIsNotCleanedUpIfItIsStillReferenced() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2", "default");
        UserGroupInformation stillInUse = cache.getUserGroupInformation(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        // at this point, stillInUse is expired but still in use
        cache.getUserGroupInformation(session2, false); // trigger cleanup
        assertStillInCache(session, stillInUse);
        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        cache.getUserGroupInformation(session2, false);

        verify(provider, times(1)).destroy(stillInUse);
    }

    @Test
    public void ensureExpiredProxyUGIIsNotCleanedUpIfItIsStillReferenced() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2", "default");
        UserGroupInformation stillInUse = cache.getUserGroupInformation(session, true);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        // at this point, stillInUse is expired but still in use
        cache.getUserGroupInformation(session2, true); // trigger cleanup
        assertStillInCache(session, stillInUse);
        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        cache.getUserGroupInformation(session2, true);

        verify(provider, times(1)).destroy(stillInUse);
    }

    @Test
    public void putsItemsBackInTheQueueWhenResettingExpirationDate() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2", "default");
        SessionId session3 = new SessionId(0, "txn-id", "the-user-3", "default");

        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY - 1000);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session2, true);
        cache.release(session2, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY - 1000);
        cache.release(session, false);
        fakeTicker.advanceTime(2 * MINUTES);
        cache.getUserGroupInformation(session3, true);

        assertStillInCache(session, ugi1);
        assertNoLongerInCache(session2, ugi2);
    }

    @Test
    public void releaseWithoutImmediateCleanup() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);

        cache.release(session, false);
        assertStillInCache(session, ugi1);
    }

    @Test
    public void releaseWithImmediateCleanup() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);

        cache.release(session, true);
        assertNoLongerInCache(session, ugi1);
    }

    @Test
    public void releaseWithImmediateCleanupOnlyCleansUGIsForThatSegment() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);

        SessionId differentSeg = new SessionId(999, "txn-id", "user", "default");
        UserGroupInformation ugi2 = cache.getUserGroupInformation(differentSeg, true);

        cache.release(differentSeg, false); // ugi2 is now unreferenced
        cache.release(session, true);
        assertNoLongerInCache(session, ugi1);
        assertStillInCache(differentSeg, ugi2);
        assertCacheSize(1);
    }

    @Test
    public void releaseResetsTheExpirationTime() throws Exception {
        UserGroupInformation reference1 = cache.getUserGroupInformation(session, true);
        cache.getUserGroupInformation(session, true);

        cache.release(session, true);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);
        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY - 1000);

        assertStillInCache(session, reference1);
    }

    @Test
    public void releaseAnExpiredUGIResetsTheTimer() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);
        cache.release(session, false);

        assertStillInCache(session, ugi1);

        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY - 1000);
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2", "default");
        cache.getUserGroupInformation(session2, false); // triggers cleanup
        assertStillInCache(session, ugi1);
    }

    @Test
    public void releaseAnExpiredProxyUGIResetsTheTimer() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);
        cache.release(session, false);

        assertStillInCache(session, ugi1);

        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY - 1000);
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2", "default");
        cache.getUserGroupInformation(session2, true); // triggers cleanup
        assertStillInCache(session, ugi1);
    }

    @Test
    public void releaseAndReacquireDoesNotFreeResources() throws Exception {
        cache.getUserGroupInformation(session, true);
        cache.release(session, false);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session, true);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);
        UserGroupInformation ugi3 = cache.getUserGroupInformation(session, true);
        // this does not clean up any UGIs because our ugi is still in use.
        assertEquals(ugi3, ugi2);
        verify(provider, times(1)).createProxyUGI("the-user", UserGroupInformation.getLoginUser());
        verify(provider, never()).destroy(any(UserGroupInformation.class));
        assertStillInCache(session, ugi2);
    }

    @Test
    public void releaseAndAcquireAfterTimeoutFreesResources() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);

        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);
        assertStillInCache(session, ugi1);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session, true);
        verify(provider).destroy(ugi1);
        assertNotEquals(ugi2, ugi1);
        assertStillInCache(session, ugi2);
    }

    @Test
    public void releaseDoesNotFreeResourcesIfUGIIsUsedElsewhere() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);
        cache.getUserGroupInformation(session, true); // increments ref count to 2

        cache.release(session, true);
        fakeTicker.advanceTime(60 * MINUTES);
        // UGI was not cleaned up because we are still holding a reference
        assertStillInCache(session, ugi1);
    }

    @Test
    public void releasingAllReferencesFreesResources() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session, true);

        assertEquals(ugi1, ugi2);

        cache.release(session, true);
        assertStillInCache(session, ugi1);
        cache.release(session, true);
        // at this point, the initial UGI has been freed.
        assertNoLongerInCache(session, ugi1);
    }

    @Test
    public void errorsThrownByCreatingAUgiAreNotCaught() throws Exception {
        when(provider.createProxyUGI("the-user", UserGroupInformation.getLoginUser())).thenThrow(new IOException("test exception"));

        assertThrows(IOException.class,
                () -> cache.getUserGroupInformation(session, true));
    }

    @Test
    public void errorsThrownByDestroyingAUgiAreCaught() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session, true);
        doThrow(new IOException("test exception")).when(provider).destroy(ugi1);
        cache.release(session, true); // does not throw
    }

    @Test
    public void releaseAnEntryNotInTheCache() {
        // this could happen if some caller of the cache calls release twice.
        assertThrows(IllegalStateException.class,
                () -> cache.release(session, false));
    }

    private void assertStillInCache(SessionId session, UserGroupInformation ugi) throws Exception {
        assertTrue(cache.contains(session));
        verify(provider, never()).destroy(ugi);
    }

    private void assertNoLongerInCache(SessionId session, UserGroupInformation ugi) throws Exception {
        assertFalse(cache.contains(session));
        verify(provider, times(1)).destroy(ugi);
    }

    private void assertCacheSize(int expectedSize) {
        assertEquals(expectedSize, cache.size());
        assertEquals(expectedSize, cache.allQueuesSize());
    }
}
