package org.greenplum.pxf.api.security;

import com.sun.security.jgss.ExtendedGSSCredential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.PxfUserGroupInformation;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GSSCredentialProviderTest {

    private GSSCredentialProvider provider;
    private Configuration configuration;

    @Mock
    private GSSManager mockGSSManager;
    @Mock
    private ExtendedGSSCredential mockCredential;
    @Mock
    private ExtendedGSSCredential mockCredential2;
    @Mock
    private GSSName mockGSSName;
    @Mock
    private GSSCredential mockProxyCredential;
    @Mock
    private GSSCredential mockProxyCredential2;
    @Mock
    private PxfUserGroupInformation mockPxfUgi;
    @Mock
    private KerberosTicket mockTGT;
    @Mock
    private KerberosTicket mockTGT2;
    @Mock
    private KerberosTicket mockTGT3;
    @Mock
    private KerberosPrincipal mockKerberosPrincipal;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
        provider = new GSSCredentialProvider(mockPxfUgi, Duration.ofHours(1L));
        provider.setGssManager(mockGSSManager);
    }

    @Test
    public void testGetCredentialFailsTGTIsNull() {
        Exception e = assertThrows(NullPointerException.class, () -> provider.getGSSCredential("user", "server"));
        assertEquals("No TGT found in the Subject.", e.getMessage());
    }

    @Test
    public void testGetCredentialFailsTGTIsDestroyed() {
        when(mockPxfUgi.getTGT(any())).thenReturn(mockTGT);
        when(mockTGT.isDestroyed()).thenReturn(true);
        Exception e = assertThrows(IllegalStateException.class, () -> provider.getGSSCredential("user", "server"));
        assertEquals("TGT is destroyed.", e.getMessage());
    }

    @Test
    public void testGetCredentialFailsTGTIsNotCurrent() {
        when(mockPxfUgi.getTGT(any())).thenReturn(mockTGT);
        when(mockTGT.isDestroyed()).thenReturn(false);
        when(mockTGT.isCurrent()).thenReturn(false);
        Exception e = assertThrows(IllegalStateException.class, () -> provider.getGSSCredential("user", "server"));
        assertEquals("TGT is not current.", e.getMessage());
    }

    @Test
    public void testGetCredentialFailsToObtainCredentialWhenLoadingCache() throws GSSException {
        when(mockPxfUgi.getTGT(any())).thenReturn(mockTGT);
        when(mockTGT.isDestroyed()).thenReturn(false);
        when(mockTGT.isCurrent()).thenReturn(true);
        when(mockTGT.getServer()).thenReturn(mockKerberosPrincipal);
        when(mockKerberosPrincipal.getRealm()).thenReturn("REALM");
        when(mockGSSManager.createCredential(GSSCredential.INITIATE_ONLY)).thenReturn(mockCredential);
        when(mockGSSManager.createName("user@REALM", GSSName.NT_USER_NAME)).thenReturn(mockGSSName);
        GSSException gssException = new GSSException(GSSException.BAD_BINDINGS); // use an arbitrary error code
        when(mockCredential.impersonate(mockGSSName)).thenThrow(gssException);

        Exception e = assertThrows(RuntimeException.class, () -> provider.getGSSCredential("user", "server"));
        assertSame(gssException, e.getCause());
        assertEquals("GSSException: Channel binding mismatch", e.getMessage());
    }

    @Test
    public void testGetCredentialNotCachedValidTGTAfterGet() throws GSSException {
        mockFlowForFirstCall();

        // first call
        GSSCredential result = provider.getGSSCredential("user", "server");
        assertSame(mockProxyCredential, result);

        verifyImpersonationCalls(mockCredential);
    }


    @Test
    public void testGetCredentialCachedValidTGTSameTGT() throws GSSException {
        // first call will cache the credential
        mockFlowForFirstCall();

        // first call
        GSSCredential result = provider.getGSSCredential("user", "server");
        assertSame(mockProxyCredential, result);
        // second call
        GSSCredential result2 = provider.getGSSCredential("user", "server");
        assertSame(mockProxyCredential, result2);

        verifyImpersonationCalls(mockCredential);
    }

    @Test
    public void testGetCredentialCachedForDifferentServers() throws GSSException {
        // first call will cache the credential
        // 1st call: load, getCurrent; 2nd call: load, getCurrent
        when(mockPxfUgi.getTGT(any())).thenReturn(mockTGT, mockTGT, mockTGT2, mockTGT2);
        // 1st call: load, getCurrent, checkCached
        when(mockTGT.isDestroyed()).thenReturn(false); // original is never destroyed
        // 2nd call: load, getCurrent, checkCached
        when(mockTGT2.isDestroyed()).thenReturn(false); // new is never destroyed
        // always current
        when(mockTGT.isCurrent()).thenReturn(true);
        when(mockTGT2.isCurrent()).thenReturn(true);
        when(mockTGT.getServer()).thenReturn(mockKerberosPrincipal);
        when(mockTGT2.getServer()).thenReturn(mockKerberosPrincipal);
        //
        when(mockKerberosPrincipal.getRealm()).thenReturn("REALM");
        when(mockGSSManager.createCredential(GSSCredential.INITIATE_ONLY)).thenReturn(mockCredential, mockCredential2);
        when(mockGSSManager.createName("user@REALM", GSSName.NT_USER_NAME)).thenReturn(mockGSSName);
        when(mockCredential.impersonate(mockGSSName)).thenReturn(mockProxyCredential);
        when(mockCredential2.impersonate(mockGSSName)).thenReturn(mockProxyCredential2);

        // first call
        GSSCredential result = provider.getGSSCredential("user", "server1");
        assertSame(mockProxyCredential, result);

        // second call for a different server
        GSSCredential result2 = provider.getGSSCredential("user", "server2");
        assertSame(mockProxyCredential2, result2);

        verifyImpersonationCalls(mockCredential, mockCredential2);
    }

    @Test
    public void testGetCredentialCachedDestroyedTGTDifferentTGT() throws GSSException {
        // 1st call: load, getCurrent; 2nd call: getCurrent, load, getCurrent
        when(mockPxfUgi.getTGT(any())).thenReturn(mockTGT, mockTGT, mockTGT2, mockTGT2, mockTGT2);
        // 1st call: load, getCurrent, checkCached; 2nd call: checkCached
        when(mockTGT.isDestroyed()).thenReturn(false, false, false, true);
        // 2nd call: getCurrent, load, getCurrent, checkCached
        when(mockTGT2.isDestroyed()).thenReturn(false, false, false, false);
        // always current
        when(mockTGT.isCurrent()).thenReturn(true);
        when(mockTGT2.isCurrent()).thenReturn(true);
        when(mockTGT.getServer()).thenReturn(mockKerberosPrincipal);
        when(mockTGT2.getServer()).thenReturn(mockKerberosPrincipal);

        when(mockKerberosPrincipal.getRealm()).thenReturn("REALM");
        when(mockGSSManager.createCredential(GSSCredential.INITIATE_ONLY)).thenReturn(mockCredential, mockCredential2);
        when(mockGSSManager.createName("user@REALM", GSSName.NT_USER_NAME)).thenReturn(mockGSSName);
        when(mockCredential.impersonate(mockGSSName)).thenReturn(mockProxyCredential);
        when(mockCredential2.impersonate(mockGSSName)).thenReturn(mockProxyCredential2);

        // first call
        GSSCredential result = provider.getGSSCredential("user", "server");
        assertSame(mockProxyCredential, result);
        // second call
        GSSCredential result2 = provider.getGSSCredential("user", "server");
        assertSame(mockProxyCredential2, result2);

        verifyImpersonationCalls(mockCredential, mockCredential2);
    }

    @Test
    public void testGetCredentialCachedNotDestroyedTGTDifferentTGT() throws GSSException {
        // this will unlikely happen in reality (maye in HDFS HA case?) where the current TGT is different,
        // but the cached one is not destroyed, since TGT changes or re-login from keytab when old TGT is destroyed

        // 1st call: load, getCurrent; 2nd call: getCurrent, load, getCurrent
        when(mockPxfUgi.getTGT(any())).thenReturn(mockTGT, mockTGT, mockTGT2, mockTGT2, mockTGT2);
        // 1st call: load, getCurrent, checkCached; 2nd call: checkCached
        when(mockTGT.isDestroyed()).thenReturn(false); // original is never destroyed
        // 2nd call: getCurrent, load, getCurrent, checkCached
        when(mockTGT2.isDestroyed()).thenReturn(false); // new is never destroyed
        // always current
        when(mockTGT.isCurrent()).thenReturn(true);
        when(mockTGT2.isCurrent()).thenReturn(true);
        when(mockTGT.getServer()).thenReturn(mockKerberosPrincipal);
        when(mockTGT2.getServer()).thenReturn(mockKerberosPrincipal);
        //
        when(mockKerberosPrincipal.getRealm()).thenReturn("REALM");
        when(mockGSSManager.createCredential(GSSCredential.INITIATE_ONLY)).thenReturn(mockCredential, mockCredential2);
        when(mockGSSManager.createName("user@REALM", GSSName.NT_USER_NAME)).thenReturn(mockGSSName);
        when(mockCredential.impersonate(mockGSSName)).thenReturn(mockProxyCredential);
        when(mockCredential2.impersonate(mockGSSName)).thenReturn(mockProxyCredential2);

        // first call
        GSSCredential result = provider.getGSSCredential("user", "server");
        assertSame(mockProxyCredential, result);
        // second call
        GSSCredential result2 = provider.getGSSCredential("user", "server");
        assertSame(mockProxyCredential2, result2);

        verifyImpersonationCalls(mockCredential, mockCredential2);
    }

    @Test
    public void testGetCredentialNotLoopingForever() throws GSSException {
        // this is not a realistic scnerip, but we need to make sure we do not try to obtain the credential forever
        // in a case where cached TGT is not valid or switches again after a retry attempt.

        // 1st call: load, getCurrent; 2nd call: getCurrent, load, getCurrent
        when(mockPxfUgi.getTGT(any())).thenReturn(mockTGT, mockTGT, mockTGT2, mockTGT2, mockTGT3);
        // 1st call: load, getCurrent, checkCached; 2nd call: checkCached
        when(mockTGT.isDestroyed()).thenReturn(false); // original is never destroyed
        // 2nd call: getCurrent, load, getCurrent, checkCached
        when(mockTGT2.isDestroyed()).thenReturn(false); // new is never destroyed
        // always current
        when(mockTGT.isCurrent()).thenReturn(true);
        when(mockTGT2.isCurrent()).thenReturn(true);
        when(mockTGT3.isCurrent()).thenReturn(true);
        when(mockTGT.getServer()).thenReturn(mockKerberosPrincipal);
        when(mockTGT2.getServer()).thenReturn(mockKerberosPrincipal);
        //
        when(mockKerberosPrincipal.getRealm()).thenReturn("REALM");
        when(mockGSSManager.createCredential(GSSCredential.INITIATE_ONLY)).thenReturn(mockCredential, mockCredential2);
        when(mockGSSManager.createName("user@REALM", GSSName.NT_USER_NAME)).thenReturn(mockGSSName);
        when(mockCredential.impersonate(mockGSSName)).thenReturn(mockProxyCredential);
        when(mockCredential2.impersonate(mockGSSName)).thenReturn(mockProxyCredential2);

        // first call
        GSSCredential result = provider.getGSSCredential("user", "server");
        assertSame(mockProxyCredential, result);
        // second call
        Exception e = assertThrows(RuntimeException.class, () -> provider.getGSSCredential("user", "server"));
        assertEquals("Unexpected state: failed to obtain GSS credential.", e.getMessage());

        verifyImpersonationCalls(mockCredential, mockCredential2);
    }

    private void mockFlowForFirstCall() throws GSSException {
        // first call will cache the credential
        when(mockPxfUgi.getTGT(any())).thenReturn(mockTGT);
        when(mockTGT.isDestroyed()).thenReturn(false);
        when(mockTGT.isCurrent()).thenReturn(true);
        when(mockTGT.getServer()).thenReturn(mockKerberosPrincipal);
        when(mockKerberosPrincipal.getRealm()).thenReturn("REALM");
        when(mockGSSManager.createCredential(GSSCredential.INITIATE_ONLY)).thenReturn(mockCredential);
        when(mockGSSManager.createName("user@REALM", GSSName.NT_USER_NAME)).thenReturn(mockGSSName);
        when(mockCredential.impersonate(mockGSSName)).thenReturn(mockProxyCredential);
    }

    private void verifyImpersonationCalls(ExtendedGSSCredential... credentials) throws GSSException {
        // credential is not obtained again once cached
        Object[] mocks = new Object[credentials.length + 1];
        mocks[0] = mockGSSManager;
        verify(mockGSSManager, times(credentials.length)).createCredential(GSSCredential.INITIATE_ONLY);
        for (ExtendedGSSCredential credential : credentials) {
            verify(credential).impersonate(mockGSSName);
        }
        System.arraycopy(credentials, 0, mocks, 1, credentials.length);
        verifyNoMoreInteractions(mocks);
    }
}
