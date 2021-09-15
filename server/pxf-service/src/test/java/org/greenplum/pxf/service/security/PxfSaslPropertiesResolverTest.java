package org.greenplum.pxf.service.security;

import com.sun.security.jgss.ExtendedGSSCredential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PxfSaslPropertiesResolverTest {

    private PxfSaslPropertiesResolver resolver;
    private Configuration configuration;

    @Mock private GSSManager mockGSSManager;
    @Mock private ExtendedGSSCredential mockCredential;
    @Mock private GSSName mockGSSName;
    @Mock private GSSCredential mockProxyCredential;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
    }

    @Test
    public void testCanBeInstantiated() {
        configuration.set("hadoop.security.saslproperties.resolver.class", PxfSaslPropertiesResolver.class.getName());
        SaslPropertiesResolver saslResolver = SaslPropertiesResolver.getInstance(configuration);
        assertTrue(saslResolver instanceof PxfSaslPropertiesResolver);
        assertSame(configuration, saslResolver.getConf());
    }

    @Test
    public void testReturnsDefaultPropertiesAndCredential_UserNameWithoutRealmSuffix() throws UnknownHostException, GSSException {
        expectSuccessfulInteractions("testUser", "EXAMPLE.FOO","testUser@EXAMPLE.FOO");
        assertReturnedDefaultProperties();
    }

    @Test
    public void testReturnsDefaultPropertiesAndCredential_UserNameWithRealmSuffix() throws UnknownHostException, GSSException {
        expectSuccessfulInteractions("testUser@EXAMPLE.FOO", "EXAMPLE.FOO", "testUser@EXAMPLE.FOO");
        assertReturnedDefaultProperties();
    }

    @Test
    public void testReturnsDefaultPropertiesAndCredential_UserNameWithSimilarSuffix() throws UnknownHostException, GSSException {
        // in reality this composite principal name will not happen as realms will match,
        // but the code does not validate realms and the below is what will happen for the given input
        expectSuccessfulInteractions("testUser@BAR.EXAMPLE.FOO", "EXAMPLE.FOO", "testUser@BAR.EXAMPLE.FOO@EXAMPLE.FOO");
        assertReturnedDefaultProperties();
    }

    @Test
    public void testReturnsQopPropertiesAndCredential() throws UnknownHostException, GSSException {
        configuration.set("hadoop.rpc.protection", " integrity ,privacy "); // use extra white spaces to test trimming
        expectSuccessfulInteractions("testUser", "EXAMPLE.FOO","testUser@EXAMPLE.FOO");
        assertReturnedQopProperties("auth-int,auth-conf"); // parent class parses QOP levels
    }

    @Test
    public void testFailureDuringImpersonation() throws GSSException {
        configuration.set("pxf.session.user", "testUser");
        configuration.set("pxf.service.kerberos.realm", "EXAMPLE.FOO");

        // create the resolver using provided configuration
        resolver = new PxfSaslPropertiesResolver(() -> mockGSSManager);
        resolver.setConf(configuration);

        when(mockGSSManager.createCredential(GSSCredential.INITIATE_ONLY)).thenReturn(mockCredential);
        when(mockGSSManager.createName("testUser@EXAMPLE.FOO", GSSName.NT_USER_NAME)).thenReturn(mockGSSName);
        Exception expectedException = new GSSException(GSSException.FAILURE);
        when(mockCredential.impersonate(mockGSSName)).thenThrow(expectedException);

        Exception e = assertThrows(RuntimeException.class, () -> resolver.getClientProperties(InetAddress.getLocalHost()));
        assertSame(expectedException, e.getCause());
    }

    private void expectSuccessfulInteractions(String userName, String realmName, String compositeName) throws GSSException {
        configuration.set("pxf.session.user", userName); // missing @EXAMPLE sequence
        configuration.set("pxf.service.kerberos.realm", realmName);

        // create the resolver using provided configuration
        resolver = new PxfSaslPropertiesResolver(() -> mockGSSManager);
        resolver.setConf(configuration);

        when(mockGSSManager.createCredential(GSSCredential.INITIATE_ONLY)).thenReturn(mockCredential);
        when(mockGSSManager.createName(compositeName, GSSName.NT_USER_NAME)).thenReturn(mockGSSName);
        when(mockCredential.impersonate(mockGSSName)).thenReturn(mockProxyCredential);
    }

    private void assertReturnedDefaultProperties() throws UnknownHostException {
        assertReturnedQopProperties("auth");
    }

    private void assertReturnedQopProperties(String expectedQop) throws UnknownHostException {
        Map<String, ?> props = resolver.getClientProperties(InetAddress.getLocalHost());
        assertEquals(3, props.size());
        // default props from parent class
        assertEquals(expectedQop, props.get("javax.security.sasl.qop"));
        assertEquals("true", props.get("javax.security.sasl.server.authentication"));
        // special property with the credential
        assertSame(mockProxyCredential, props.get("javax.security.sasl.credentials"));
    }

}
