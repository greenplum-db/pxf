package org.greenplum.pxf.service.security;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.security.GSSCredentialProvider;
import org.ietf.jgss.GSSCredential;
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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PxfSaslPropertiesResolverTest {

    private PxfSaslPropertiesResolver resolver;
    private Configuration configuration;
    private InetAddress address;

    @Mock
    private GSSCredentialProvider mockCredentialProvider;
    @Mock
    private GSSCredential mockCredential;

    @BeforeEach
    public void setup() throws UnknownHostException {
        address = InetAddress.getLocalHost();
        configuration = new Configuration();
        resolver = new PxfSaslPropertiesResolver(mockCredentialProvider);
        resolver.setConf(configuration);
    }

    @Test
    public void testGetClientPropertiesFailsOnMissingUser() {
        configuration.set("pxf.config.server.name", "server");
        Exception e = assertThrows(IllegalStateException.class, () -> resolver.getClientProperties(address));
        assertEquals("User name is missing from the configuration.", e.getMessage());
    }

    @Test
    public void testGetClientPropertiesFailsOnMissingServer() {
        configuration.set("pxf.session.remote-user", "user");
        Exception e = assertThrows(IllegalStateException.class, () -> resolver.getClientProperties(address));
        assertEquals("Server name is missing from the configuration.", e.getMessage());
    }

    @Test
    public void testGetClientPropertiesNoPortContainsGSSCredential() {
        configuration.set("pxf.session.remote-user", "user");
        configuration.set("pxf.config.server.name", "server");
        when(mockCredentialProvider.getGSSCredential("user", "server")).thenReturn(mockCredential);

        Map<String, String> props = resolver.getClientProperties(address);
        assertReturnedQopProperties("auth", props);
    }

    @Test
    public void testGetClientPropertiesWithPortContainsGSSCredential() {
        configuration.set("pxf.session.remote-user", "user");
        configuration.set("pxf.config.server.name", "server");
        when(mockCredentialProvider.getGSSCredential("user", "server")).thenReturn(mockCredential);

        Map<String, String> props = resolver.getClientProperties(address, 1010);
        assertReturnedQopProperties("auth", props);
    }

    @Test
    public void testGetClientPropertiesReturnsQopPropertiesAndCredential() {
        configuration.set("pxf.session.remote-user", "user");
        configuration.set("pxf.config.server.name", "server");
        configuration.set("hadoop.rpc.protection", " integrity ,privacy "); // use extra white spaces to test trimming
        resolver.setConf(configuration); // reset conf to re-parse specified QOP
        when(mockCredentialProvider.getGSSCredential("user", "server")).thenReturn(mockCredential);
        Map<String, String> props = resolver.getClientProperties(address);
        assertReturnedQopProperties("auth-int,auth-conf", props); // parent class parses QOP levels
    }

    @Test
    public void testFailureDuringGettingCredential() {
        configuration.set("pxf.session.remote-user", "user");
        configuration.set("pxf.config.server.name", "server");
        Exception expectedException = new RuntimeException("foo");
        when(mockCredentialProvider.getGSSCredential("user", "server")).thenThrow(expectedException);
        Exception e = assertThrows(RuntimeException.class, () -> resolver.getClientProperties(address));
        assertSame(expectedException, e);
    }

    private void assertReturnedQopProperties(String expectedQop, Map<String, ?> props) {
        assertEquals(3, props.size());
        // default props from parent class
        assertEquals(expectedQop, props.get("javax.security.sasl.qop"));
        assertEquals("true", props.get("javax.security.sasl.server.authentication"));
        // special property with the credential
        assertSame(mockCredential, props.get("javax.security.sasl.credentials"));
    }
}
