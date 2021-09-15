package org.greenplum.pxf.service.security;

import com.sun.security.jgss.ExtendedGSSCredential;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.greenplum.pxf.api.security.SecureLogin;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_SESSION_USER_PROPERTY;

/**
 * Customized resolver that is called by Hadoop SASL mechanism when a SASL connection needs to be established.
 * It obtains and stores an S4U2 proxy credential under "javax.security.sasl.credentials" property name so that
 * the downstream machinery can perform the Kerberos Constrained Delegation logic. This resolver must be set
 * on the Hadoop configuration object under "hadoop.security.saslproperties.resolver.class" property name.
 */
public class PxfSaslPropertiesResolver extends SaslPropertiesResolver {

    private static final Logger LOG = LoggerFactory.getLogger(PxfSaslPropertiesResolver.class);
    private final Supplier<GSSManager> gssManagerProvider; // break dependency on static method of GSSManager

    /**
     * Default constructor, uses built-in GSSManager provider. It is used by the Hadoop security framework.
     */
    public PxfSaslPropertiesResolver() {
        gssManagerProvider = GSSManager::getInstance;
    }

    /**
     * Constructor used for testing that takes a GSSManager provider as a parameter.
     * @param gssManagerProvider a provider of GSSManager
     */
    PxfSaslPropertiesResolver(Supplier<GSSManager> gssManagerProvider) {
        this.gssManagerProvider = gssManagerProvider;
    }

    @Override
    public Map<String, String> getClientProperties(InetAddress serverAddress, int ingressPort) {
        return getClientProperties(serverAddress);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, String> getClientProperties(InetAddress serverAddress) {
        Map<String, String> props = super.getClientProperties(serverAddress);
        // return a wrapper that will delegate all requests to the map obtained from the parent
        // but will execute a special logic when the key "javax.security.sasl.credentials" is requested.
        return new MapWrapper(props);
    }

    /**
     * Obtains an S4U2 proxy credential for the user and realm specified by the configuration.
     * It must be run under from a "doAs" block from a Subject having a service Kerberos ticket.
     *
     * @return the proxy credential
     */
    private GSSCredential getKerberosProxyCredential() {
        // TODO: cache credential per user/server and check if credential is near expiration ?
        // find the name of the Greenplum user
        String userName = getConf().get(PXF_SESSION_USER_PROPERTY);
        String realm = getConf().get(SecureLogin.CONFIG_KEY_SERVICE_REALM);
        String realmSuffix = "@" + realm;
        String userPrincipal = userName.endsWith(realmSuffix) ? userName : userName + realmSuffix;
        LOG.debug("Created principal name={} for user={} and realm={}", userPrincipal, userName, realm);
        GSSManager manager = gssManagerProvider.get();
        try {
            GSSCredential serviceCredentials = manager.createCredential(GSSCredential.INITIATE_ONLY);
            GSSName other = manager.createName(userPrincipal, GSSName.NT_USER_NAME);
            GSSCredential result = ((ExtendedGSSCredential) serviceCredentials).impersonate(other);
            LOG.debug("Obtained S4U2 credential {}", result);
            return result;
        } catch (GSSException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * A parameterized wrapper class to get around the fact that the return type of
     * SaslPropertiesResolver.getClientProperties() method is defined as a Map<String,String>
     * while the GssKrb5Client constructor that uses this map to lookup the credentials stored in this map
     * with the "javax.security.sasl.credentials" key is expecting a Map<String, ?> as a parameter and expects
     * the value to be an object, or more precisely a GSSCredential instance.
     *
     * Using this wrapper satisfies the compiler and at runtime the actual type parameter is erased anyways.
     * @param <T> type of map values
     */
    class MapWrapper<T> extends HashMap<String, T> {

        @SuppressWarnings("unchecked")
        public MapWrapper(Map<String, String> props) {
            super((Map<? extends String, ? extends T>) props);
            super.put(Sasl.CREDENTIALS, (T) getKerberosProxyCredential());
        }
    }
}
