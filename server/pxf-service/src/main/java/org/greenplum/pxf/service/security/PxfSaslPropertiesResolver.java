package org.greenplum.pxf.service.security;

import com.sun.security.jgss.ExtendedGSSCredential;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_SESSION_USER_PROPERTY;

public class PxfSaslPropertiesResolver extends SaslPropertiesResolver {

    @Override
    public Map<String, String> getClientProperties(InetAddress serverAddress, int ingressPort) {
        return getClientProperties(serverAddress);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, String> getClientProperties(InetAddress serverAddress) {
        Map<String, String> props = super.getClientProperties(serverAddress);
        return new MapWrapper(props);
    }

    // needs to be run under doAs from a subject having a service kerberos ticket
    private GSSCredential getKerberosProxyCredential() {
        String userName = getConf().get(PXF_SESSION_USER_PROPERTY);
        // TODO get realm from principal name
        // Subject.getSubject(AccessController.getContext()).getConf().get("pxf.user.name");
        String realm = "C.DATA-GPDB-UD.INTERNAL";
        GSSManager manager = GSSManager.getInstance();
        try {
            GSSCredential serviceCredentials = manager.createCredential(GSSCredential.INITIATE_ONLY);
            GSSName other = manager.createName(userName + "@" + realm, GSSName.NT_USER_NAME);
            return ((ExtendedGSSCredential) serviceCredentials).impersonate(other);
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
        private final Map<String, String> props;

        public MapWrapper(Map<String, String> props) {
            this.props = props;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T get(Object key) {
            if (key.toString().equals("javax.security.sasl.credentials")) {
                // TODO: cache credential in configuration ? check if credential is near expiration ?
                return (T) getKerberosProxyCredential();
            } else {
                return (T) props.get(key);
            }
        }
    }
}
