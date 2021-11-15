package org.greenplum.pxf.service.security;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.security.GSSCredentialProvider;
import org.greenplum.pxf.api.utilities.SpringContext;
import org.ietf.jgss.GSSCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;


/**
 * Customized resolver that is called by Hadoop SASL mechanism when a SASL connection needs to be established.
 * It obtains an S4U2self credential from the provider and stores it under "javax.security.sasl.credentials"
 * property name so that the downstream machinery can perform the Kerberos Constrained Delegation logic.
 * This resolver must be set on the Hadoop configuration object under "hadoop.security.saslproperties.resolver.class"
 * property name.
 */
public class PxfSaslPropertiesResolver extends SaslPropertiesResolver {

    private static final Logger LOG = LoggerFactory.getLogger(PxfSaslPropertiesResolver.class);
    private final GSSCredentialProvider credentialProvider;

    /**
     * Default constructor, uses Spring-managed singleton of GSSCredentialProvider class.
     * It is used by the Hadoop security framework.
     */
    public PxfSaslPropertiesResolver() {
        this.credentialProvider = SpringContext.getBean(GSSCredentialProvider.class);
    }

    /**
     * Constructor used for testing that takes a GSSCredentialProvider provider as a parameter.
     * @param credentialProvider a provider of GSS credentials
     */
    PxfSaslPropertiesResolver(GSSCredentialProvider credentialProvider) {
        this.credentialProvider = credentialProvider;
    }

    @Override
    public Map<String, String> getClientProperties(InetAddress serverAddress, int ingressPort) {
        return getClientProperties(serverAddress);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, String> getClientProperties(InetAddress serverAddress) {
        Map<String, String> props = super.getClientProperties(serverAddress);
        // return a enhanced map initialized with properties obtained from the parent class and having
        // an extra property "javax.security.sasl.credentials" added with the value of the GSS credential
        // obtained from the credential provider.
        return new MapWrapper(props);
    }

    /**
     * Obtains an S4U2self credential for the GP user (or service user) and PXF server specified by the configuration.
     * It must be run under from a "doAs" block from a Subject having a service Kerberos ticket.
     *
     * @return the proxy credential
     */
    private GSSCredential getKerberosProxyCredential() {
        String userName = getConf().get(ConfigurationFactory.PXF_SESSION_REMOTE_USER_PROPERTY);
        String server = getConf().get(ConfigurationFactory.PXF_SERVER_NAME_PROPERTY);
        Preconditions.checkState(StringUtils.isNotBlank(userName), "User name is missing from the configuration.");
        Preconditions.checkState(StringUtils.isNotBlank(server), "Server name is missing from the configuration.");
        LOG.debug("Requesting GSS credential for user {} and server {}", userName, server);
        return credentialProvider.getGSSCredential(userName, server);
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
