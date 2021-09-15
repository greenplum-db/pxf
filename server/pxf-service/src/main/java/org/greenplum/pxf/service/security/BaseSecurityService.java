package org.greenplum.pxf.service.security;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.security.PrivilegedAction;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS;
import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_CONFIG_SERVER_DIRECTORY_PROPERTY;

/**
 * Security Service
 */
@Service
public class BaseSecurityService implements SecurityService {

    private static final Logger LOG = LoggerFactory.getLogger(BaseSecurityService.class);

    private final SecureLogin secureLogin;
    private final UGIProvider ugiProvider;

    /* feature flag to expand Kerberos User Principal name when impersonating */
    private boolean isExpandUserPrincipal = true;

    public BaseSecurityService(SecureLogin secureLogin, UGIProvider ugiProvider,
                               @Value("${pxf.features.kerberos.expand-user-principal}") boolean isExpandUserPrincipal) {
        this.secureLogin = secureLogin;
        this.ugiProvider = ugiProvider;
        this.isExpandUserPrincipal = isExpandUserPrincipal;
    }

    /**
     * If user impersonation is configured, examines the request for the
     * presence of the expected security headers and create a proxy user to
     * execute further request chain. If security is enabled for the
     * configuration server used for the requests, makes sure that a login
     * UGI for the the Kerberos principal is created.
     *
     * <p>Responds with an HTTP error if the header is missing or the chain
     * processing throws an exception.
     *
     * @param context the context for the given request
     * @param action  the action to be executed
     * @throws Exception if the operation fails
     */
    public <T> T doAs(RequestContext context, PrivilegedAction<T> action) throws Exception {
        // retrieve user header and make sure header is present and is not empty
        final String gpdbUser = context.getUser();
        final String serverName = context.getServerName();
        final String configDirectory = context.getConfig();
        final Configuration configuration = context.getConfiguration();
        final boolean isConstrainedDelegationEnabled = secureLogin.isConstrainedDelegationEnabled(configuration);
        final boolean isUserImpersonationEnabled = secureLogin.isUserImpersonationEnabled(configuration);
        final boolean isSecurityEnabled = Utilities.isSecurityEnabled(configuration);

        // Establish the UGI for the login user or the Kerberos principal for the given server, if applicable
        boolean exceptionDetected = false;
        UserGroupInformation userGroupInformation = null;
        try {
            UserGroupInformation loginUser = secureLogin.getLoginUser(serverName, configDirectory, configuration);

            String serviceUser = loginUser.getUserName();

            if (!isUserImpersonationEnabled && isSecurityEnabled) {
                // When impersonation is disabled and security is enabled
                // we check whether the pxf.service.user.name property was provided
                // and if provided we use the value as the remote user instead of
                // the principal defined in pxf.service.kerberos.principal. However,
                // the principal will need to have proxy privileges on hadoop.
                String pxfServiceUserName = configuration.get(SecureLogin.CONFIG_KEY_SERVICE_USER_NAME);
                if (StringUtils.isNotBlank(pxfServiceUserName)) {
                    serviceUser = pxfServiceUserName;
                }
            }

            String remoteUser = (isUserImpersonationEnabled ? gpdbUser : serviceUser);

            if (isSecurityEnabled) {
                // derive realm from the logged in user, rather than parsing principal info ourselves
                String realm = (new HadoopKerberosName(loginUser.getUserName())).getRealm();
                // store in the configuration for any future reference within this request
                configuration.set(SecureLogin.CONFIG_KEY_SERVICE_REALM, realm);
                // include realm in the principal name, if required
                remoteUser = expandRemoteUserName(remoteUser, realm, isUserImpersonationEnabled, isConstrainedDelegationEnabled);
            }

            // validate and set properties required for enabling Kerberos constrained delegation, if necessary
            processConstrainedDelegation(configuration, isSecurityEnabled, isUserImpersonationEnabled, isConstrainedDelegationEnabled);

            // Retrieve proxy user UGI from the UGI of the logged in user
            if (isUserImpersonationEnabled) {
                LOG.debug("Creating proxy user = {}", remoteUser);
                userGroupInformation = ugiProvider.createProxyUser(remoteUser, loginUser);
            } else {
                LOG.debug("Creating remote user = {}", remoteUser);
                userGroupInformation = ugiProvider.createRemoteUser(remoteUser, loginUser, isSecurityEnabled);
            }

            LOG.debug("Retrieved proxy user {} for server {}", userGroupInformation, serverName);
            LOG.debug("Performing request for gpdb_user = {} as [remote_user={}, service_user={}, login_user={}] with{} impersonation",
                    gpdbUser, remoteUser, serviceUser, loginUser.getUserName(), isUserImpersonationEnabled ? "" : "out");
            // Execute the servlet chain as that user
            return userGroupInformation.doAs(action);
        } catch (Exception e) {
            exceptionDetected = true;
            throw e;
        } finally {
            LOG.debug("Releasing UGI resources. {}", exceptionDetected ? " Exception while processing." : "");
            try {
                if (userGroupInformation != null) {
                    ugiProvider.destroy(userGroupInformation);
                }
            } catch (Throwable t) {
                LOG.warn("Error releasing UGI resources, ignored.", t);
            }
        }
    }

    /**
     * Validates consistency of property values for Kerberos constrained delegation and sets configuration properties
     * that support this feature for the request that holds this configuration.
     * PXF profiles will get this enhanced configuration from the RequestContext and will pass
     * it to Hadoop FileSystem operations making it available downstream in Hadoop SASL layers.
     *
     * @param configuration configuration for the current request
     * @param isSecurityEnabled whether Kerberos security is enabled
     * @param isUserImpersonationEnabled whether user impersonation is enabled
     * @param isConstrainedDelegationEnabled whether constrained delegation is enabled
     */
    private void processConstrainedDelegation(Configuration configuration, boolean isSecurityEnabled,
                                              boolean isUserImpersonationEnabled, boolean isConstrainedDelegationEnabled) {
        if (isConstrainedDelegationEnabled) {
            if (!isSecurityEnabled) {
                throw new PxfRuntimeException("Kerberos constrained delegation should not be enabled for non-secure cluster.",
                        String.format("Set the value of %s property to false in %s/pxf-site.xml file.",
                                SecureLogin.CONFIG_KEY_SERVICE_CONSTRAINED_DELEGATION,
                                configuration.get(PXF_CONFIG_SERVER_DIRECTORY_PROPERTY)));
            }
            if (!isUserImpersonationEnabled) {
                throw new PxfRuntimeException("User impersonation is not enabled for Kerberos constrained delegation.",
                        String.format("Set the value of %s property to true in %s/pxf-site.xml file.",
                                SecureLogin.CONFIG_KEY_SERVICE_USER_IMPERSONATION,
                                configuration.get(PXF_CONFIG_SERVER_DIRECTORY_PROPERTY)));
            }
            configuration.set(HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS, PxfSaslPropertiesResolver.class.getName());
            LOG.debug("Kerberos constrained delegation and user impersonation are enabled, setting up PxfSaslPropertiesResolver");
        }
    }

    /**
     * Expands Kerberos user principal name by adding the Kerberos realm at the end, if needed.
     * Kerberos constrained delegation requires realm in the principal name, we will ensure that for any
     * secure impersonation, for now guarded by a feature flag.
     *
     * @param remoteUser remote user name
     * @param realm Kerberos realm
     * @param isUserImpersonation whether user impersonation is enabled
     * @param isConstrainedDelegationEnabled whether constrained delegation is enabled
     * @return expanded user principal name or the original name if expansion was not required
     */
    private String expandRemoteUserName(String remoteUser, String realm, boolean isUserImpersonation, boolean isConstrainedDelegationEnabled) {
        String result = remoteUser;
        if (isUserImpersonation &&
                (isConstrainedDelegationEnabled || isExpandUserPrincipal) &&
                !remoteUser.endsWith(realm)) {
            result += "@" + realm;
            LOG.debug("Expanded user principal name from {} to {}", remoteUser, result);
        }
        return result;
    }

}
