package org.greenplum.pxf.service.security;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.SessionId;
import org.greenplum.pxf.service.UGICache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;

/**
 * Security Service
 */
@Service
public class BaseSecurityService implements SecurityService {

    private static final Logger LOG = LoggerFactory.getLogger(BaseSecurityService.class);

    private final SecureLogin secureLogin;

    private final UGICache ugiCache;

    public BaseSecurityService(SecureLogin secureLogin, UGICache ugiCache) {
        this.secureLogin = secureLogin;
        this.ugiCache = ugiCache;
    }

    /**
     * If user impersonation is configured, examines the request for the
     * presence of the expected security headers and create a proxy user to
     * execute further request chain. If security is enabled for the
     * configuration server used for the requests, makes sure that a login
     * UGI for the the Kerberos principal is created and cached for future use.
     *
     * <p>Responds with an HTTP error if the header is missing or the chain
     * processing throws an exception.
     *
     * @param context the context for the given request
     * @param action  the action to be executed
     * @throws IOException when an IO error occurs
     */
    public <T> T doAs(RequestContext context, final boolean lastCallForSegment, PrivilegedExceptionAction<T> action) throws IOException {
        // retrieve user header and make sure header is present and is not empty
        final String gpdbUser = context.getUser();
        final String transactionId = context.getTransactionId();
        final Integer segmentId = context.getSegmentId();
        final String serverName = context.getServerName();
        final String configDirectory = context.getConfig();
        Configuration configuration = context.getConfiguration();

        boolean isUserImpersonation = secureLogin.isUserImpersonationEnabled(configuration);

        // Establish the UGI for the login user or the Kerberos principal for the given server, if applicable
        UserGroupInformation loginUser = secureLogin.getLoginUser(serverName, configDirectory, configuration);

        String serviceUser = loginUser.getUserName();

        if (!isUserImpersonation && Utilities.isSecurityEnabled(configuration)) {
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

        String remoteUser = (isUserImpersonation ? gpdbUser : serviceUser);

        SessionId session = new SessionId(
                segmentId,
                transactionId,
                remoteUser,
                serverName,
                configuration,
                loginUser);

        try {
            // Retrieve proxy user UGI from the UGI of the logged in user
            UserGroupInformation userGroupInformation = ugiCache
                    .getUserGroupInformation(session, isUserImpersonation);

            LOG.debug("Retrieved proxy user {} for server {} and session {}", userGroupInformation, serverName, session);
            LOG.debug("Performing request for gpdb_user = {} as [remote_user = {} service_user = {} login_user ={}] with{} impersonation",
                    gpdbUser, remoteUser, serviceUser, loginUser.getUserName(), isUserImpersonation ? "" : "out");
            // Execute the servlet chain as that user
            return userGroupInformation.doAs(action);
        } catch (UndeclaredThrowableException ute) {
            // unwrap the real exception thrown by the action
            throw new IOException(ute.getCause());
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        } finally {
            // Optimization to cleanup the cache if it is the last fragment
            LOG.debug("Releasing proxy user for session: {}. {}",
                    session, lastCallForSegment ? " Last fragment call" : "");
            try {
                ugiCache.release(session, lastCallForSegment);
            } catch (Throwable t) {
                LOG.error("Error releasing UGICache for session: {}", session, t);
            }
            if (lastCallForSegment) {
                LOG.info("Finished processing {}", session);
            }
        }
    }
}
