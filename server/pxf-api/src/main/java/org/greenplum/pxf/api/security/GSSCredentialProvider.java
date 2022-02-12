package org.greenplum.pxf.api.security;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.sun.security.jgss.ExtendedGSSCredential;
import lombok.Data;
import org.apache.hadoop.security.PxfUserGroupInformation;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import java.security.AccessController;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Provider of S4U2self credentials for Kerberos Constrained Delegation SASL/GSS connection.
 * This provider obtains a Kerberos service ticket from the KDC for the end-user to the PXF service
 * thus impersonating the end user. Having this credential, SASL/GSS layer then obtains S4U2proxy credential
 * which is a TGS from the end user to the target Hadoop service.
 *
 * The provider uses a cache where it stores obtained credentials per user/pxf-server combo, since LoginContext and
 * the corresponding TGT is per PXF server. When the credential is cached, a PXF service TGT (for a given PXF server)
 * is stored along, since the credential will be invalid when TGT expires. When the credential is retrieved from the cache,
 * this provider checks whether the cached TGT is destroyed or cached TGT is different from the current TGT. If true,
 * the cached credential is discarded and a new one is re-obtained from the KDC with the currently active TGT.
 *
 * Cache expiration is managed by "pxf.service.kerberos.constrained-delegation.credential-cache.expiration" property
 * and is set to 1 day by default, as this is the typical duration of the TGT tickets. If the actual duration is shorter,
 * the entry will be replaced by above-mentioned logic sooner than that. If it is longer, then the entry will be evicted
 * earlier than it could've been, but will be replaced on the next request.
 * Doing this once a day does not have any overhead.
 */
@Component
public class GSSCredentialProvider {
    private static final Logger LOG = LoggerFactory.getLogger(GSSCredentialProvider.class);
    private static final String CACHE_EXPIRATION_PROPERTY =
            "pxf.service.kerberos.constrained-delegation.credential-cache.expiration";

    private final PxfUserGroupInformation pxfUgi;
    private GSSManager gssManager;
    private final Cache<String, GSSCredentialHolder> credentialCache;

    /**
     * A holder class wrapping a credential and a TGT. It is used as an entry in the credential cache.
     */
    @Data
    class GSSCredentialHolder {
        private final GSSCredential credential;
        private final KerberosTicket tgt;
    }

    /**
     * Constructor, creates a new instance. This is Spring singleton, so only one instance (and one cache) is created.
     * @param pxfUgi PxfUserGroupInformation instance
     * @param expiration cache entry duration before expiration
     */
    public GSSCredentialProvider(PxfUserGroupInformation pxfUgi,
                                 @Value("${" + CACHE_EXPIRATION_PROPERTY + "}")
                                 Duration expiration) {
        this.pxfUgi = pxfUgi;
        this.gssManager = GSSManager.getInstance();
        this.credentialCache = initCache(expiration);
    }

    /**
     * Gets Kerberos S4U2self credential for the given user and the PXF configuration server. It first checks in the
     * internal cache for the presence of the cached value. If the credential is already cached and the TGT used when
     * it was cached is still valid and is the same as the current TGT, the method returns the cached value.
     * If there is no cached value or the value is stale, discards the cached value (if any) and obtains
     * a new credential by using Kerberos impersonation flow.
     * @param userName name of the end user
     * @param server name of the PXF configuration server
     * @return the credential previously cached or newly obtained from the impersonation flow
     */
    public GSSCredential getGSSCredential(final String userName, final String server) {
        final String key = userName + ":" + server;
        try {
            int attemptsLeft = 2; // guard against infinite loop, will try 1 get and 1 reload/get for 2 attempts total
            while (attemptsLeft-- > 0) {
                LOG.debug("Getting GSS credential from cache for key {} and user {}", key, userName);
                GSSCredentialHolder credentialHolder = credentialCache.get(key, () -> obtainKerberosProxyCredential(userName));
                GSSCredential credential = credentialHolder.getCredential();
                KerberosTicket cachedTgt = credentialHolder.getTgt();
                KerberosTicket currentTgt = getCurrentTgt();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got GSS credential {} and TGT[{}] from cache, current TGT[{}]",
                            credential.getClass().getName(), cachedTgt.hashCode(), currentTgt.hashCode());
                }
                if (!cachedTgt.isDestroyed() && cachedTgt == currentTgt) {
                    // cached TGT is still good and current, so the cached credential should be good as well
                    return credential;
                }

               // original TGT has been destroyed and replaced with the new one, so the cached credential is not valid
               // get a new credential on retry when cache value will be loaded again using currently active TGT
               // do not destroy the credential explicitly as it still might be used by the other threads
               LOG.debug("Cached GSS credential uses non-current TGT, removing the credential from the cache");
               credentialCache.invalidate(key);
            }
            throw new RuntimeException("Unexpected state: failed to obtain GSS credential.");
        } catch (UncheckedExecutionException | ExecutionException e) {
            // Unwrap the exception
            Exception exception = e.getCause() != null ? (Exception) e.getCause() : e;
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Sets an instance of GSSManager for testing. Having another constructor with this additional parameter
     * does not work with Spring autowiring.
     * @param gssManager an instance of GSS Manager to use
     */
    void setGssManager(GSSManager gssManager) {
        this.gssManager = gssManager;
    }

    /**
     * Initializes credential cache and sets desired entry duration before expiration since the entry has been written.
     * @param expiration entry duration before expiration
     * @return initialized cache
     */
    private Cache<String, GSSCredentialHolder> initCache(Duration expiration) {
        long expirationMillis = expiration.toMillis();
        LOG.info("Creating GSS credential cache with expiration of {}ms", expirationMillis);
        return CacheBuilder.newBuilder()
                .expireAfterWrite(expirationMillis, TimeUnit.MILLISECONDS)
                .removalListener((RemovalListener<String, GSSCredentialHolder>) notification ->
                        LOG.debug("Removed GSS credential from cache for key {} with cause {}",
                                notification.getKey(),
                                notification.getCause().toString()))
                .build();
    }

    /**
     * Obtains an S4U2self credential from KDC for the GP user and the current Kerberos TGT
     * in a holder object that can be stored in the cache.
     * This method must be run under from a "doAs" block from a Subject having a valid Kerberos TGT ticket.
     * @param userName name of the end user
     * @return a holder object containing the obtained credential and the current TGT
     */
    private GSSCredentialHolder obtainKerberosProxyCredential(String userName) {
        try {
            KerberosTicket tgt = getCurrentTgt();
            String userPrincipal = getSessionPrincipal(userName, tgt.getServer().getRealm());
            LOG.debug("Obtaining GSS credential for principal {}", userPrincipal);
            GSSCredential serviceCredentials = gssManager.createCredential(GSSCredential.INITIATE_ONLY);
            GSSName other = gssManager.createName(userPrincipal, GSSName.NT_USER_NAME);
            GSSCredential credential = ((ExtendedGSSCredential) serviceCredentials).impersonate(other);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Obtained GSS credential: {}", credential.toString().replace("\n", " | "));
            }
            return new GSSCredentialHolder(credential, tgt);
        } catch (GSSException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get current KerberosTicket that is a TGT under the Subject running this code.
     * @return the TGT KerberosTicket
     */
    private KerberosTicket getCurrentTgt() {
        KerberosTicket tgt = pxfUgi.getTGT(Subject.getSubject(AccessController.getContext()));
        Preconditions.checkNotNull(tgt, "No TGT found in the Subject.");
        Preconditions.checkState(!tgt.isDestroyed(), "TGT is destroyed.");
        Preconditions.checkState(tgt.isCurrent(), "TGT is not current.");
        LOG.trace("Found TGT in the Subject: {}", tgt); // this will be a multi-line log message
        return tgt;
    }

    /**
     * Get the Kerberos principal name form the end user name and the name of the Kerberos realm.
     * @param userName name of the user
     * @param realm name of the Kerberos realm
     * @return name of the Kerberos principal representing the end user
     */
    private String getSessionPrincipal(String userName, String realm) {
        String realmSuffix = "@" + realm;
        String userPrincipal = userName.endsWith(realmSuffix) ? userName : userName + realmSuffix;
        return userPrincipal;
    }
}
