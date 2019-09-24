package org.greenplum.pxf.service;

import org.apache.hadoop.security.UserGroupInformation;

import java.util.Objects;

/**
 * Stores information about Kerberos login details for a given configuration server.
 */
public class KerberosLoginSession {

    private String configDirectory;
    private String principalName;
    private String keytabPath;
    private String keytabMd5;
    private UserGroupInformation ugi;

    /**
     * Creates a new session object.
     * @param configDirectory server configuration directory
     * @param principalName Kerberos principal name to use to obtain tokens
     * @param keytabPath full path to a keytab file for the principal
     * @param keytabMd5 MD5 hash of the keytab file
     */
    public KerberosLoginSession(String configDirectory, String principalName, String keytabPath, String keytabMd5) {
        this(configDirectory, principalName, keytabPath, keytabMd5, null);
    }

    /**
     * Creates a new session object.
     * @param configDirectory server configuration directory
     * @param principalName Kerberos principal name to use to obtain tokens
     * @param keytabPath full path to a keytab file for the principal
     * @param keytabMd5 MD5 hash of the keytab file
     * @param ugi UserGroupInformation for the given principal after login to Kerberos was performed
     */
    public KerberosLoginSession(String configDirectory, String principalName, String keytabPath, String keytabMd5, UserGroupInformation ugi) {
        this.configDirectory = configDirectory;
        this.principalName = principalName;
        this.keytabPath = keytabPath;
        this.keytabMd5 = keytabMd5;
        this.ugi = ugi;
    }

    /**
     * Get the login UGI for this session
     * @return the UGI for this session
     */
    public UserGroupInformation getUgi() {
        return ugi;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KerberosLoginSession that = (KerberosLoginSession) o;
        // ugi is not included into expression below as it is a transient derived value
        return Objects.equals(configDirectory, that.configDirectory) &&
                Objects.equals(principalName, that.principalName) &&
                Objects.equals(keytabPath, that.keytabPath) &&
                Objects.equals(keytabMd5, that.keytabMd5);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configDirectory, principalName, keytabPath, keytabMd5);
    }
}
