package org.apache.hadoop.security;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import javax.security.auth.Subject;
import java.util.Objects;

/**
 * Stores information about Kerberos login details for a given configuration server.
 */
public class LoginSession {

    private String configDirectory;
    private String principalName;
    private String keytabPath;
    private UserGroupInformation loginUser;
    private Subject subject;
    private User user;

    private long kerberosMinSecondsBeforeRelogin;

    /**
     * Creates a new session object.
     *
     * @param configDirectory server configuration directory
     * @param principalName   Kerberos principal name to use to obtain tokens
     * @param keytabPath      full path to a keytab file for the principal
     */
    public LoginSession(String configDirectory, String principalName, String keytabPath) {
        this(configDirectory, principalName, keytabPath, null, null, 0);
    }

    /**
     * Creates a new session object.
     *
     * @param configDirectory                 server configuration directory
     * @param principalName                   Kerberos principal name to use to obtain tokens
     * @param keytabPath                      full path to a keytab file for the principal
     * @param loginUser                       UserGroupInformation for the given principal after login to Kerberos was performed
     * @param subject                         the subject
     * @param kerberosMinSecondsBeforeRelogin the number of seconds before re-login
     */
    public LoginSession(String configDirectory, String principalName, String keytabPath, UserGroupInformation loginUser,
                        Subject subject, long kerberosMinSecondsBeforeRelogin) {
        this.configDirectory = configDirectory;
        this.principalName = principalName;
        this.keytabPath = keytabPath;
        this.loginUser = loginUser;
        this.subject = subject;
        if (subject != null) {
            this.user = subject.getPrincipals(User.class).iterator().next();
        }
        this.kerberosMinSecondsBeforeRelogin = kerberosMinSecondsBeforeRelogin;
    }

    /**
     * Get the login UGI for this session
     *
     * @return the UGI for this session
     */
    public UserGroupInformation getLoginUser() {
        return loginUser;
    }

    public long getKerberosMinSecondsBeforeRelogin() {
        return kerberosMinSecondsBeforeRelogin;
    }

    public Subject getSubject() {
        return subject;
    }

    public User getUser() {
        return user;
    }

    public String getKeytabPath() {
        return keytabPath;
    }

    public String getPrincipalName() {
        return principalName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoginSession that = (LoginSession) o;
        // ugi and subject are not included into expression below as they are transient derived values
        return Objects.equals(configDirectory, that.configDirectory) &&
                Objects.equals(principalName, that.principalName) &&
                Objects.equals(keytabPath, that.keytabPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configDirectory, principalName, keytabPath);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("config", configDirectory)
                .append("principal", principalName)
                .append("keytab", keytabPath)
                .toString();
    }
}
