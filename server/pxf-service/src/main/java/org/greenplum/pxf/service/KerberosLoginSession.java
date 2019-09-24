package org.greenplum.pxf.service;

import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Objects;

public class KerberosLoginSession {

    private String configDirectory;
    private String principalName;
    private String keytabPath;
    private String keytabMd5;
    private UserGroupInformation ugi;

    public KerberosLoginSession(String configDirectory, String principalName, String keytabPath, String keytabMd5) {
        this(configDirectory, principalName, keytabPath, keytabMd5, null);
    }

    public KerberosLoginSession(String configDirectory, String principalName, String keytabPath, String keytabMd5, UserGroupInformation ugi) {
        this.configDirectory = configDirectory;
        this.principalName = principalName;
        this.keytabPath = keytabPath;
        this.keytabMd5 = keytabMd5;
        this.ugi = ugi;
    }

    public UserGroupInformation getUgi() {
        return ugi;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KerberosLoginSession that = (KerberosLoginSession) o;
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
