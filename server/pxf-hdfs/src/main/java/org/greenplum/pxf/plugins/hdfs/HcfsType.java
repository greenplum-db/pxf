package org.greenplum.pxf.plugins.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.greenplum.pxf.api.model.RequestContext;

import java.net.URI;
import java.util.Arrays;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public enum HcfsType {
    ADL,
    CUSTOM {
        @Override
        public String getDataUri(Configuration configuration, RequestContext context) {
            String contextProtocol = StringUtils.isBlank(context.getProtocol()) ? "" : context.getProtocol() + "://";
            return getDataUriForPrefix(configuration, context, contextProtocol);
        }
    },
    FILE {
        @Override
        public String getDataUri(Configuration configuration, RequestContext context) {
            throw new IllegalStateException("core-site.xml is missing or using unsupported file:// as default filesystem");
        }

        @Override
        public String normalizeDataSource(String dataSource) {
            return dataSource;
        }
    },
    GS,
    HDFS,
    LOCALFILE("file") {
        @Override
        public String normalizeDataSource(String dataSource) {
            return dataSource;
        }
    },
    S3,
    S3A,
    S3N,
    // We prefer WASBS over WASB for Azure Blob Storage,
    // as it uses SSL for communication to Azure servers
    WASBS;

    private static final String FILE_SCHEME = "file";
    private String prefix;

    HcfsType() {
        this(null);
    }

    HcfsType(String prefix) {
        this.prefix = (prefix != null ? prefix : name().toLowerCase()) + "://";
    }

    public static HcfsType fromString(String s) {
        return Arrays.stream(HcfsType.values())
                .filter(v -> v.name().equals(s))
                .findFirst()
                .orElse(HcfsType.CUSTOM);
    }

    /**
     * Returns the type of filesystem being accesses
     * Profile will override the default filesystem configured
     *
     * @param context The input data parameters
     * @return an absolute data path
     */
    public static HcfsType getHcfsType(Configuration conf, RequestContext context) {
        String scheme = getScheme(conf, context);

        // now we have scheme, resolve to enum
        return HcfsType.fromString(scheme.toUpperCase());
    }

    private static String getScheme(Configuration configuration, RequestContext context) {
        // if defaultFs is defined and not file://, it takes precedence over protocol
        String schemeFromContext = context.getProtocol();
        URI defaultFS = FileSystem.getDefaultUri(configuration);
        String defaultFSScheme = defaultFS.getScheme();
        if (StringUtils.isBlank(defaultFSScheme)) {
            throw new IllegalStateException(String.format("No scheme for property %s=%s", FS_DEFAULT_NAME_KEY, defaultFS));
        }

        // protocol from RequestContext will take precedence over defaultFS
        if (StringUtils.isNotBlank(schemeFromContext)) {
            checkForConfigurationMismatch(defaultFSScheme, schemeFromContext);
            return schemeFromContext;
        }

        return defaultFSScheme;
    }

    private static void checkForConfigurationMismatch(String defaultFSScheme, String schemeFromContext) {
        // do not allow protocol mismatch, unless defaultFs has file:// scheme
        if (!FILE_SCHEME.equals(defaultFSScheme) &&
                !StringUtils.equalsIgnoreCase(defaultFSScheme, schemeFromContext)) {
            throw new IllegalArgumentException(
                    String.format("profile protocol (%s) is not compatible with server filesystem (%s)",
                            schemeFromContext, defaultFSScheme));
        }
    }

    /**
     * Returns a fully resolved path include protocol
     *
     * @param context The input data parameters
     * @return an absolute data path
     */
    public String getDataUri(Configuration configuration, RequestContext context) {
        return getDataUriForPrefix(configuration, context, this.prefix);
    }

    /**
     * Returns the normalized data source for the given protocol
     *
     * @param dataSource The path to the data source
     * @return the normalized path to the data source
     */
    public String normalizeDataSource(String dataSource) {
        return StringUtils.removeStart(dataSource, "/");
    }

    protected String getDataUriForPrefix(Configuration configuration, RequestContext context, String prefix) {
        URI defaultFS = FileSystem.getDefaultUri(configuration);

        if (FILE_SCHEME.equals(defaultFS.getScheme())) {
            // if the defaultFS is file://, but enum is not FILE, use enum prefix only
            return prefix + normalizeDataSource(context.getDataSource());
        } else {
            // if the defaultFS is not file://, use it, instead of enum prefix and append user's path
            return StringUtils.removeEnd(defaultFS.toString(), "/") + "/" + StringUtils.removeStart(context.getDataSource(), "/");
        }
    }
}
