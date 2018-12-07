package org.greenplum.pxf.plugins.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.greenplum.pxf.api.model.RequestContext;

import java.net.URI;
import java.util.Arrays;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public enum HcfsType {
    HDFS, /*{
        @Override
        public String getDataUri(Configuration configuration, RequestContext context) {
            return (!StringUtils.endsWith(configuration.get(FS_DEFAULT_NAME_KEY), "/") ?
                    configuration.get(FS_DEFAULT_NAME_KEY) + "/" :
                    configuration.get(FS_DEFAULT_NAME_KEY)) +
                    context.getDataSource();
        }
    },*/
    ADL,
    CUSTOM(""), /*{
        @Override
        public String getDataUri(Configuration configuration, RequestContext context) {
            // TODO: do we need the absolute datapath?
            return Utilities.absoluteDataPath(context.getDataSource());
        }
    },*/
    S3,
    S3A,
    S3N,
    FILE {
        @Override
        public String getDataUri(Configuration configuration, RequestContext context) {
            // TODO: evaluate if we want to allow PXF to read from local FS?
            throw new IllegalStateException("core-site.xml is missing or using unsupported file:// as default filesystem");
        }
    };

    /*
    switch (getHcfsType(configuration, context)) {
-            case S3:
-                dataUri = PROTOCOL_S3 + (StringUtils.startsWith(context.getDataSource(), "/") ?
-                        context.getDataSource().substring(1) : context.getDataSource());
-                break;
-            case ADLS:
-                dataUri = PROTOCOL_AZURE + context.getDataSource();
-                break;
-            case HDFS:
-                dataUri = (!StringUtils.endsWith(configuration.get(FS_DEFAULT_NAME_KEY), "/") ?
-                        configuration.get(FS_DEFAULT_NAME_KEY) + "/" :
-                        configuration.get(FS_DEFAULT_NAME_KEY)) + context.getDataSource();
-
-                break;
-            default:
-                dataUri = Utilities.absoluteDataPath(context.getDataSource());
     */

    private static final String FILE_SCHEME = "file";
    private String prefix;

    HcfsType() {
        this(null);
    }

    HcfsType(String prefix) {
        this.prefix = (prefix == null ? name().toLowerCase() : prefix) + "://";
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
        // TODO: Move this logic somewhere else, we need to keep in mind we are trying to
        // TODO: understand what "protocol" we are dealing with

        // if defaultFs is defined and not file://, it takes precedence over protocol

        String protocolFromContext = context.getProtocol();
        URI defaultFS = FileSystem.getDefaultUri(conf);
        String scheme = defaultFS.getScheme();
        if (StringUtils.isBlank(scheme)) {
            throw new IllegalStateException(String.format("No scheme for property %s=%s", FS_DEFAULT_NAME_KEY, defaultFS));
        } else if (FILE_SCHEME.equals(scheme)) {
            // default FS of file:// is likely defaulted, see if context protocol can be used
            if (StringUtils.isNotBlank(protocolFromContext)) {
                scheme = protocolFromContext; // use the value from context
            }
        } else {
            // defaultFS is explicitly set to smth which is not file://, it will take precedence over context protocol
        }

        // now we have scheme, resolve to enum
        return HcfsType.fromString(scheme.toUpperCase());
    }

    /**
     * Returns a fully resolved path include protocol
     *
     * @param context The input data parameters
     * @return an absolute data path
     */
    public String getDataUri(Configuration configuration, RequestContext context) {
        URI defaultFS = FileSystem.getDefaultUri(configuration);

        if (FILE_SCHEME.equals(defaultFS.getScheme())) {
            // if the defaultFS is file://, but enum is not FILE, use enum prefix only
            return prefix + StringUtils.removeStart(context.getDataSource(), "/");
        } else {
            // if the defaultFS is not file://, use it, instead of enum prefix and append user's path
            return StringUtils.removeEnd(defaultFS.toString(), "/") + "/" + StringUtils.removeStart(context.getDataSource(), "/");
        }
    }
}
