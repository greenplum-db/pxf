package org.greenplum.pxf.plugins.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_CONFIG_SERVER_DIRECTORY_PROPERTY;

public enum HcfsType {
    ADL,
    CUSTOM {
        @Override
        public String getDataUri(RequestContext context) {
            String profileScheme = StringUtils.isBlank(context.getProfileScheme()) ? "" : context.getProfileScheme() + "://";
            return getDataUriForPrefix(context, profileScheme);
        }
    },
    FILE {
        @Override
        public String getDataUri(RequestContext context) {
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

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static final String FILE_SCHEME = "file";
    protected String prefix;

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
    public static HcfsType getHcfsType(RequestContext context) {
        String scheme = getScheme(context);

        // now we have scheme, resolve to enum
        HcfsType type = HcfsType.fromString(scheme.toUpperCase());
        // disableSecureTokenRenewal for this configuration if non-secure
        type.getDataUriForPrefix(context.getConfiguration(), "/", scheme);
        return type;
    }

    private static String getScheme(RequestContext context) {
        // if defaultFs is defined and not file://, it takes precedence over protocol
        String schemeFromContext = context.getProfileScheme();
        URI defaultFS = FileSystem.getDefaultUri(context.getConfiguration());
        String defaultFSScheme = defaultFS.getScheme();
        if (StringUtils.isBlank(defaultFSScheme)) {
            throw new IllegalStateException(String.format("No scheme for property %s=%s", FS_DEFAULT_NAME_KEY, defaultFS));
        }

        // protocol from RequestContext will take precedence over defaultFS
        if (StringUtils.isNotBlank(schemeFromContext)) {
            checkForConfigurationMismatch(defaultFSScheme, schemeFromContext, context.getServerName(), context.getConfiguration().get(PXF_CONFIG_SERVER_DIRECTORY_PROPERTY));
            return schemeFromContext;
        }

        return defaultFSScheme;
    }

    private static void checkForConfigurationMismatch(String defaultFSScheme, String schemeFromContext, String serverName, String configurationDirectory) {
        // do not allow protocol mismatch, unless defaultFs has file:// scheme
        if (!FILE_SCHEME.equals(defaultFSScheme) &&
                !StringUtils.equalsIgnoreCase(defaultFSScheme, schemeFromContext)) {
            throw new PxfRuntimeException(
                    String.format("profile '%s' is not compatible with server's '%s' configuration ('%s')",
                            schemeFromContext, serverName, defaultFSScheme),
                    String.format("Ensure that '%s' includes only the configuration files for profile '%s'.",
                            configurationDirectory, schemeFromContext));
        }
    }

    /**
     * Returns a unique fully resolved URI including the protocol for write.
     * The filename is generated with the transaction and segment IDs resulting
     * in <TRANSACTION-ID>_<SEGMENT-ID>. If a COMPRESSION_CODEC is provided, the
     * default codec extension will be appended to the name of the file.
     *
     * @param context The input data parameters
     * @return an absolute data path for write
     */
    public String getUriForWrite(RequestContext context) {
        return getUriForWrite(context, false);
    }

    /**
     * Returns a unique fully resolved URI including the protocol for write.
     * The filename is generated with the transaction and segment IDs resulting
     * in <TRANSACTION-ID>_<SEGMENT-ID>. If a COMPRESSION_CODEC is provided and
     * the skipCodedExtension parameter is false, the default codec extension
     * will be appended to the name of the file.
     *
     * @param context            the input data parameters
     * @param skipCodecExtension true if the codec extension is not desired, false otherwise
     * @return an absolute data path for write
     */
    public String getUriForWrite(RequestContext context, boolean skipCodecExtension) {
        String fileName = StringUtils.removeEnd(getDataUri(context), "/") +
                "/" +
                context.getTransactionId() +
                "_" +
                context.getSegmentId();

        if (!skipCodecExtension) {
            String compressCodec = context.getOption("COMPRESSION_CODEC");
            if (compressCodec != null) {
                // get compression codec default extension
                CodecFactory codecFactory = CodecFactory.getInstance();
                String extension;
                try {
                    extension = codecFactory
                            .getCodec(compressCodec, context.getConfiguration())
                            .getDefaultExtension();
                } catch (IllegalArgumentException e) {
                    LOG.debug("Unable to get extension for codec '{}'", compressCodec);
                    extension = codecFactory
                            .getCodec(compressCodec, CompressionCodecName.UNCOMPRESSED)
                            .getExtension();
                }
                // append codec extension to the filename
                fileName += extension;
            }
        }

        LOG.debug("File name for write: {}", fileName);
        return fileName;
    }

    /**
     * Returns a fully resolved path include protocol
     *
     * @param context The input data parameters
     * @return an absolute data path
     */
    public String getDataUri(RequestContext context) {
        return getDataUriForPrefix(context, this.prefix);
    }

    /**
     * Returns a fully resolved path include protocol
     *
     * @param path The path to file
     * @return an absolute data path
     */
    public String getDataUri(Configuration configuration, String path) {
        return getDataUriForPrefix(configuration, path, this.prefix);
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

    protected String getDataUriForPrefix(RequestContext context, String scheme) {
        return getDataUriForPrefix(context.getConfiguration(), context.getDataSource(), scheme);
    }

    protected String getDataUriForPrefix(Configuration configuration, String dataSource, String scheme) {

        URI defaultFS = FileSystem.getDefaultUri(configuration);

        String uri;
        String normalizedDataSource = normalizeDataSource(dataSource);

        if (FILE_SCHEME.equals(defaultFS.getScheme())) {
            // if the defaultFS is file://, but enum is not FILE, use enum scheme only
            uri = StringUtils.removeEnd(scheme, "://") + "://" + normalizedDataSource;
        } else {
            // if the defaultFS is not file://, use it, instead of enum scheme and append user's path
            uri = StringUtils.removeEnd(defaultFS.toString(), "/") + "/" + normalizedDataSource;
        }

        disableSecureTokenRenewal(uri, configuration);
        return uri;
    }

    /**
     * For secured cluster, circumvent token renewal for non-HDFS hcfs access (such as s3 etc)
     *
     * @param uri           URI of the resource to access
     * @param configuration configuration used for HCFS operations
     */
    protected void disableSecureTokenRenewal(String uri, Configuration configuration) {
        if (Utilities.isSecurityEnabled(configuration) || StringUtils.isBlank(uri))
            return;

        // find the "host" that TokenCache will check against the exclusion list, for cloud file systems (like S3)
        // it might actually be a bucket in the full resource path
        String host = Utilities.getHost(uri);
        if (host != null) {
            LOG.debug("Disabling token renewal for host {} for path {}", host, uri);
            // disable token renewal for the "host" in the path
            configuration.set(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE, host);
        }
    }
}
