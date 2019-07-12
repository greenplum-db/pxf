package org.greenplum.pxf.plugins.s3;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.ProtocolHandler;
import org.greenplum.pxf.api.model.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

/**
 * Implementation of ProtocolHandler for "s3" protocol.
 */
public class S3ProtocolHandler implements ProtocolHandler {

    public static final String S3_SELECT_OPTION = "S3-SELECT";

    private static final Logger LOG = LoggerFactory.getLogger(S3ProtocolHandler.class);
    private static final Set<String> SUPPORTED_FORMATS = Sets.newHashSet("TEXT", "CSV", "PARQUET", "JSON");
    private static final Set<SupportMatrixEntry> SUPPORT_MATRIX = Sets.newHashSet(
            new SupportMatrixEntry("PARQUET", OutputFormat.TEXT, S3Mode.ON),
            new SupportMatrixEntry("TEXT", OutputFormat.TEXT, S3Mode.ON),
            new SupportMatrixEntry("CSV", OutputFormat.TEXT, S3Mode.ON),
            new SupportMatrixEntry("JSON", OutputFormat.TEXT, S3Mode.ON),
            new SupportMatrixEntry("PARQUET", OutputFormat.TEXT, S3Mode.AUTO),
            new SupportMatrixEntry("TEXT", OutputFormat.TEXT, S3Mode.AUTO),
            new SupportMatrixEntry("CSV", OutputFormat.TEXT, S3Mode.AUTO),
            new SupportMatrixEntry("JSON", OutputFormat.TEXT, S3Mode.AUTO),
            new SupportMatrixEntry("TEXT", OutputFormat.TEXT, S3Mode.OFF),
            new SupportMatrixEntry("CSV", OutputFormat.TEXT, S3Mode.OFF),
            new SupportMatrixEntry("PARQUET", OutputFormat.GPDBWritable, S3Mode.AUTO),
            new SupportMatrixEntry("JSON", OutputFormat.GPDBWritable, S3Mode.AUTO),
            new SupportMatrixEntry("PARQUET", OutputFormat.GPDBWritable, S3Mode.OFF),
            new SupportMatrixEntry("JSON", OutputFormat.GPDBWritable, S3Mode.OFF)
    );

    private static final String STRING_PASS_RESOLVER = "org.greenplum.pxf.plugins.hdfs.StringPassResolver";
    private static final String HCFS_FILE_FRAGMENTER = "org.greenplum.pxf.plugins.hdfs.HdfsFileFragmenter";

    @Override
    public String getFragmenterClassName(RequestContext context) {
        String fragmenter = context.getFragmenter(); // default to fragmenter defined by the profile
        if (useS3Select(context)) {
            fragmenter = HCFS_FILE_FRAGMENTER;
        }
        LOG.debug("Determined to use {} fragmenter", fragmenter);
        return fragmenter;
    }

    @Override
    public String getAccessorClassName(RequestContext context) {
        String accessor = context.getAccessor(); // default to accessor defined by the profile
        if (useS3Select(context)) {
            accessor = S3SelectAccessor.class.getName();
        }
        LOG.debug("Determined to use {} accessor", accessor);
        return accessor;
    }

    @Override
    public String getResolverClassName(RequestContext context) {
        String resolver = context.getResolver(); // default to resolver defined by the profile
        if (useS3Select(context)) {
            resolver = STRING_PASS_RESOLVER;
        }
        LOG.debug("Determined to use {} resolver", resolver);
        return resolver;

    }

    private boolean useS3Select(RequestContext context) {
        String format = StringUtils.upperCase(context.getFormat());
        String compressionType = context.getOption(S3SelectAccessor.COMPRESSION_TYPE);
        OutputFormat outputFormat = context.getOutputFormat();
        S3Mode selectMode = S3Mode.fromString(context.getOption(S3_SELECT_OPTION));
        boolean isS3SelectSupportedFormat = SUPPORTED_FORMATS.contains(format);

        if (!isS3SelectSupportedFormat) {
            if (selectMode == S3Mode.ON) {
                throw new IllegalArgumentException(String.format("S3-SELECT optimization is not supported for format '%s'", format));
            }
            return false;
        }

        switch (selectMode) {
            case ON:
                return formatSupported(outputFormat, format, S3Mode.ON, compressionType, true);
            case AUTO:
                // if supported for ON and beneficial, use it
                // if supported for ON and not beneficial -> if supported for OFF -> use OFF, else use ON
                // if not supported for ON -> if supported for OFF -> use OFF, else ERROR out
                if (formatSupported(outputFormat, format, S3Mode.ON, compressionType, false)) {
                    if (willBenefitFromSelect(context) || hasFormatOptions(format, context)) {
                        return true;
                    } else {
                        return !formatSupported(outputFormat, format, S3Mode.OFF, compressionType, false);
                    }
                } else {
                    return !formatSupported(outputFormat, format, S3Mode.OFF, compressionType, true);
                }
            default:
                return false;
        }
    }

    /**
     * Returns true if there are any format options for the given format
     *
     * @param format  the format
     * @param context the request context
     * @return true if there are any format options for the given format, false otherwise
     */
    private boolean hasFormatOptions(String format, RequestContext context) {
        // TODO: FDW: this will not be required in the FDW framework
        if (StringUtils.equals("CSV", format) || StringUtils.equals("TEXT", format)) {
            String fileHeaderInfo = context.getOption(S3SelectAccessor.FILE_HEADER_INFO);
            // There is a problem when the FORMAT options are different from the default
            // options we use for CSV parsing. The formatter information is not passed
            // from the client in the case of the external table framework. We cannot
            // guarantee that they will be in sync.
            return StringUtils.isNotEmpty(context.getOption(S3SelectAccessor.FIELD_DELIMITER)) ||
                    StringUtils.isNotEmpty(context.getOption(S3SelectAccessor.QUOTE_CHARACTER)) ||
                    StringUtils.isNotEmpty(context.getOption(S3SelectAccessor.QUOTE_ESCAPE_CHARACTER)) ||
                    StringUtils.isNotEmpty(context.getOption(S3SelectAccessor.RECORD_DELIMITER)) ||
                    StringUtils.equalsIgnoreCase(fileHeaderInfo, S3SelectAccessor.FILE_HEADER_INFO_IGNORE) ||
                    StringUtils.equalsIgnoreCase(fileHeaderInfo, S3SelectAccessor.FILE_HEADER_INFO_USE);
        }
        return false;
    }

    /**
     * Determines if the using S3-SELECT will be beneficial for performance, such as where there is
     * a column projection or a predicate pushdown for the given query
     *
     * @param context request context
     * @return true if using S3-SELECT will be beneficial, false otherwise
     */
    private boolean willBenefitFromSelect(RequestContext context) {
        return context.hasFilter() || context.hasColumnProjection();
    }

    /**
     * Determines if the given data format can be retrieved from S3 using S3-SELECT protocol
     * and sent back to Greenplum using given OutputFormat
     *
     * @param outputFormat    output format
     * @param format          data format
     * @param selectMode      s3-select mode requested by a user
     * @param compressionType the remote file compression type (i.e GZIP or BZip2)
     * @param raiseException  true if an exception needs to be raised if the format is not supported, false otherwise
     * @return true if the data format is supported to be retrieved with s3select protocol
     */
    private boolean formatSupported(OutputFormat outputFormat, String format, S3Mode selectMode, String compressionType, boolean raiseException) {
        boolean supported = SUPPORT_MATRIX.contains(new SupportMatrixEntry(format, outputFormat, selectMode));
        if (!supported && raiseException) {
            throw new IllegalArgumentException(String.format("S3-SELECT optimization is not supported for format '%s'", format));
        }

        return supported;
    }

    /**
     * Enumeration of modes a user can configure for using S3-SELECT optimization
     */
    enum S3Mode {
        /**
         * ON mode will apply S3-SELECT access always, no matter whether it's optimal or not
         */
        ON,

        /**
         * OFF mode will make sure S3-SELECT access is never applied, even if it would be the optimal one
         */
        OFF,

        /**
         * AUTO mode will apply S3-SELECT only if it will result in more optimal execution
         */
        AUTO;

        /**
         * Default mode if the value is not provided
         */
        private static final S3Mode DEFAULT_MODE = OFF;

        /**
         * Looks up the enumeration instance based on the name, using default if name is empty or null
         *
         * @param modeName name of the mode
         * @return enumeration instance
         */
        static S3Mode fromString(String modeName) {
            if (StringUtils.isBlank(modeName)) {
                return DEFAULT_MODE;
            }
            try {
                return valueOf(modeName.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("Invalid value '%s' for %s option", modeName, S3_SELECT_OPTION), e);
            }
        }
    }

    /**
     * Encapsulates the file format, outputFormat, and S3Mode, as an entry
     * in the SUPPORT_MATRIX
     */
    private static class SupportMatrixEntry {
        private final String format;
        private final OutputFormat outputFormat;
        private final S3Mode s3Mode;

        SupportMatrixEntry(String format, OutputFormat outputFormat, S3Mode s3Mode) {
            this.format = format;
            this.outputFormat = outputFormat;
            this.s3Mode = s3Mode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SupportMatrixEntry that = (SupportMatrixEntry) o;
            return Objects.equals(format, that.format) &&
                    Objects.equals(outputFormat, that.outputFormat) &&
                    Objects.equals(s3Mode, that.s3Mode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(format, outputFormat, s3Mode);
        }
    }
}
