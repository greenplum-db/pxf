package org.greenplum.pxf.plugins.s3;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.ProtocolHandler;
import org.greenplum.pxf.api.model.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.greenplum.pxf.plugins.s3.S3SelectAccessor.FILE_HEADER_INFO_IGNORE;
import static org.greenplum.pxf.plugins.s3.S3SelectAccessor.FILE_HEADER_INFO_USE;

/**
 * Implementation of ProtocolHandler for "s3" protocol.
 */
public class S3ProtocolHandler implements ProtocolHandler {

    public static final String S3_SELECT_OPTION = "S3_SELECT";

    private static final Logger LOG = LoggerFactory.getLogger(S3ProtocolHandler.class);
    private static final Set<String> SUPPORTED_FORMATS = Sets.newHashSet("TEXT", "CSV", "PARQUET", "JSON");
    private static final Set<String> SUPPORTED_COMPRESSION_TYPE_FOR_TEXT = Sets.newHashSet("GZIP", "BZIP2");
    private static final Set<String> SUPPORTED_COMPRESSION_TYPE_FOR_PARQUET = Sets.newHashSet("GZIP", "SNAPPY");
    private static final Map<String, Set<String>> SUPPORTED_COMPRESSION_TYPES =
            Collections.unmodifiableMap(new HashMap<String, Set<String>>() {{
                put("TEXT", SUPPORTED_COMPRESSION_TYPE_FOR_TEXT);
                put("CSV", SUPPORTED_COMPRESSION_TYPE_FOR_TEXT);
                put("JSON", SUPPORTED_COMPRESSION_TYPE_FOR_TEXT);
                put("PARQUET", SUPPORTED_COMPRESSION_TYPE_FOR_PARQUET);
            }});

    private static final Set<SupportMatrixEntry> SUPPORT_MATRIX = Sets.newHashSet(
            new SupportMatrixEntry("PARQUET", OutputFormat.TEXT, S3Mode.ON),
            new SupportMatrixEntry("TEXT", OutputFormat.TEXT, S3Mode.ON),
            new SupportMatrixEntry("CSV", OutputFormat.TEXT, S3Mode.ON),
            new SupportMatrixEntry("JSON", OutputFormat.TEXT, S3Mode.ON),
            new SupportMatrixEntry("PARQUET", OutputFormat.TEXT, S3Mode.OFF),
            new SupportMatrixEntry("TEXT", OutputFormat.TEXT, S3Mode.OFF),
            new SupportMatrixEntry("CSV", OutputFormat.TEXT, S3Mode.OFF),
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
        String compressionType = StringUtils.upperCase(context.getOption(S3SelectAccessor.COMPRESSION_TYPE));
        OutputFormat outputFormat = context.getOutputFormat();
        S3Mode selectMode = S3Mode.fromString(context.getOption(S3_SELECT_OPTION));
        boolean isS3SelectSupportedFormat = SUPPORTED_FORMATS.contains(format);

        if (!isS3SelectSupportedFormat) {
            if (selectMode == S3Mode.ON) {
                throw new IllegalArgumentException(String.format("%s optimization is not supported for format '%s'. Use %s=OFF for this format", S3_SELECT_OPTION, format, S3_SELECT_OPTION));
            }
            return false;
        }

        boolean isS3SelectSupportedCompressionType = StringUtils.isBlank(compressionType) ||
                SUPPORTED_COMPRESSION_TYPES.get(format).contains(compressionType);

        if (!isS3SelectSupportedCompressionType) {
            if (selectMode == S3Mode.ON) {
                throw new IllegalArgumentException(String.format("%s optimization is not supported for compression type '%s'. Use %s=OFF for this compression codec", S3_SELECT_OPTION, compressionType, S3_SELECT_OPTION));
            }
            return false;
        }

        switch (selectMode) {
            case ON:
                return formatSupported(outputFormat, format, S3Mode.ON, true);
            case AUTO:
                // if supported for ON and beneficial, use it
                // if file has header line, use S3 Select because reading with headers is not supported
                // if supported for ON and not beneficial -> if supported for OFF -> use OFF, else use ON
                // if not supported for ON -> if supported for OFF -> use OFF, else ERROR out
                if (formatSupported(outputFormat, format, S3Mode.ON, false)) {
                    if (willBenefitFromSelect(context) || fileHasHeaderLine(format, context)) {
                        return true;
                    } else {
                        return !formatSupported(outputFormat, format, S3Mode.OFF, false);
                    }
                } else {
                    return !formatSupported(outputFormat, format, S3Mode.OFF, true);
                }
            default:
                return false;
        }
    }

    /**
     * For CSV or TEXT files, it returns true if the file has headers
     *
     * @param context the request context
     * @return true if the CSV/TEXT file has headers, false otherwise
     */
    private boolean fileHasHeaderLine(String format, RequestContext context) {
        if (StringUtils.equals("CSV", format) || StringUtils.equals("TEXT", format)) {
            // Currently, when you create a PXF external table,
            // you cannot use the HEADER option in your formatter
            // specification
            String fileHeaderInfo = StringUtils.upperCase(
                    context.getOption(S3SelectAccessor.FILE_HEADER_INFO));

            return StringUtils.equals(FILE_HEADER_INFO_IGNORE, fileHeaderInfo) ||
                    StringUtils.equals(FILE_HEADER_INFO_USE, fileHeaderInfo);
        }

        return false;
    }

    /**
     * Determines if the using S3_SELECT will be beneficial for performance, such as where there is
     * a column projection or a predicate pushdown for the given query
     *
     * @param context request context
     * @return true if using S3_SELECT will be beneficial, false otherwise
     */
    private boolean willBenefitFromSelect(RequestContext context) {
        return context.hasFilter() || context.hasColumnProjection();
    }

    /**
     * Determines if the given data format can be retrieved from S3 using S3_SELECT protocol
     * and sent back to Greenplum using given OutputFormat
     *
     * @param outputFormat   output format
     * @param format         data format
     * @param selectMode     s3_select mode requested by a user
     * @param raiseException true if an exception needs to be raised if the format is not supported, false otherwise
     * @return true if the data format is supported to be retrieved with s3select protocol
     */
    private boolean formatSupported(OutputFormat outputFormat, String format, S3Mode selectMode, boolean raiseException) {
        boolean supported = SUPPORT_MATRIX.contains(new SupportMatrixEntry(format, outputFormat, selectMode));
        if (!supported && raiseException) {
            throw new IllegalArgumentException(String.format("%s optimization is not supported for format '%s'", S3_SELECT_OPTION, format));
        }

        return supported;
    }

    /**
     * Enumeration of modes a user can configure for using S3_SELECT optimization
     */
    enum S3Mode {
        /**
         * ON mode will apply S3_SELECT access always, no matter whether it's optimal or not
         */
        ON,

        /**
         * OFF mode will make sure S3_SELECT access is never applied, even if it would be the optimal one
         */
        OFF,

        /**
         * AUTO mode will apply S3_SELECT only if it will result in more optimal execution
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
