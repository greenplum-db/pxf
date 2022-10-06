package org.greenplum.pxf.plugins.s3;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class S3ProtocolHandlerTest {

    private static final String FILE_FRAGMENTER = "org.greenplum.pxf.plugins.hdfs.HdfsFileFragmenter";
    private static final String STRING_PASS_RESOLVER = "org.greenplum.pxf.plugins.hdfs.StringPassResolver";
    private static final String S3_ACCESSOR = S3SelectAccessor.class.getName();
    private static final String DEFAULT_ACCESSOR = "default-accessor";
    private static final String DEFAULT_RESOLVER = "default-resolver";
    private static final String DEFAULT_FRAGMENTER = "default-fragmenter";
    private static final String NOT_SUPPORTED = "ERROR";

    private static final String[] FORMATS = {"parquet", "text", "csv", "json", "avro"};

    private static final String[] EXPECTED_RESOLVER_TEXT_ON = {STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, NOT_SUPPORTED};
    private static final String[] EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT = {STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, DEFAULT_RESOLVER};
    private static final String[] EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT = {DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, STRING_PASS_RESOLVER, DEFAULT_RESOLVER};
    private static final String[] EXPECTED_RESOLVER_TEXT_OFF = {DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER};

    private static final String[] EXPECTED_RESOLVER_GPDB_WRITABLE_ON = {NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED};
    private static final String[] EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO = {DEFAULT_RESOLVER, NOT_SUPPORTED, NOT_SUPPORTED, DEFAULT_RESOLVER, DEFAULT_RESOLVER};
    private static final String[] EXPECTED_RESOLVER_GPDB_WRITABLE_OFF = {DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER};

    private static final String[] EXPECTED_FRAGMENTER_TEXT_ON = {FILE_FRAGMENTER, FILE_FRAGMENTER, FILE_FRAGMENTER, FILE_FRAGMENTER, NOT_SUPPORTED};
    private static final String[] EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT = {FILE_FRAGMENTER, FILE_FRAGMENTER, FILE_FRAGMENTER, FILE_FRAGMENTER, DEFAULT_FRAGMENTER};
    private static final String[] EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT = {DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, FILE_FRAGMENTER, DEFAULT_FRAGMENTER};
    private static final String[] EXPECTED_FRAGMENTER_TEXT_OFF = {DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER};

    private static final String[] EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON = {NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED};
    private static final String[] EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO = {DEFAULT_FRAGMENTER, NOT_SUPPORTED, NOT_SUPPORTED, DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER};
    private static final String[] EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF = {DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER};

    private static final String[] EXPECTED_ACCESSOR_TEXT_ON = {S3_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, NOT_SUPPORTED};
    private static final String[] EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT = {S3_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, DEFAULT_ACCESSOR};
    private static final String[] EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT = {DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, S3_ACCESSOR, DEFAULT_ACCESSOR};
    private static final String[] EXPECTED_ACCESSOR_TEXT_OFF = {DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR};

    private static final String[] EXPECTED_ACCESSOR_GPDB_WRITABLE_ON = {NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED};
    private static final String[] EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO = {DEFAULT_ACCESSOR, NOT_SUPPORTED, NOT_SUPPORTED, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR};
    private static final String[] EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF = {DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR};

    private static final String[] EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS = {DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, S3_ACCESSOR, DEFAULT_ACCESSOR};
    private static final String[] EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS = {DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, STRING_PASS_RESOLVER, DEFAULT_RESOLVER};
    private static final String[] EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS = {DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, DEFAULT_FRAGMENTER, FILE_FRAGMENTER, DEFAULT_FRAGMENTER};

    private static final String[] EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT_HAS_HEADER = {DEFAULT_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, DEFAULT_ACCESSOR};
    private static final String[] EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT_HAS_HEADER = {DEFAULT_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, DEFAULT_RESOLVER};
    private static final String[] EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT_HAS_HEADER = {DEFAULT_FRAGMENTER, FILE_FRAGMENTER, FILE_FRAGMENTER, FILE_FRAGMENTER, DEFAULT_FRAGMENTER};

    private S3ProtocolHandler handler;
    private RequestContext context;

    @BeforeEach
    public void before() {
        handler = new S3ProtocolHandler();
        context = new RequestContext();
        context.setFragmenter("default-fragmenter");
        context.setAccessor("default-accessor");
        context.setResolver("default-resolver");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("c1", 1, 0, "INT", null, true)); // actual args do not matter
        columns.add(new ColumnDescriptor("c2", 2, 0, "INT", null, true)); // actual args do not matter
        context.setTupleDescription(columns);
    }

    @Test
    public void testTextWithSelectOnResolver() {
        context.addOption("S3_SELECT", "on");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_ON);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_ON);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitFilterOnlyResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setFilterString("abc");
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitProjectionOnlyResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitFilterAndProjectionResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setFilterString("abc");
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitFilterAndFullProjectionResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setFilterString("abc");
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitResolverWithDelimiterOption() {
        context.addOption("S3_SELECT", "auto");
        context.addOption("DELIMITER", "|");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitResolverWithQuoteCharacterOption() {
        context.addOption("S3_SELECT", "auto");
        context.addOption("QUOTE", "'");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitResolverWithQuoteEscapeCharacterOption() {
        context.addOption("S3_SELECT", "auto");
        context.addOption("ESCAPE", "\\");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitResolverWithRecordDelimiterOption() {
        context.addOption("S3_SELECT", "auto");
        context.addOption("NEWLINE", "\r");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT_HAS_FORMAT_OPTIONS);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitResolverWithFileHeaderInfoOptionUSE() {
        context.addOption("S3_SELECT", "auto");
        context.addOption("FILE_HEADER", "USE");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT_HAS_HEADER);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT_HAS_HEADER);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT_HAS_HEADER);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitResolverWithFileHeaderInfoOptionIGNORE() {
        context.addOption("S3_SELECT", "auto");
        context.addOption("FILE_HEADER", "IGNORE");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT_HAS_HEADER);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT_HAS_HEADER);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT_HAS_HEADER);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitResolverWithFileHeaderInfoOptionNONE() {
        context.addOption("S3_SELECT", "auto");
        context.addOption("FILE_HEADER", "NONE");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitAllProjectedResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT);
    }

    @Test
    public void testTextWithSelectOffResolver() {
        context.addOption("S3_SELECT", "off");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_OFF);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_OFF);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_OFF);
    }

    @Test
    public void testGPDBWritableWithSelectOnResolver() {
        context.addOption("S3_SELECT", "on");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_ON);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitFilterOnlyResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setFilterString("abc");
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitProjectionOnlyResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitFilterAndProjectionResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setFilterString("abc");
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitFilterAndFullProjectionResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setFilterString("abc");
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithNoBenefitResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithNoBenefitAllProjectedResolver() {
        context.addOption("S3_SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectOffResolver() {
        context.addOption("S3_SELECT", "off");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_OFF);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF);
    }

    @Test
    public void testTextSelectOptionMissing() {
        context.setFormat("CSV");
        context.setOutputFormat(OutputFormat.TEXT);
        assertEquals("default-accessor", handler.getAccessorClassName(context));
    }

    @Test
    public void testGPDBWritableSelectOptionMissing() {
        context.setFormat("CSV");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
        assertEquals("default-fragmenter", handler.getFragmenterClassName(context));
    }

    @Test
    public void testSelectInvalid() {
        context.setFormat("CSV");
        context.addOption("S3_SELECT", "foo");

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> handler.getAccessorClassName(context));
        assertEquals("Invalid value 'foo' for S3_SELECT option", e.getMessage());
    }

    @Test
    public void testSelectOffMissingFormat() {
        context.addOption("S3_SELECT", "off");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
    }

    @Test
    public void testSelectOffUnsupportedFormat() {
        context.addOption("S3_SELECT", "off");
        context.setFormat("custom");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
    }

    @Test
    public void testSelectOnMissingFormat() {
        context.addOption("S3_SeLeCt", "on");

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> handler.getAccessorClassName(context));
        assertEquals("S3_SELECT optimization is not supported for format 'null'. Use S3_SELECT=OFF for this format", e.getMessage());
    }

    @Test
    public void testSelectOnUnsupportedFormat() {
        context.addOption("S3_SELECT", "on");
        context.setFormat("custom");

        Exception e = assertThrows(RuntimeException.class,
                () -> handler.getAccessorClassName(context));
        assertEquals("S3_SELECT optimization is not supported for format 'CUSTOM'. Use S3_SELECT=OFF for this format", e.getMessage());
    }

    @Test
    public void testSelectAutoMissingFormat() {
        context.addOption("S3_SELECT", "AUTO");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
    }

    @Test
    public void testSelectAutoUnsupportedFormat() {
        context.addOption("S3_SELECT", "Auto");
        context.setFormat("custom");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
    }

    @Test
    public void testTextWithSelectOnAndWithSupportedCompressionType() {
        context.addOption("S3_SELECT", "on");
        context.addOption(S3SelectAccessor.COMPRESSION_TYPE, "gZiP");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_ON);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_ON);
    }

    @Test
    public void testTextWithSelectOnAndWithNoSupportedCompressionType() {
        context.addOption("S3_SELECT", "on");
        context.addOption(S3SelectAccessor.COMPRESSION_TYPE, "");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_ON);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_ON);
    }

    @Test
    public void testWithSelectOnAndWithUnSupportedCompressionType() {
        context.addOption("S3_SELECT", "on");
        context.addOption(S3SelectAccessor.COMPRESSION_TYPE, "foo");
        // EXPECTED_RESOLVER_GPDB_WRITABLE_ON is used as its values are desirable as expected value
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_ON);
    }

    @Test
    public void testWithSelectOffAndWithUnSupportedCompressionType() {
        context.addOption("S3_SELECT", "off");
        context.addOption(S3SelectAccessor.COMPRESSION_TYPE, "foo");
        // EXPECTED_RESOLVER_GPDB_WRITABLE_OFF is used as its values are desirable as expected value
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_OFF);
    }

    @Test
    public void testTextIdentifierAndSelectOff() {
        context.addOption("S3_SELECT", "off");
        context.addOption("IDENTIFIER", "c1");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_OFF);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_OFF);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_OFF);
    }

    @Test
    public void testTextIdentifierNoParallelReadAndSelectOff() {
        context.addOption("S3_SELECT", "off");
        context.addOption("IDENTIFIER", "c1");
        context.addOption("USE_PARALLEL_READ", "false");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_OFF);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_OFF);
        String[] EXPECTED_FRAGMENTERS = EXPECTED_FRAGMENTER_TEXT_OFF.clone();
        EXPECTED_FRAGMENTERS[3] = FILE_FRAGMENTER; // index 3 is json
        verifyFragmenters(context, EXPECTED_FRAGMENTERS);
    }

    @Test
    public void testTextIdentifierAndSelectOn() {
        // s3 options should override multiline json fragmenter option
        context.addOption("S3_SELECT", "on");
        context.addOption("IDENTIFIER", "c1");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_ON);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_ON);
    }

    @Test
    public void testTextIdentifierNoParallelReadAndSelectOn() {
        // s3 options should override multiline json fragmenter option
        context.addOption("S3_SELECT", "on");
        context.addOption("IDENTIFIER", "c1");
        context.addOption("USE_PARALLEL_READ", "false");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_ON);
        String[] EXPECTED_FRAGMENTERS = EXPECTED_FRAGMENTER_TEXT_ON.clone();
        EXPECTED_FRAGMENTERS[3] = FILE_FRAGMENTER; // index 3 is json
        verifyFragmenters(context, EXPECTED_FRAGMENTERS);
    }

    @Test
    public void testTextIdentifierAndSelectAuto() {
        // s3 options should override multiline json fragmenter option
        context.addOption("S3_SELECT", "auto");
        context.addOption("IDENTIFIER", "c1");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT);
    }


    @Test
    public void testTextIdentifierNoParallelReadAndSelectAuto() {
        context.addOption("S3_SELECT", "auto");
        context.addOption("IDENTIFIER", "c1");
        context.addOption("USE_PARALLEL_READ", "false");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
        String[] EXPECTED_FRAGMENTERS = EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT.clone();
        EXPECTED_FRAGMENTERS[3] = FILE_FRAGMENTER; // index 3 is json
        verifyFragmenters(context, EXPECTED_FRAGMENTERS);
    }

    private void verifyFragmenters(RequestContext context, String[] expected) {
        IntStream.range(0, FORMATS.length).forEach(i -> {
            context.setFormat(FORMATS[i]);
            try {
                assertEquals(expected[i], handler.getFragmenterClassName(context));
            } catch (IllegalArgumentException e) {
                if (!expected[i].equals(NOT_SUPPORTED)) {
                    throw e;
                }
            }
        });
    }

    private void verifyResolvers(RequestContext context, String[] expected) {
        IntStream.range(0, FORMATS.length).forEach(i -> {
            context.setFormat(FORMATS[i]);
            try {
                assertEquals(expected[i], handler.getResolverClassName(context));
            } catch (IllegalArgumentException e) {
                if (!expected[i].equals(NOT_SUPPORTED)) {
                    throw e;
                }
            }
        });
    }

    private void verifyAccessors(RequestContext context, String[] expected) {
        IntStream.range(0, FORMATS.length).forEach(i -> {
            context.setFormat(FORMATS[i]);
            try {
                assertEquals(expected[i], handler.getAccessorClassName(context));
            } catch (IllegalArgumentException e) {
                if (!expected[i].equals(NOT_SUPPORTED)) {
                    throw e;
                }
            }
        });
    }
}
