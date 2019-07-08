package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class S3ProtocolHandlerTest {

    private static final String STRING_PASS_RESOLVER = "org.greenplum.pxf.plugins.hdfs.StringPassResolver";
    private static final String S3_ACCESSOR = S3SelectAccessor.class.getName();
    private static final String DEFAULT_ACCESSOR = "default-accessor";
    private static final String DEFAULT_RESOLVER = "default-resolver";
    private static final String NOT_SUPPORTED = "ERROR";

    private static String[] FORMATS = {"parquet", "text", "csv", "json", "avro"};

    private static String[] EXPECTED_RESOLVER_TEXT_ON = {STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, NOT_SUPPORTED};
    private static String[] EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT = {STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, STRING_PASS_RESOLVER, DEFAULT_RESOLVER};
    private static String[] EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT = {STRING_PASS_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, STRING_PASS_RESOLVER, DEFAULT_RESOLVER};
    private static String[] EXPECTED_RESOLVER_TEXT_OFF = {DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER};

    private static String[] EXPECTED_RESOLVER_GPDB_WRITABLE_ON = {NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED};
    private static String[] EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO = {DEFAULT_RESOLVER, NOT_SUPPORTED, NOT_SUPPORTED, DEFAULT_RESOLVER, DEFAULT_RESOLVER};
    private static String[] EXPECTED_RESOLVER_GPDB_WRITABLE_OFF = {DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER, DEFAULT_RESOLVER};

    private static String[] EXPECTED_ACCESSOR_TEXT_ON = {S3_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, NOT_SUPPORTED};
    private static String[] EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT = {S3_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, S3_ACCESSOR, DEFAULT_ACCESSOR};
    private static String[] EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT = {S3_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, S3_ACCESSOR, DEFAULT_ACCESSOR};
    private static String[] EXPECTED_ACCESSOR_TEXT_OFF = {DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR};

    private static String[] EXPECTED_ACCESSOR_GPDB_WRITABLE_ON = {NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED, NOT_SUPPORTED};
    private static String[] EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO = {DEFAULT_ACCESSOR, NOT_SUPPORTED, NOT_SUPPORTED, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR};
    private static String[] EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF = {DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR, DEFAULT_ACCESSOR};

    private S3ProtocolHandler handler;
    private RequestContext context;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        handler = new S3ProtocolHandler();
        context = new RequestContext();
        context.setAccessor("default-accessor");
        context.setResolver("default-resolver");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("c1", 1, 0, "INT", null, true)); // actual args do not matter
        columns.add(new ColumnDescriptor("c2", 2, 0, "INT", null, true)); // actual args do not matter
        context.setTupleDescription(columns);
    }

    @Test
    public void testTextWithSelectOnResolver() {
        context.addOption("S3-SELECT", "on");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_ON);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitFilterOnlyResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setFilterString("abc");
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitProjectionOnlyResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitFilterAndProjectionResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setFilterString("abc");
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitFilterAndFullProjectionResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setFilterString("abc");
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitAllProjectedResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
    }

    @Test
    public void testTextWithSelectOffResolver() {
        context.addOption("S3-SELECT", "off");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_OFF);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_OFF);
    }

    @Test
    public void testGPDBWritableWithSelectOnResolver() {
        context.addOption("S3-SELECT", "on");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_ON);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitFilterOnlyResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setFilterString("abc");
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitProjectionOnlyResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitFilterAndProjectionResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setFilterString("abc");
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitFilterAndFullProjectionResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setFilterString("abc");
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithNoBenefitResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithNoBenefitAllProjectedResolver() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectOffResolver() {
        context.addOption("S3-SELECT", "off");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_OFF);
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
    }

    @Test
    public void testSelectInvalid() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Invalid value 'foo' for S3-SELECT option");

        context.setFormat("CSV");
        context.addOption("S3-SELECT", "foo");
        handler.getAccessorClassName(context);
    }

    @Test
    public void testSelectOffMissingFormat() {
        context.addOption("S3-SELECT", "off");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
    }

    @Test
    public void testSelectOffUnsupportedFormat() {
        context.addOption("S3-SELECT", "off");
        context.setFormat("custom");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
    }

    @Test
    public void testSelectOnMissingFormat() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("S3-SELECT optimization is not supported for format 'null'");

        context.addOption("S3-SeLeCt", "on");
        handler.getAccessorClassName(context);
    }

    @Test
    public void testSelectOnUnsupportedFormat() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("S3-SELECT optimization is not supported for format 'CUSTOM'");

        context.addOption("S3-SELECT", "on");
        context.setFormat("custom");
        handler.getAccessorClassName(context);
    }


    @Test
    public void testSelectAutoMissingFormat() {
        context.addOption("S3-SELECT", "AUTO");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
    }

    @Test
    public void testSelectAutoUnsupportedFormat() {
        context.addOption("S3-SELECT", "Auto");
        context.setFormat("custom");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
    }

    private void verifyResolvers(RequestContext context, String[] expected) {
        IntStream.range(0, FORMATS.length).forEach(i -> {
            context.setFormat(FORMATS[i]);
            try {
                assertEquals(expected[i], handler.getResolverClassName(context));
            } catch (IllegalArgumentException e) {
                // Check reference
                //noinspection StringEquality
                if (expected[i] != NOT_SUPPORTED) {
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
                // Check reference
                //noinspection StringEquality
                if (expected[i] != NOT_SUPPORTED) {
                    throw e;
                }
            }
        });
    }
}
