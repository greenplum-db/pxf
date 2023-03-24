package org.greenplum.pxf.api.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestContextTest {

    private RequestContext context;

    @BeforeEach
    public void before() {
        context = new RequestContext();
    }

    @Test
    public void testDefaultValues() {
        assertEquals("default", context.getServerName());
        assertEquals(0, context.getStatsMaxFragments());
        assertEquals(0, context.getStatsSampleRatio(), 0.1);
    }

    @Test
    public void testSettingBlankToDefaultServerName() {
        context.setServerName("      ");
        assertEquals("default", context.getServerName());
    }

    @Test
    public void testInvalidServerName() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.setServerName("foo,bar"));
        assertEquals("Invalid server name 'foo,bar'", e.getMessage());
    }

    @Test
    public void testStatsMaxFragmentsFailsOnZero() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.setStatsMaxFragments(0));
        assertEquals("Wrong value '0'. STATS_MAX_FRAGMENTS must be a positive integer", e.getMessage());
    }

    @Test
    public void testStatsMaxFragmentsFailsOnNegative() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.setStatsMaxFragments(-1));
        assertEquals("Wrong value '-1'. STATS_MAX_FRAGMENTS must be a positive integer", e.getMessage());
    }

    @Test
    public void testStatsSampleRatioFailsOnOver1() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.setStatsSampleRatio(1.1f));
        assertEquals("Wrong value '1.1'. STATS_SAMPLE_RATIO must be a value between 0.0001 and 1.0", e.getMessage());
    }

    @Test
    public void testStatsSampleRatioFailsOnZero() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.setStatsSampleRatio(0));
        assertEquals("Wrong value '0.0'. STATS_SAMPLE_RATIO must be a value between 0.0001 and 1.0", e.getMessage());
    }

    @Test
    public void testStatsSampleRatioFailsOnLessThanOneTenThousand() {
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.setStatsSampleRatio(0.00005f));
        assertEquals("Wrong value '5.0E-5'. STATS_SAMPLE_RATIO must be a value between 0.0001 and 1.0", e.getMessage());
    }

    @Test
    public void testValidateFailsWhenStatsSampleRatioIsNotSet() {
        context.setAccessor("DummyAccessor");
        context.setResolver("DummyResolver");
        context.setStatsMaxFragments(5);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.validate());
        assertEquals("Missing parameter: STATS_SAMPLE_RATIO and STATS_MAX_FRAGMENTS must be set together", e.getMessage());
    }

    @Test
    public void testValidateFailsWhenStatsMaxFragmentsIsNotSet() {
        context.setAccessor("DummyAccessor");
        context.setResolver("DummyResolver");
        context.setStatsSampleRatio(0.1f);
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.validate());
        assertEquals("Missing parameter: STATS_SAMPLE_RATIO and STATS_MAX_FRAGMENTS must be set together", e.getMessage());
    }

    @Test
    public void testValidateFailsWhenAccessorIsNotSet() {
        context.setResolver("dummy");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.validate());
        assertEquals("Property ACCESSOR has no value in the current request", e.getMessage());
    }

    @Test
    public void testValidateFailsWhenResolverIsNotSet() {
        context.setAccessor("dummy");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.validate());
        assertEquals("Property RESOLVER has no value in the current request", e.getMessage());
    }

    @Test
    public void testSuccessfulValidationWhenStatsAreUnset() {
        context.setAccessor("DummyAccessor");
        context.setResolver("DummyResolver");
        context.validate();
    }

    @Test
    public void testSuccessfulValidationWhenStatsAreSet() {
        context.setAccessor("DummyAccessor");
        context.setResolver("DummyResolver");
        context.setStatsMaxFragments(4);
        context.setStatsSampleRatio(0.5f);
        context.validate();
    }

    @Test
    public void testServerNameIsLowerCased() {
        context.setServerName("DUMMY");
        assertEquals("dummy", context.getServerName());
    }

    @Test
    public void testReturnDefaultOptionValue() {
        assertEquals("bar", context.getOption("foo", "bar"));
    }

    @Test
    public void testReturnDefaultIntOptionValue() {
        assertEquals(77, context.getOption("foo", 77));
    }

    @Test
    public void testReturnDefaultNaturalIntOptionValue() {
        assertEquals(77, context.getOption("foo", 77, true));
    }

    @Test
    public void testFailsOnInvalidIntegerOptionWhenRequestedInteger() {
        context.addOption("foo", "junk");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.getOption("foo", 77));
        assertEquals("Property foo has incorrect value junk : must be an integer", e.getMessage());
    }

    @Test
    public void testFailsOnInvalidIntegerOptionWhenRequestedNaturalInteger() {
        context.addOption("foo", "junk");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.getOption("foo", 77, true));
        assertEquals("Property foo has incorrect value junk : must be a non-negative integer", e.getMessage());
    }

    @Test
    public void testFailsOnInvalidNaturalIntegerOptionWhenRequestedNaturalInteger() {
        context.addOption("foo", "-5");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.getOption("foo", 77, true));
        assertEquals("Property foo has incorrect value -5 : must be a non-negative integer", e.getMessage());
    }

    @Test
    public void testReturnDefaultBooleanOptionValue() {
        assertTrue(context.getOption("foo", true));
        assertFalse(context.getOption("foo", false));
    }

    @Test
    public void testReturnBooleanOptionValue() {
        context.addOption("lowercase_false", "false");
        context.addOption("uppercase_false", "FALSE");
        context.addOption("mixed_case_false", "FaLsE");
        context.addOption("lowercase_true", "true");
        context.addOption("uppercase_true", "TRUE");
        context.addOption("mixed_case_true", "tRuE");

        assertFalse(context.getOption("lowercase_false", true));
        assertFalse(context.getOption("uppercase_false", true));
        assertFalse(context.getOption("mixed_case_false", true));
        assertTrue(context.getOption("lowercase_true", false));
        assertTrue(context.getOption("uppercase_true", false));
        assertTrue(context.getOption("mixed_case_true", false));
    }

    @Test
    public void testFailsOnInvalidBooleanOptionWhenRequestedBoolean() {
        context.addOption("foo", "junk");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> context.getOption("foo", false));
        assertEquals("Property foo has incorrect value junk : must be either true or false", e.getMessage());
    }

    @Test
    public void testReturnsUnmodifiableOptionsMap() {
        Map<String, String> unmodifiableMap = context.getOptions();
        assertThrows(UnsupportedOperationException.class,
                () -> unmodifiableMap.put("foo", "bar"));
    }

    @Test
    public void testConfigOptionIsSetWhenProvided() {
        context.setConfig("foobar");
        assertEquals("foobar", context.getConfig());
    }

    @Test
    public void testSucceedsWhenConfigOptionIsARelativeDirectoryName() {
        context.setConfig("../../relative");
        assertEquals("../../relative", context.getConfig());
    }

    @Test
    public void testSucceedsWhenConfigOptionIsAnAbsoluteDirectoryName() {
        context.setConfig("/etc/hadoop/conf");
        assertEquals("/etc/hadoop/conf", context.getConfig());
    }

    @Test
    public void testSucceedsWhenConfigOptionIsTwoDirectories() {
        context.setConfig("foo/bar");
        assertEquals("foo/bar", context.getConfig());
    }
}
