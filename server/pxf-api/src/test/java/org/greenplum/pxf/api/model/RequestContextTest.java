package org.greenplum.pxf.api.model;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class RequestContextTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private RequestContext context;

    @Before
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
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Invalid server name 'foo,bar'");
        context.setServerName("foo,bar");
    }

    @Test
    public void testStatsMaxFragmentsFailsOnZero() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Wrong value '0'. STATS-MAX-FRAGMENTS must be a positive integer");
        context.setStatsMaxFragments(0);
    }

    @Test
    public void testStatsMaxFragmentsFailsOnNegative() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Wrong value '-1'. STATS-MAX-FRAGMENTS must be a positive integer");
        context.setStatsMaxFragments(-1);
    }

    @Test
    public void testStatsSampleRatioFailsOnOver1() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Wrong value '1.1'. STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
        context.setStatsSampleRatio(1.1f);
    }

    @Test
    public void testStatsSampleRatioFailsOnZero() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Wrong value '0.0'. STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
        context.setStatsSampleRatio(0);
    }

    @Test
    public void testStatsSampleRatioFailsOnLessThanOneTenThousand() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Wrong value '5.0E-5'. STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
        context.setStatsSampleRatio(0.00005f);
    }

    @Test
    public void testValidateFailsWhenStatsSampleRatioIsNotSet() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together");
        context.setAccessor("DummyAccessor");
        context.setResolver("DummyResolver");
        context.setStatsMaxFragments(5);
        context.validate();
    }

    @Test
    public void testValidateFailsWhenStatsMaxFragmentsIsNotSet() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together");
        context.setAccessor("DummyAccessor");
        context.setResolver("DummyResolver");
        context.setStatsSampleRatio(0.1f);
        context.validate();
    }

    @Test
    public void testValidateFailsWhenAccessorIsNotSet() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property ACCESSOR has no value in the current request");
        context.setResolver("dummy");
        context.validate();
    }

    @Test
    public void testValidateFailsWhenResolverIsNotSet() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property RESOLVER has no value in the current request");
        context.setAccessor("dummy");
        context.validate();
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
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property foo has incorrect value junk : must be an integer");

        context.addOption("foo", "junk");
        context.getOption("foo", 77);
    }

    @Test
    public void testFailsOnInvalidIntegerOptionWhenRequestedNaturalInteger() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property foo has incorrect value junk : must be a non-negative integer");

        context.addOption("foo", "junk");
        context.getOption("foo", 77, true);
    }

    @Test
    public void testFailsOnInvalidNaturalIntegerOptionWhenRequestedNaturalInteger() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property foo has incorrect value -5 : must be a non-negative integer");

        context.addOption("foo", "-5");
        context.getOption("foo", 77, true);
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
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Property foo has incorrect value junk : must be either true or false");

        context.addOption("foo", "junk");
        context.getOption("foo", false);
    }

    @Test
    public void testReturnsUnmodifiableOptionsMap() {
        thrown.expect(UnsupportedOperationException.class);

        Map<String, String> unmodifiableMap = context.getOptions();
        unmodifiableMap.put("foo", "bar");
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
