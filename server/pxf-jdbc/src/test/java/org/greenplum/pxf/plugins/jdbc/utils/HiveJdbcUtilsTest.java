package org.greenplum.pxf.plugins.jdbc.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HiveJdbcUtilsTest {

    @Test
    public void testURLWithoutProperties() throws Exception {
        String url = "jdbc:hive2://server:10000/default";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithoutImpersonationProperty() throws Exception {
        String url = "jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf";
        assertEquals("jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithoutImpersonationPropertyAndWithQuestionMark() throws Exception {
        String url = "jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf?hive.property=value";
        assertEquals("jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf;hive.server2.proxy.user=foo?hive.property=value",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithoutImpersonationPropertyAndWithHash() throws Exception {
        String url = "jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf#hive.property=value";
        assertEquals("jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf;hive.server2.proxy.user=foo#hive.property=value",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithoutImpersonationPropertyWithQuestionMarkAndWithHash() throws Exception {
        String url = "jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf?hive.question=value#hive.hash=value";
        assertEquals("jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf;hive.server2.proxy.user=foo?hive.question=value#hive.hash=value",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyStart() throws Exception {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar;otherProperty=otherValue;saslQop=auth-conf";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo;otherProperty=otherValue;saslQop=auth-conf",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyMiddle() throws Exception {
        String url = "jdbc:hive2://server:10000/default;otherProperty=otherValue;hive.server2.proxy.user=bar;saslQop=auth-conf";
        assertEquals("jdbc:hive2://server:10000/default;otherProperty=otherValue;hive.server2.proxy.user=foo;saslQop=auth-conf",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyEnd() throws Exception {
        String url = "jdbc:hive2://server:10000/default;otherProperty=otherValue;saslQop=auth-conf;hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;otherProperty=otherValue;saslQop=auth-conf;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyOnly() throws Exception {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyAndQuestionMark() throws Exception {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar?hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo?hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyAndQuestionMark2() throws Exception {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar;other-property=value?hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo;other-property=value?hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyAndHash() throws Exception {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar#hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo#hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyAndQuestionMarkAndHash() throws Exception {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar?hive.server2.proxy.user=bar#hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo?hive.server2.proxy.user=bar#hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithSimilarPropertyName() throws Exception {
        String url = "jdbc:hive2://server:10000/default;non-hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;non-hive.server2.proxy.user=bar;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testImpersonationPropertyAfterQuestionMark() throws Exception {
        String url = "jdbc:hive2://server:10000/default?questionMarkProperty=questionMarkValue;hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo?questionMarkProperty=questionMarkValue;hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testImpersonationPropertyAfterHash() throws Exception {
        String url = "jdbc:hive2://server:10000/default#questionMarkProperty=questionMarkValue;hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo#questionMarkProperty=questionMarkValue;hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }
}
