package org.greenplum.pxf.plugins.jdbc.utils;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.util.Set;

/**
 * Utilities class to handle Hive JDBC specific logic.
 */
public class HiveJdbcUtils {

    private static final String HIVE_URL_IMPERSONATION_PROPERTY = ";hive.server2.proxy.user";

    private static final Set<Character> TERMINATORS_THREE_CHARS = Sets.newHashSet(';', '?', '#');
    private static final Set<Character> TERMINATORS_TWO_CHARS = Sets.newHashSet('?', '#');

    /**
     * Ensures "hive.server2.proxy.user" property is set to the given user in the URL.
     *
     * @param url  url to update
     * @param user user name
     * @return updated url
     */
    public static String updateImpersonationPropertyInHiveJdbcUrl(String url, String user) {
        String suffix, prefix = null;
        int terminatorIndex = findTerminatorIndex(url, 0, TERMINATORS_TWO_CHARS);

        // impersonation property might already be in the URL
        int impersonationPropertyIndex = url.indexOf(HIVE_URL_IMPERSONATION_PROPERTY);
        if (impersonationPropertyIndex > -1 && (terminatorIndex == -1 || impersonationPropertyIndex < terminatorIndex)) {
            // unlikely to happen, unless users are trying to hack the system and provide this property in the DDL
            prefix = url.substring(0, impersonationPropertyIndex);

            // find where the token terminates (by ; ? # or EOL, whatever comes first)
            terminatorIndex = findTerminatorIndex(url, impersonationPropertyIndex + HIVE_URL_IMPERSONATION_PROPERTY.length(), TERMINATORS_THREE_CHARS);
        }

        suffix = terminatorIndex < 0 ? "" : url.substring(terminatorIndex);

        if (prefix == null) {
            // when the HIVE_URL_IMPERSONATION_PROPERTY is not present
            prefix = terminatorIndex < 0 ? url : url.substring(0, terminatorIndex);
        }

        return String.format("%s%s=%s%s", StringUtils.removeEnd(prefix, ";"), HIVE_URL_IMPERSONATION_PROPERTY, user, suffix);
    }

    private static int findTerminatorIndex(String s, int start, Set<Character> terminators) {
        int ndx = start - 1;
        while (++ndx < s.length()) {
            if (terminators.contains(s.charAt(ndx))) {
                return ndx;
            }
        }
        return -1;
    }
}
