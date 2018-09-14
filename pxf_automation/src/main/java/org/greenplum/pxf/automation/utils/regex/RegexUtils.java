package org.greenplum.pxf.automation.utils.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/***
 * Utility for working with regular expressions
 */
public abstract class RegexUtils {

    /**
     * Matches pattern on data
     *
     * @param regex required regex
     * @param data data to search in
     * @return true if pattern is found in data
     */
    public static boolean match(String regex, String data) {

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(data);

        return matcher.find();
    }

    /**
     * Returns count of found matched according to given regex
     *
     * @param regex required regex
     * @param data data to search in
     * @return number of matches of regex in data
     */
    public static int countMatches(String regex, String data) {

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(data);

        int count = 0;
        while (matcher.find()) {
            count++;
        }

        return count;
    }

    /**
     * Returns a matched regex expression
     *
     * @param regex required regex with an expression to match
     * @param data data to search in
     * @return substring of matched expression or null
     */
    public static String find(String regex, String data) {

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(data);
        if (matcher.find()) {
            if (matcher.groupCount() >= 1) {
                return matcher.group(1);
            } else {
                return matcher.group();
            }
        }
        return null;
    }
}
