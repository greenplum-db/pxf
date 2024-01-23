package org.greenplum.pxf.automation.features.hcfs;

import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.with;

/**
 * Functional Globbing Tests. Tests are based on Hadoop Glob Tests
 * https://github.com/apache/hadoop/blob/rel/release-3.2.1/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestGlobPaths.java
 */
@WorksWithFDW
public class HcfsGlobbingTest extends BaseFeature {

    public static final String[] FIELDS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMatchAnySingleCharacter() throws Exception {
        prepareTestScenario("match_any_single_character", "abc", "a2c", "a.c", "abcd", "a?c");
        runTestScenario("match_any_single_character");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMatchZeroOrMoreCharacters() throws Exception {
        prepareTestScenario("match_zero_or_more_characters_1", "a", "abc", "abc.p", "bacd", "a*");
        prepareTestScenario("match_zero_or_more_characters_2", "a.", "a.txt", "a.old.java", ".java", "a.*");
        prepareTestScenario("match_zero_or_more_characters_3", "a.txt.x", "ax", "ab37x", "bacd", "a*x");
        prepareTestScenario("match_zero_or_more_characters_4", "dir1/file1", "dir2/file2", "dir3/file1", null, "*/file1");
        prepareTestScenario("match_zero_or_more_characters_5", "dir1/file1", "file1", null, null, "*/file1");

        runTestScenario("match_zero_or_more_characters");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMatchASingleCharacterFromCharSet() throws Exception {
        prepareTestScenario("match_single_character_from_set", "a.c", "a.cpp", "a.hlp", "a.hxy", "a.[ch]??");
        runTestScenario("match_single_character_from_set");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMatchASingleCharacterFromCharRange() throws Exception {
        prepareTestScenario("match_single_character_from_range", "a.d", "a.e", "a.f", "a.h", "a.[d-f]");
        runTestScenario("match_single_character_from_range");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMatchASingleCharacterNotFromCharSetOrRange() throws Exception {
        prepareTestScenario("match_single_character_set_exclusion", "a.d", "a.e", "a.0", "a.h", "a.[^a-cg-z0-9]");
        runTestScenario("match_single_character_set_exclusion");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testEscapeSpecialCharacters() throws Exception {
        prepareTestScenario("escape_special_characters", "ab[c.d", null, null, null, "ab\\\\[c.d");
        runTestScenario("escape_special_characters");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testMatchAStringFromStringSet() throws Exception {
        prepareTestScenario("match_string_from_string_set_1", "a.abcxx", "a.abxy", "a.hlp", "a.jhyy", "a.{abc,jh}??");
        // nested curlies
        prepareTestScenario("match_string_from_string_set_2", "a.abcxx", "a.abdxy", "a.hlp", "a.jhyy", "a.{ab{c,d},jh}??");
        // cross-component curlies
        prepareTestScenario("match_string_from_string_set_3", "a/b", "a/d", "c/b", "c/d", "{a/b,c/d}");
        // test standalone }
        prepareTestScenario("match_string_from_string_set_4", "}bc", "}c", null, null, "}{a,b}c");
        // test {b}
        prepareTestScenario("match_string_from_string_set_4", "match_string_from_string_set_5", null, null, null, null, "}{b}c");
        // test {}
        prepareTestScenario("match_string_from_string_set_4", "match_string_from_string_set_6", null, null, null, null, "}{}bc");
        // test {,}
        prepareTestScenario("match_string_from_string_set_4", "match_string_from_string_set_7", null, null, null, null, "}{,}bc");
        // test {b,}
        prepareTestScenario("match_string_from_string_set_4", "match_string_from_string_set_8", null, null, null, null, "}{b,}c");
        // test {,b}
        prepareTestScenario("match_string_from_string_set_4", "match_string_from_string_set_9", null, null, null, null, "}{,b}c");
        // test a combination of {} and ?
        prepareTestScenario("match_string_from_string_set_4", "match_string_from_string_set_10", null, null, null, null, "}{ac,?}");
        // test ill-formed curly
        prepareTestScenario("match_string_from_string_set_4", "match_string_from_string_set_11", null, null, null, null, "}{bc");

        // test escape curly
        prepareTestScenario("match_string_from_string_set_12", "}{bc", "}bc", null, null, "}\\\\{bc");
        runTestScenario("match_string_from_string_set");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testJavaRegexSpecialChars() throws Exception {
        // The earlier path (.|+)bc and the glob value (.|+)* is failing for GP7 with this error:
        // select * from hcfs_glob_java_regex_special_chars;
        // ERROR:  invalid URI '+)*?PROFILE=hdfs:text' : undefined structure
        // due to the fact that the current implementation of gp_exttable_fdw that supports external tables in GP7
        // treats the pipe "|" symbol in the value of LOCATION clause as a separator for the list of URLs
        // So we have to change this test to remove the pipe symbol from our file names and glob values
        prepareTestScenario("java_regex_special_chars", "(.+)bc", "abc", null, null, "(.+)*");
        runTestScenario("java_regex_special_chars");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void testCombination() throws Exception {
        prepareTestScenario("combination", "user/aa/a.c", "user/bb/a.cpp", "user1/cc/b.hlp", "user/dd/a.hxy", "use?/*/a.[ch]{lp,xy}");
        runTestScenario("combination");
    }

    private void runTestScenario(String testGroup) throws Exception {
        runSqlTest("features/hcfs/globbing/" + testGroup);
    }

    private void prepareTestScenario(String testName, String data1, String data2, String data3, String data4, String glob) throws Exception {
        prepareTestScenario(testName, testName, data1, data2, data3, data4, glob);
    }

    private void prepareTestScenario(String path, String testName, String data1, String data2, String data3, String data4, String glob) throws Exception {
        prepareTableData(path, data1, "1a");
        prepareTableData(path, data2, "2b");
        prepareTableData(path, data3, "3c");
        prepareTableData(path, data4, "4d");

        // wait until all the files exist, before continuing the test
        List<String> datafiles = Arrays.asList(data1, data2, data3, data4);
        datafiles.parallelStream().forEach(datafile -> {
            if (datafile != null) {
                with().pollInterval(20, MILLISECONDS)
                        .and().with().pollDelay(20, MILLISECONDS)
                        .await().atMost(240, SECONDS)
                        .until(() -> hdfs.doesFileExist("/" + hdfs.getWorkingDirectory() + "/" + path + "/" + datafile));
            }
        });

        ProtocolEnum protocol = ProtocolUtils.getProtocol();

        // Create GPDB external table directed to the HDFS file

        String updatedPath = protocol.getExternalTablePath(hdfs.getBasePath(), hdfs.getWorkingDirectory()) + "/" + path + "/" + glob;
        if (FDWUtils.useFDW) {
            updatedPath = "E'"+ updatedPath +"'";
        }

        exTable = TableFactory.getPxfReadableTextTable(
                "hcfs_glob_" + testName,
                FIELDS,
                updatedPath,
                ",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
    }

    private void prepareTableData(String path, String name, String prefix) throws Exception {
        if (name == null) return;

        Table dataTable = getSmallData(prefix + " " + name, 20);
        hdfs.writeTableToFile(hdfs.getWorkingDirectory() + "/" + path + "/" + name, dataTable, ",");
    }
}
