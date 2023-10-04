package org.greenplum.pxf.automation.components.regress;

import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

import java.io.File;
import java.io.IOException;
import java.util.StringJoiner;

/**
 * Utility class for running pxf_regress
 */
public class Regress extends ShellSystemObject {
    private String regressTestFolder;
    private String regressRunner;
    private String dbName;

    @Override
    public void init() throws Exception {
        ReportUtils.startLevel(report, getClass(), "init");
        regressRunner = new File("pxf_regress/pxf_regress").getAbsolutePath();
        super.init();
        runCommand("source $GPHOME/greenplum_path.sh");
        runCommand("cd " + new File(regressTestFolder).getAbsolutePath());
        ReportUtils.stopLevel(report);
    }

    /**
     * Run the SQL test queries in the named tinc Python module with pxf_regress
     * @param tincTestModule name of tinc Python module that contains SQL test queries to run
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void runTest(final String tincTestModule) throws IOException, ShellCommandErrorException {
        ReportUtils.startLevel(report, getClass(), "Run test: " + tincTestModule);
        String testPath = mapToRegressDir(tincTestModule);
        ReportUtils.report(report, getClass(), "test path: " + testPath);

        setCommandTimeout(_10_MINUTES);
        StringJoiner commandToRun = new StringJoiner(" ");

        commandToRun.add("PGDATABASE=" + dbName);
        commandToRun.add(regressRunner);
        commandToRun.add(testPath);

        ReportUtils.report(report, getClass(), "running command \"" + commandToRun + "\"");

        runCommand(commandToRun.toString());
        ReportUtils.stopLevel(report);
    }

    /**
     * Convert a tinc Python module name into a directory name
     * <p>
     * Tinc test cases are specified with a Python module name (e.g. "pxf.smoke.small_data.runTest") which are used by
     * tinc to load and run SQL test queries. Since pxf_regress is not Python, it does not understand Python modules;
     * instead we convert the module name to its matching directory name. This is done by replacing all "." with "/" and
     * removing the trailing ".runTest". For example "pxf.smoke.small_data.runTest" is converted to
     * "pxf/smoke/small_data".
     * @param tincTestPath tinc Python module name
     * @return directory containing the named Python module
     */
    public String mapToRegressDir(String tincTestPath) {
        return StringUtils.replace(
                StringUtils.removeEnd(tincTestPath, ".runTest"),
                ".",
                "/"
        );
    }

    public void setRegressTestFolder(String regressTestFolder) {
        this.regressTestFolder = regressTestFolder;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
