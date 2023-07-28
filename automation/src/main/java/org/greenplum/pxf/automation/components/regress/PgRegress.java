package org.greenplum.pxf.automation.components.regress;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.StringJoiner;

public class PgRegress extends ShellSystemObject {
    private String regressTestFolder;
    private String pgRegress;
    // TODO: GP6 has "--psqldir"
    // TODO: GP7 has "--bindir"
    private String psqlDir;
    private String dbName;
    private String initFile;
    @Override
    public void init() throws Exception {
        ReportUtils.startLevel(report, getClass(), "init");
        super.init();
        runCommand("source $GPHOME/greenplum_path.sh");
        runCommand("cd " + new File(regressTestFolder).getAbsolutePath());
        ReportUtils.stopLevel(report);
    }

    /**
     * <p>Runs pg_regress test case</p>
     *
     * <p>The named test is checked to see if it is a tinc-style test name and mapped to the corresponding regress test
     * director; if it isn't a tinc-style test, it is used as-is.</p>
     *
     * @param tincTestPath
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void runPgRegress(String tincTestPath) throws IOException, ShellCommandErrorException {
        ReportUtils.startLevel(report, getClass(), "Run test: " + tincTestPath);

        String testPath = mapToRegressDir(tincTestPath);
        ReportUtils.report(report, getClass(), "test path: " + testPath);

        setCommandTimeout(_10_MINUTES);
        StringJoiner commandToRun = new StringJoiner(" ");

        commandToRun.add(pgRegress);
        commandToRun.add("--use-existing");
        commandToRun.add("--psqldir=${GPHOME}/bin");
        commandToRun.add("--inputdir=" + testPath);
        commandToRun.add("--outputdir=" + testPath);
        commandToRun.add("--schedule=" + testPath + "/schedule");
        commandToRun.add("--init-file=" + initFile);
        commandToRun.add("--dbname=" + dbName);

        runCommand(commandToRun.toString());

        ReportUtils.stopLevel(report);
    }

    public boolean isTincModule(String testPath) {
        return StringUtils.startsWith(testPath, "pxf.") && StringUtils.endsWith(testPath, "runTest");
    }

    public String mapToRegressDir(String tincTestPath) {
        return StringUtils.replace(
                StringUtils.removeEnd(tincTestPath, ".runTest"),
                ".",
                "/"
        );
    }

    public String getRegressTestFolder() {
        return regressTestFolder;
    }

    public void setRegressTestFolder(String regressTestFolder) {
        this.regressTestFolder = regressTestFolder;
    }

    public String getPgRegress() {
        return pgRegress;
    }

    public void setPgRegress(String pgRegress) {
        this.pgRegress = pgRegress;
    }

    public String getPsqlDir() {
        return psqlDir;
    }

    public void setPsqlDir(String psqlDir) {
        this.psqlDir = psqlDir;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getInitFile() {
        return initFile;
    }

    public void setInitFile(String initFile) {
        this.initFile = initFile;
    }
}
