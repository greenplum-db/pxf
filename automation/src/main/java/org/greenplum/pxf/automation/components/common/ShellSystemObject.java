package org.greenplum.pxf.automation.components.common;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.greenplum.pxf.automation.utils.curl.CurlUtils;
import jsystem.framework.report.Reporter;

import org.apache.commons.lang.StringUtils;

import org.greenplum.pxf.automation.components.common.cli.PivotalCliConnectionImpl;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import systemobject.terminal.Prompt;

import com.aqua.sysobj.conn.CliCommand;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * General Shell system objects, each System Object can extend it or use it.
 */
public class ShellSystemObject extends BaseSystemObject {
    private PivotalCliConnectionImpl connection;
    private String host = "localHost";
    private String masterHost = "localHost";
    private String hostName = "";
    private String userName;
    private String password;
    private String privateKey;
    private String lastCmdResult = "";
    private int lastCommandExitCode = 0;
    // ignore passing local env vars to ssh connection
    private boolean ignoreEnvVars = false;

    public static final long _1_SECOND = 1000;
    public static final long _2_SECONDS = (_1_SECOND * 2);
    public static final long _5_SECONDS = (_1_SECOND * 5);
    public static final long _10_SECONDS = (_5_SECONDS * 2);
    public static final long _30_SECONDS = (_1_SECOND * 30);
    public static final long _1_MINUTE = (_30_SECONDS * 2);
    public static final long _2_MINUTES = (_1_MINUTE * 2);
    public static final long _5_MINUTES = (_1_MINUTE * 5);
    public static final long _10_MINUTES = _5_MINUTES * 2;
    public static final long _30_MINUTES = _10_MINUTES * 3;

    public static final int MIN_COMMAND_TIMEOUT = 100;

    public static final int EXIT_CODE_SUCCESS = 0;
    public static final int EXIT_CODE_NOT_EXISTS = -1;

    // the max timeout for command execution
    private long commandTimeout = _10_SECONDS;

    private String[] requiredEnvParams = new String[] {
            "JAVA_HOME",
            "GPHOME",
            "GPHD_ROOT",
            "GPDATA",
            "MASTER_DATA_DIRECTORY",
            "PGPORT",
            "PGHOST",
            "PGDATABASE",
            "TERM"
    };

    public ShellSystemObject() {

    }

    /**
     * C'tor with option if to use silent mode of jsystem report
     *
     * @param silentReport if true silent else will try to write to jsystem
     *            report for every report
     */
    public ShellSystemObject(boolean silentReport) {
        super(silentReport);
    }

    @Override
    public void init() throws Exception {
        super.init();

        ReportUtils.startLevel(report, getClass(), "init");

        // if no user injected by the user, use "user.name" of the machine
        if (getUserName() == null) {
            setUserName(System.getProperty("user.name"));
        }

        // if no password injected us empty string
        if (getPassword() == null) {
            setPassword("");
        }

        ReportUtils.report(report, getClass(), "Establish connection to: "
                + host + " (User Name: " + getUserName() + " Password: "
                + getPassword() + ")");
        connection = new PivotalCliConnectionImpl(host, getUserName(),
                getPassword());

        /**
         * mention SSH_RSA connection type
         */
        connection.setProtocol("ssh-rsa");

        /**
         * Direct to Private Key instead of Password based connection.
         */
        String privateKeyFileName = privateKey;
        if (privateKeyFileName == null) {
            privateKeyFileName = System.getProperty("user.home") + "/.ssh/id_rsa";
        }
        File privateKeyFile = new File(privateKeyFileName);
        connection.setPrivateKey(privateKeyFile);
        ReportUtils.report(
                report,
                getClass(),
                "Attempt to create SSH-RSA connection (User: "
                        + getUserName() + " Public-Key File:"
                        + privateKeyFile.getAbsolutePath() + ")");

        /**
         * PivotalCliConnectionImpl is setting the prompt to be '#'.Add the
         * required prompt to the connection.
         */
        Prompt p = new Prompt();
        p.setCommandEnd(true);
        p.setPrompt("#");

        connection.addPrompts(new Prompt[] { p });
        connection.init();

        if (!ignoreEnvVars) {
            runCommand(getExportForRequiredEnvVars());
        }

        ReportUtils.stopLevel(report);
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.disconnect();
            connection.close();
        }
        super.close();
    }

    /**
     * execute command-line command, verify exit code to be EXIT_CODE_SUCCESS
     * and store the result in lastCmdResult.
     *
     * @param command command to execute
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void runCommand(String command) throws IOException,
            ShellCommandErrorException {
        runCommand(command, EXIT_CODE_SUCCESS);
    }

    /**
     * execute command-line command, check expectedExitCode and store the result
     * in lastCmdResult.
     *
     * @param command command to execute
     * @param expectedExitCode to check after command execution
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void runCommand(String command, int expectedExitCode)
            throws IOException, ShellCommandErrorException {
        String commandAddtionalMessage = "("
                + getHost()
                + ((StringUtils.isEmpty(getHostName())) ? (")") : ("/"
                        + getHostName() + ")"));
        ReportUtils.startLevel(report, getClass(), commandAddtionalMessage,
                command);

        CliCommand cmd = new CliCommand();
        cmd.setTimeout(commandTimeout);
        cmd.setCommand(command);

        connection.command(cmd);
        lastCmdResult = cmd.getResult();

        ReportUtils.report(report, getClass(), lastCmdResult);

        // if expectedExitCode=EXIT_CODE_NOT_EXISTS it means no need to check
        // exit code
        if (expectedExitCode != EXIT_CODE_NOT_EXISTS) {
            // get the last ran command exit code
            lastCommandExitCode = getLastExitCode();
            // throw exception if last command failed
            if (lastCommandExitCode != expectedExitCode) {
                throw new ShellCommandErrorException("Command: \"" + command
                        + "\" returned exit code " + lastCommandExitCode
                        + " expected: " + expectedExitCode);
            }
        }
        ReportUtils.stopLevel(report);
    }

    /**
     * get exit code using "echo $?". It highly recommended to call it after
     * performing command otherwise it might not get the exit code and will
     * raise a {@link NumberFormatException}
     */
    private int getLastExitCode() {
        // get exit code
        CliCommand cmd = new CliCommand();
        cmd.setTimeout(commandTimeout);
        cmd.setCommand("echo $?");
        connection.command(cmd);

        // split result to new line
        String[] splitArray = cmd.getResult().split(System.lineSeparator());
        // go over splitResult and look for Numeric result
        for (int i = 0; i < splitArray.length; i++) {
            try {
                int result = Integer.parseInt(splitArray[i].trim());
                return result;
            } catch (Exception e) {
                continue;
            }
        }
        // if not found return EXIT_CODE_NOT_EXISTS
        return EXIT_CODE_NOT_EXISTS;
    }

    /**
     * perform jps command
     *
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    protected void jps() throws IOException, ShellCommandErrorException {
        runCommand("jps");
    }

    /**
     * Runs curl command
     *
     * @param host host
     * @param port port
     * @param path path (excluding host and port)
     * @throws Exception if curl command failed or response didn't have expected
     *             format
     */
    public String curl(String host, String port, String path) throws Exception {
        CurlUtils curl = new CurlUtils(host, port, path);
        runCommand(curl.getCommand());
        return parseCurlResponse();
    }

    /**
     * Runs curl command
     *
     * @param host host
     * @param port port
     * @param path path (excluding host and port)
     * @param params params (map of key value pairs of params for post requests)
     * @return parsed curl response
     * @throws Exception if curl command failed or response didn't have expected
     *             format
     */
    public String curl(String host, String port, String path, String requestType, Map<String, String> headers, List<String> params) throws Exception {
        CurlUtils curl = new CurlUtils(host, port, path, requestType, headers, params);
        runCommand(curl.getCommand());
        return parseCurlResponse();
    }

    /**
     * Parses result and get the actual output from the last command result. The
     * actual output is of the form:
     * {@code <command><new line><output><terminator char>} e.g.
     * {@code
     * curl "http://localhost:5888/pxf/ProtocolVersion"
     * PXF protocol version v14#
     * }
     *
     * @return parsed curl response
     * @throws Exception if response doesn't have new line as expected
     */
    private String parseCurlResponse() throws Exception {
        String response = getLastCmdResult();
        if (StringUtils.isEmpty(response)) {
            return response;
        }
        int newLineIndex = response.indexOf("\r\n");
        // response need to have new line (\r\n) and some data afterwards (the
        // actual response). If that's not the case, we have a problem
        if (newLineIndex == -1 || response.length() <= 2) {
            throw new Exception(
                    "Curl response is not formatted as expected (response: "
                            + response + ")");
        }
        return response.substring(newLineIndex + 2, response.length() - 1);
    }

    /**
     * Close shell connection
     */
    public void disconnect() {
        connection.disconnect();
    }

    /**
     * get shell export command for all required sut or env vars.
     *
     * @return export command for the env vars
     */
    protected String getExportForRequiredEnvVars() {
        String result = "export";
        for (int i = 0; i < requiredEnvParams.length; i++) {
            result += " " + getEnvVarStatement(requiredEnvParams[i]);
        }
        return result;
    }

    /**
     * returns string that includes the required env vatibale key = it value
     *
     * @param envVariable required env variable
     * @return VAR=<value> if exists, else returns empty string.
     */
    private String getEnvVarStatement(String envVariable) {
        // get value of env variable
        String envVarValue = getEnvVar(envVariable);
        // if not empty construct string to return
        if (StringUtils.isNotEmpty(envVarValue)) {
            return (envVariable + "=" + envVarValue);
        }
        // if empty return empty string
        return "";
    }

    /**
     * Read variable from SUT ShellSystemObject element. If value doesn't exist
     * or empty, read it from system env.
     *
     * @param var environment variable name
     * @return environment variable var value
     */
    private String getEnvVar(String var) {
        String result = null;

        try {
            result = sut.getValue("/sut/shellsystemobject/" + var);
        } catch (Exception e) {
            ReportUtils.report(report, getClass(), "Didn't find SUT value for "
                    + var, Reporter.WARNING);
            result = null;
        }

        if ((result == null) || result.isEmpty()) {
            result = System.getenv(var);
        }

        ReportUtils.report(report, getClass(), "Value for var " + var + " is "
                + result);

        return result;
    }

    /**
     * Delete directory recursively
     *
     * @param directoryToDelete
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void deleteDirectory(String directoryToDelete) throws IOException,
            ShellCommandErrorException {
        runCommand("rm -rf " + directoryToDelete);
    }

    /**
     * Returns sshpass command if password is provided.
     *
     * @param password
     * @return sshpass command or empty string
     */
    private String getSshPass(String password) {
        String sshPass = "";
        if (!StringUtils.isEmpty(password)) {
            sshPass = "sshpass -p '" + password + "' ";
        }
        return sshPass;
    }

    /**
     * copy from remote machine to local path
     *
     * @param user host's user
     * @param password hosts's password
     * @param host host name or IP
     * @param fromPath remote path to copy from
     * @param toPath local destination path
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void copyFromRemoteMachine(String user, String password,
                                      String host, String fromPath,
                                      String toPath) throws IOException,
            ShellCommandErrorException {
        runCommand(getSshPass(password) + "scp -o StrictHostKeyChecking=no -r "
                + user + "@" + host + ":" + fromPath + " " + toPath);
    }

    /**
     * copy from local machine path to remote machine
     *
     * @param user host's user
     * @param password hosts's password
     * @param host host name or IP
     * @param fromPath remote path to copy from
     * @param toPath local destination path
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void copyToRemoteMachine(String user, String password, String host,
                                    String fromPath, String toPath)
            throws IOException, ShellCommandErrorException {
        runCommand(getSshPass(password) + "scp -o StrictHostKeyChecking=no -r "
                + fromPath.replace("$", "\\$") + " " + user + "@" + host + ":"
                + toPath);
    }

    /**
     * Copy to List of remote machines with same credentials
     *
     * @param user machines user name
     * @param password machines password
     * @param filePath to copy
     * @param target in remote machines
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void copyToRemoteMachines(String user, String password,
                                     List<String> machines, String filePath,
                                     String target) throws IOException,
            ShellCommandErrorException {
        for (String node : machines) {
            copyToRemoteMachine(user, password, node, filePath, target);
        }
    }

    /**
     * Run command on remote node
     *
     * @param user machine's username
     * @param password machine's password
     * @param host machine's host
     * @param command command to execute
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void runRemoteCommand(String user, String password, String host,
                                 String command) throws IOException,
            ShellCommandErrorException {
        runCommand(getSshPass(password) + "ssh -o StrictHostKeyChecking=no "
                + user + "@" + host + " -t \"" + command + "\"");
    }

    /**
     * Delete file from remote machine using machien's credentials.
     *
     * @param user machine's username
     * @param password machine's password
     * @param host machine's host
     * @param filePath of file to delete
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public void deleteFileFromRemoteMachine(String user, String password,
                                            String host, String filePath,
                                            boolean sudo) throws IOException,
            ShellCommandErrorException {
        String deleteCmd = "rm -rf " + filePath;
        if (sudo) {
            deleteCmd = "sudo -s " + deleteCmd;
        }
        runRemoteCommand(user, password, host, deleteCmd);
    }

    /**
     * Check if fileName exists on given path
     *
     * @param path to check if file exists
     * @param fileName to find
     * @return true if file exists in given path
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public boolean checkFileExists(String path, String fileName)
            throws IOException, ShellCommandErrorException {
        // empty command to clean result buffer
        runCommand(" ");
        // may get error on this command
        runCommand("ls " + path + "/" + fileName, EXIT_CODE_NOT_EXISTS);
        // parse the result
        String result = getLastCmdResult().split("\r\n")[1];
        // if equals return true
        return (result.trim().equals(path + "/" + fileName));
    }

    /**
     * @return which user is logged in at this moment
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    public String getLoggedUser() throws IOException,
            ShellCommandErrorException {
        runCommand("whoami");
        // parse user from returned result
        return getLastCmdResult().split("\r\n")[1].trim();
    }

    public String getLastCmdResult() {
        return lastCmdResult;
    }

    public void setLastCmdResult(String lastCmdResult) {
        this.lastCmdResult = lastCmdResult;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setMasterHost(String masterHost) {
        this.masterHost = masterHost;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public int getLastCommandExitCode() {
        return lastCommandExitCode;
    }

    public long getCommandTimeout() {
        return commandTimeout;
    }

    public void setCommandTimeout(long commandTimeout) {
        this.commandTimeout = commandTimeout;
    }

    public boolean isIgnoreEnvVars() {
        return ignoreEnvVars;
    }

    public void setIgnoreEnvVars(boolean ignoreEnvVars) {
        this.ignoreEnvVars = ignoreEnvVars;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
}
