package org.greenplum.pxf.automation;

import jsystem.framework.report.ListenerstManager;
import jsystem.framework.sut.SutFactory;
import jsystem.framework.system.SystemManagerImpl;
import jsystem.framework.system.SystemObject;
import jsystem.utils.FileUtils;
import listeners.CustomAutomationLogger;
import listeners.FDWSkipTestAnalyzer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.gpdb.Gpdb;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.components.tinc.Tinc;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import reporters.CustomAutomationReport;

import java.io.File;
import java.lang.reflect.Method;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL;

/**
 * PXF Automation tests Base class, using {@link CustomAutomationLogger} testNG listener for custom
 * logging
 */
@Listeners({CustomAutomationLogger.class, CustomAutomationReport.class, FDWSkipTestAnalyzer.class})
public abstract class BaseTestParent {
    // Objects used in the tests
    protected PhdCluster cluster;
    protected Tinc tinc;
    protected Gpdb gpdb;
    protected Gpdb nonUtf8Gpdb;
    protected Hdfs hdfs;
    // When running against multiple hadoop environments, we need to test against
    // a non-kerberized (secured) hadoop.
    protected Hdfs hdfsNonSecure;
    protected ReadableExternalTable exTable;
    // data resources folder
    protected String localDataResourcesFolder = "src/test/resources/data";
    // temporary data folder
    protected String dataTempFolder = "dataTempFolder";
    protected String pxfHost;
    protected String pxfPort;
    protected String testUserkeyTabPathFormat = "/etc/security/keytabs/%s.headless.keytab";

    protected SystemManagerImpl systemManager;

    // c'tor
    public BaseTestParent() {
        // alert not allowed annotations in test class children
        alertNotAllowedAnnotations();
        systemManager = SystemManagerImpl.getInstance();
    }

    @BeforeClass(alwaysRun = true)
    public final void doInit() throws Exception {
        // redirect "doInit" logs to log file
        CustomAutomationLogger.redirectStdoutStreamToFile(getClass().getSimpleName(), "doInit");
        // JSystem reporter is not alive because of running from TestNg,
        // silent it prevents Exceptions in the output
        ListenerstManager.getInstance().setSilent(true);

        try {

            cluster = (PhdCluster) systemManager.getSystemObject("cluster");

            // Initialize HDFS system object
            hdfs = (Hdfs) systemManager.getSystemObject(ProtocolUtils.getProtocol().value());

            String testPrincipal = cluster.getTestKerberosPrincipal();
            trySecureLogin(hdfs, testPrincipal);

            // Initialize non-secure HDFS system object (optional system object)
            hdfsNonSecure = (Hdfs) systemManager.
                    getSystemObject("/sut", "hdfsNonSecure", -1, (SystemObject) null, false, (String) null, SutFactory.getInstance().getSutInstance());

            // Create local Data folder
            File localDataTempFolder = new File(dataTempFolder);
            localDataTempFolder.mkdirs();
            // Initialize Tinc System Object
            tinc = (Tinc) systemManager.getSystemObject("tinc");
            // Initialize GPDB System Object
            gpdb = (Gpdb) systemManager.getSystemObject("gpdb");
            // Initialize GPDB2 System Object -- database with non-utf8 encoding
            nonUtf8Gpdb = (Gpdb) systemManager.getSystemObject("gpdb2");

            // Check if userName data base exists if not create it (TINC requirement)
            String userName = System.getProperty("user.name");
            if (!gpdb.checkDataBaseExists(userName)) {
                gpdb.createDataBase(userName, false);
            }

            initializeWorkingDirectory(hdfs, gpdb.getUserName());

            if (hdfsNonSecure != null) {
                initializeWorkingDirectory(hdfsNonSecure, gpdb.getUserName());
            }

            // get pxfHost
            // check if HA:
            pxfHost = hdfs.getHaNameservice();
            pxfPort = null; // HA doesn't have ip:port
            if (StringUtils.isEmpty(pxfHost)) {
                pxfPort = "5888";
                if (cluster instanceof MultiNodeCluster) {
                    Node pxfNode = ((MultiNodeCluster) cluster).getNode(PhdCluster.EnumClusterServices.pxf).get(0);
                    pxfHost = pxfNode.getHostName();
                    if (StringUtils.isEmpty(pxfNode.getHostName())) {
                        pxfHost = pxfNode.getHost();
                    }
                } else {
                    // if other than MultiNodeCluster get pxfHost from hdfs
                    pxfHost = hdfs.getHost();
                }
            }

            // run users before class
            beforeClass();

        } catch (Throwable t) {
            // in case of failure write stack trace to file stream and throw the exception
            t.printStackTrace(System.out);
            throw t;
        } finally {
            // anyways revert System.out to original stream
            CustomAutomationLogger.revertStdoutStream();
        }
    }

    /**
     * will be called after Class run has ended
     *
     * @throws Exception
     */
    @AfterClass(alwaysRun = true)
    public final void clean() throws Exception {
        if (ProtocolUtils.getPxfTestKeepData().equals("true")) {
            return;
        }
        // redirect "clean" logs to log file
        CustomAutomationLogger.redirectStdoutStreamToFile(getClass().getSimpleName(), "clean");
        try {
            // Remove local Data folder
            FileUtils.deleteDirectory(new File(dataTempFolder));
            afterClass();
        } catch (Throwable t) {
            // in case of failure write stack trace to file stream and throw the exception
            t.printStackTrace(System.out);
            throw t;
        } finally {
            // anyways revert System.out to original stream
            CustomAutomationLogger.revertStdoutStream();
        }
        // Remove workingDirectories
        removeWorkingDirectory(hdfs);
        removeWorkingDirectory(hdfsNonSecure);
    }

    /**
     * will be called before each test method start
     *
     * @throws Exception
     */
    @BeforeMethod(alwaysRun = true)
    public void runBeforeMethod() throws Exception {
        // check if "beforeMethod exists and if so open log file and run it
        if (checkMethodImplExists("beforeMethod")) {
            // redirect "runBeforeMethod" logs to log file
            CustomAutomationLogger.redirectStdoutStreamToFile(getClass().getSimpleName(), "beforeMethod");
            try {
                beforeMethod();
            } catch (Throwable t) {
                // in case of failure write stack trace to file stream and throw the exception
                t.printStackTrace(System.out);
                throw t;
            } finally {
                // anyways revert System.out to original stream
                CustomAutomationLogger.revertStdoutStream();
            }
        }
    }

    /**
     * will be called after each test method ended
     *
     * @throws Exception
     */
    @AfterMethod(alwaysRun = true)
    public void runAfterMethod() throws Exception {
        // check if "afterMethod exists and if so open log file and run it
        if (checkMethodImplExists("afterMethod")) {
            // redirect "runAfterMethod" logs to log file
            CustomAutomationLogger.redirectStdoutStreamToFile(getClass().getSimpleName(), "afterMethod");
            try {
                afterMethod();
            } catch (Throwable t) {
                // in case of failure write stack trace to file stream and throw the exception
                t.printStackTrace(System.out);
                throw t;
            } finally {
                // anyways revert System.out to original stream
                CustomAutomationLogger.revertStdoutStream();
            }
        }
    }

    /**
     * Run given tinc Tests
     *
     * @param tincTest
     * @throws Exception in case of test fails
     */
    protected void runTincTest(String tincTest) throws Exception {
        try {
            tinc.runTest(tincTest);
        } catch (Exception e) {
            throw new Exception("Tinc Failure (" + e.getMessage() + ")");
        }
    }

    /**
     * clean up after the class finished
     *
     * @throws Exception
     */
    protected void afterClass() throws Exception {
    }

    /**
     * Preparations needed before the class starting
     *
     * @throws Exception
     */
    protected void beforeClass() throws Exception {
    }

    /**
     * clean up after the test method had finished
     *
     * @throws Exception
     */
    protected void afterMethod() throws Exception {
    }

    /**
     * Preparations needed before the test method starting
     *
     * @throws Exception
     */
    protected void beforeMethod() throws Exception {
    }

    /**
     * Alert on not allowed annotations in test class children
     */
    @SuppressWarnings("unchecked")
    private void alertNotAllowedAnnotations() {
        // get test class methods
        Method[] methods = getClass().getDeclaredMethods();
        // array of not allowed in test class annotations
        @SuppressWarnings("rawtypes")
        Class[] notAllowedAnnotations = new Class[]{BeforeMethod.class, AfterMethod.class, BeforeClass.class, AfterClass.class};
        // go over test class methods
        for (Method method : methods) {
            // check if not allowed annotations appear in current method
            for (@SuppressWarnings("rawtypes") Class annotation : notAllowedAnnotations) {
                // if do put out message about it to System.err
                if (method.getAnnotation(annotation) != null) {
                    String annotationSimpleName = annotation.getSimpleName();
                    String needToOverideMethodName =
                            Character.toLowerCase(annotationSimpleName.charAt(0)) + annotationSimpleName.substring(1, annotationSimpleName.length());
                    System.err.println("Warning: " + annotationSimpleName + " is being used in " + getClass().getName() +
                            ", please override " + needToOverideMethodName + " method instead");
                }
            }
        }
    }

    /**
     * Check if the test writer used given method and return true if so.
     *
     * @param methodName to check
     * @return true if method exists in declared methods
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    private boolean checkMethodImplExists(String methodName) throws NoSuchMethodException, SecurityException {
        // get all declared methods
        Method[] methods = getClass().getDeclaredMethods();
        // run over methods and look for methodName
        for (Method method : methods) {

            if (method.getName().equals(methodName)) {
                return true;
            }
        }
        return false;
    }

    protected void trySecureLogin(Hdfs hdfs, String kerberosPrincipal) throws Exception {
        if (StringUtils.isEmpty(kerberosPrincipal)) return;

        String testUser = kerberosPrincipal.split("@")[0];
        String testUserKeytabPath = hdfs.getTestKerberosKeytab();
        if (StringUtils.isBlank(testUserKeytabPath)) {
            testUserKeytabPath = String.format(testUserkeyTabPathFormat, testUser);
        } else {
            testUserKeytabPath = testUserKeytabPath.replace("${pxf.base}", System.getenv("PXF_BASE"));
        }
        if (!new File(testUserKeytabPath).exists()) {
            throw new Exception(String.format("Keytab file %s not found", testUserKeytabPath));
        }
        if (StringUtils.isEmpty(hdfs.getHadoopRoot())) {
            throw new Exception("SUT parameter hadoopRoot in hdfs component is not defined");
        }
        // setup the security context for kerberos
        Configuration config = new Configuration();
        config.addResource(new Path(hdfs.getHadoopRoot() + "/conf/hdfs-site.xml"));
        config.addResource(new Path(hdfs.getHadoopRoot() + "/conf/core-site.xml"));
        config.reloadConfiguration();
        config.set("hadoop.security.authentication", "Kerberos");

        // Starting with Hadoop 2.10.0, the "DEFAULT" rule will throw an
        // exception when no rules are applied while getting the principal
        // name translation into operating system user name. See
        // org.apache.hadoop.security.authentication.util.KerberosName#getShortName
        // We add a default rule that will return the service name as the
        // short name, i.e. gpadmin/_HOST@REALM will map to gpadmin
        config.set(HADOOP_SECURITY_AUTH_TO_LOCAL, "RULE:[1:$1] RULE:[2:$1] DEFAULT");

        UserGroupInformation.setConfiguration(config);
        UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, testUserKeytabPath);

        // Initialize HDFS system object again, after login
        hdfs.init();
    }

    protected void initializeWorkingDirectory(Hdfs hdfs, String userName) throws Exception {
        hdfs.removeDirectory(hdfs.getWorkingDirectory());
        hdfs.createDirectory(hdfs.getWorkingDirectory());
        if (userName != null) {
            hdfs.setOwner("/" + StringUtils.removeStart(hdfs.getWorkingDirectory(), "/"),
                    userName, userName);
        }
    }

    protected void removeWorkingDirectory(Hdfs hdfs) {
        if (hdfs == null) return;
        try {
            hdfs.removeDirectory(hdfs.getWorkingDirectory());
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
