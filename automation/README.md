Pxf Automation
===================================
Contains all the PXF automation tests for the various functionalities of PXF  
The automation framework uses TestNg APIs and tools to complete automatic testing for PXF. Compiling and running TestNg test cases being used via maven.
The Automation framework also exposes simple APIs for performing actions on services including GPDB, PXF, HDFS, HBase and Hive or to use utilities such data comparison, reading and writing files , using tools such TINC for query analysis and more.
 
Dependencies
===================================
In order to run PXF automation tests the following are needed

1. Running Hadoop cluster 
2. Running GPDB
3. JRE 1.8

Build & Test
===================================

### SSH Setup

The PXF automation project uses an old SSH2 Java library that does not support newer key exchange algorithms (`KexAlgorithms`).
Newer operating systems such as MacOS 12+ and Debian's openssh-server package (1:8.4p1-5) do not enable support for these algorithms by default.
You can check the supported algorithms with

```bash
sudo sshd -T | grep 'kexalgorithms' | grep -e diffie-hellman-group-exchange-sha1 -e diffie-hellman-group14-sha1 -e diffie-hellman-group1-sha1
```

The following algorithms _must_ be included:

- diffie-hellman-group-exchange-sha1
- diffie-hellman-group14-sha1
- diffie-hellman-group1-sha1

If they are not, you can enable them with the following config file:

```bash
sudo tee -a /etc/ssh/sshd_config.d/pxf-automation-kex.conf <<EOF
# pxf automation uses an old SSH2 Java library that doesn't support newer KexAlgorithms
# this assumes that /etc/ssh/sshd_config contains "Include /etc/ssh/sshd_config.d/*.conf"
# if it doesn't, try adding this directly to /etc/ssh/sshd_config
KexAlgorithms +diffie-hellman-group-exchange-sha1,diffie-hellman-group14-sha1,diffie-hellman-group1-sha1
EOF
```


Then restart sshd based on your OS.
For MacOS, either run in terminal

```shell
sudo launchctl unload /System/Library/LaunchDaemons/ssh.plist
sudo launchctl load -w /System/Library/LaunchDaemons/ssh.plist
```

or go to System Preferences > Sharing and toggle `Remote Login`

For Linux, run

```shell
sudo systemctl reload ssh
```

Recheck the support algorithms before proceeding

```bash
sudo sshd -T | grep 'kexalgorithms' | grep -e diffie-hellman-group-exchange-sha1 -e diffie-hellman-group14-sha1 -e diffie-hellman-group1-sha1
```

In addition to updating the `sshd_config`, you must have an RSA key for the local system ([you're not still using RSA keys for SSH are you?][ssh-ed25519])

```bash
# requires an id_rsa key in PEM format
# private key *must* be stored in id_rsa
ssh-keygen -m PEM -t rsa -b 4096 -C "pxf-automation"
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```

[ssh-ed25519]: https://medium.com/risan/upgrade-your-ssh-key-to-ed25519-c6e8d60d3c54

## Python 2 Setup

While automation is a Java project, it uses a (no longer maintained) Python utility called [`tinc`](./tinc/main).
Unfortunately, this utility is still written in python 2.7.
This is problematic because:

- its dependencies (e.g., `paramiko`) are no longer available as system packages
- `pip` is not available as a system package anymore (confirmed on Ubuntu 20.04)

If pip is already installed then run:

```bash
python2 -m pip install --user paramiko
```

If the above fails because of OpenSSL errors on the M1 Apple Mac, please set the following before rerunning the pip command.

```bash
export LDFLAGS='-L/opt/homebrew/lib -L/opt/homebrew/opt/openssl@1.1/lib'
export CPPFLAGS='-I/opt/homebrew/include -I/opt/homebrew/opt/openssl@1.1/include'
```

### General Automation Setup

Set necessary Environment Vars
```
export GPHD_ROOT=<parent directory containing hadoop,hive,etc>
export PGPORT=<gpdb port>
export GPHOME=<your gphome>
export PXF_HOME=/usr/local/pxf
```

Run all tests for GPDB
```
make GROUP=gpdb
```

Run specific test 
```
make TEST=<testclassname>
```

Run specific method from a test
```
make TEST=<testclassname>#<method>
```

If you wish to remote debug your PXF Automation test case, use the following:
```
PXF_TEST_DEBUG=true make TEST=<testclassname>
```
This will allow you to attach to port 5005 for debugging purposes. See [IntelliJ Setup](#intellij-setup) for more details.


If you wish to run with cache 
```
make OFFLINE=true
```

Note: if you get an error saying that the jar does not exist, ensure that you have
a) installed the PXF server, and
b) only have 1 jar file inside `/usr/local/pxf/application/`

### Running automation tests for GPDB7
Tinc still runs on Python2 so it won't work with Python libraries that come with GP7. To overcome that, here is a small workaround in place.
We will copy the GP6 python libraries and then run the automation for GP7.

To support running the tests for GPDB 7, we have created this new directory, `tinc-python-deps` under `/pxf/automation/tinc/main`

Steps:
1. Download the GP6 Python libs `gsutil cp gs://data-gpdb-ud-pxf-build-resources/automation-dependencies/gp6-python-libs.tar.gz ~/workspace/pxf/automation/tinc/main/tinc-python-deps`
2. Unzip the file `tar -xvzf ~/workspace/pxf/automation/tinc/main/tinc-python-deps/gp6-python-libs.tar.gz`
3. export PXFAUTOMATIONTINCDEPS `export PXFAUTOMATIONTINCDEPS=~/workspace/pxf/automation/tinc/main/tinc-python-deps/`

### Project structure
_**src/main/java**_ - contains related classes and utilities for the test

**_src/test/java_** - contains the TestNG cases.

**_tincrepo_** - contains tinc test cases.

**_src/main/java/org/greenplum/pxf/automation/components_** - contains all the supported services/components with simple API abstractions. 

### General Automation Architecture
TestNG is a powerful Java testing framework very similar to junit which supports several features like grouping, parallel run and test events listeners.
You can read more about TestNG here http://testng.org/doc/index.md

## Guidelines for creating a new test for PXF automation

1.  Decide which category of run cycle (smoke/feature/load)
2.  Extend the right java class (according to the above test class hierarchy diagram)
3.  While implementing "smoke" test you can override three methods: "<span><span>prepareData", "</span></span><span><span>createTables" and "</span></span><span><span>queryResults" and than just call "</span></span><span>runTest" method from your test case for running the three mentioned methods in that order. (see *Example 1*)</span>
4.  Use TINC framework to test GPDB query results (see below "*Add TINC Cases*" section)

## Add TINC Cases

<img src="images/68747283.png" class="confluence-embedded-image confluence-content-image-border" width="381" height="603" />

**_tincrepo/main/pxf_**  - root for PXF TINC cases, the cases inside ordered in TestNg folders hierarchy

**_sql_** - sql files

**_expected_** - expected results matched to the sql files

**_output_** - output of run including diff files for each sql file

## Test Examples
1.  **Example 1 (Smoke case)**

    ``` theme:
    public class HdfsSmokeTest extends BaseSmoke {

        String fileName = "hdfsSmallData.txt";

        @Override
        protected void beforeClass() throws Exception {
        }

        @Override
        protected void prepareData() throws Exception {
            /**
             * Create Data and write it to HDFS
             */
            Table dataTable = getSmallData();

            hdfs.writeTextFile(hdfs.getWorkingDirectory() + "/" + fileName, dataTable.getData(), ",");
        }

        @Override
        protected void createTables() throws Exception {
            /**
             * Create GPDB external table directed to the HDFS file
             */
            exTable = TableFactory.getPxfReadableTextTable("pxf_smoke_small", new String[] {
                    "name text",
                    "num integer",
                    "dub double precision",
                    "longNum bigint",
                    "bool boolean" }, hdfs.getWorkingDirectory() + "/" + fileName, ",");

            gpdb.createTableAndVerify(exTable);
        }

        @Override
        protected void queryResults() throws Exception {
            /**
             * Run Tinc "small data" test
             */
            runTincTest("pxf.small_data.runTest");
        }
        @Test(groups = "smoke")
        public void test() throws Exception {
            runTest();
        }
    }
    ```

2.  **Example 2 (Feature case)**

    ``` theme:
    public class HdfsReadableTextTest extends BaseFeature {
        // holds data for file generation
        Table dataTable = null;
        // path for storing data on HDFS
        String hdfsFilePath = "";

        /**
         * Before every method determine default hdfs data Path, default data, and default external
         * table structure. Each case change it according to it needs.
         * 
         * @throws Exception
         */
        @Override
        protected void beforeMethod() throws Exception {
            super.beforeMethod();
            // path for storing data on HDFS
            hdfsFilePath = hdfs.getWorkingDirectory() + "/data";
            // prepare data in table
            dataTable = new Table("dataTable", null);
            FileFormatsUtils.prepareData(new CustomTextPreparer(), 100, dataTable);
            // default definition of external table
            exTable = new ReadableExternalTable("pxf_hdfs_small_data", new String[] {
                            "s1 text",
                            "s2 text",
                            "s3 text",
                            "d1 timestamp",
                            "n1 int",
                            "n2 int" }, hdfsFilePath, "TEXT");

            exTable.setHost(pxfHost);
        }

        /**
         * Read delimited text file from HDFS using explicit plugins and TEXT format.
         * 
         * @throws Exception
         */
        @Test(groups = { "features" })
        public void readDelimitedTextUsingTextFormat() throws Exception {
            // set plugins and delimiter
            exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
            exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.LineBreakAccessor");
            exTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");
            exTable.setDelimiter(",");
            // create external table
            gpdb.createTableAndVerify(exTable);
            // write data to HDFS
            hdfs.writeTableToFile(hdfsFilePath, dataTable, ",");
            // verify results
            runTincTest("pxf.features.hdfs.readable.text.small_data.runTest");
        }

        /**
         * Verify deprecated "LineReaderAccessor" gives required results.
         * 
         * @throws Exception
         */
        @Test(groups = "features")
        public void deprecatedLineReaderAccessor() throws Exception {
            // set plugins and delimiter
            exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
            exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.LineReaderAccessor");
            exTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");
            exTable.setDelimiter(",");
            // create external table
            gpdb.createTableAndVerify(exTable);
            // write data to HDFS
            hdfs.writeTableToFile(hdfsFilePath, dataTable, ",");
            // verify results
            runTincTest("pxf.features.hdfs.readable.text.small_data.runTest");
        }
    }
    ```
    
## Run reports and logs
 
Automation logs will generated into "automation-logs" directory and will be divided to directories according to each ran java classes.

In every "class" directory will be files according to the following format: <time-stamp>_<ran method name>.log
<img src="images/72680961.png" class="confluence-embedded-image confluence-content-image-border" width="454" height="598" />

TestNg report will generated into target/surefire-reports
<img src="images/68125531.png" class="confluence-embedded-image confluence-content-image-border" width="1084" height="612" />

# IDE Setup (IntelliJ) and Automation Debugging

There are 2 flags that can be used to debug automation tests:

1. `PXF_TEST_KEEP_DATA` - does not remove any test data after an automation test.
2. `PXF_TEST_DEBUG` - will cause the automation application to wait for a debugger to attach to the session before proceeding.

### Intelli Automation Debug Setup
In IntelliJ, create an `Automation Debug` configuration:

1. Click Run > Edit Configurations
2. Select '+', and pick 'Remote JVM Debug'
3. Name the configuration `Automation Debug` and set the host to `localhost` and the port to `5005`
4. Set your breakpoints in the automation test of your choice.
5. Run automation with `PXF_TEST_DEBUG=true`

### PXF Server Debug during an Automation Debug

There are 2 different JVMs running when automation tests. The first is the automation application itself that can be debugged. The second is the PXF server that the automation suite is testing.
Setting the `PXF_TEST_DEBUG` flag in the automation app will also set the flag `PXF_DEBUG` to enable debugging of the PXF server itself if the automation restarts PXF server during some of the tests. This is done so that a debugger can be connected to a PXF server restarted by the automation tests.

Follow the steps [here](https://github.com/greenplum-db/pxf#debugging-the-locally-running-instance-of-pxf-server-using-intellij) to set up a PXF server debug configuration if it is not already done.

1. Run automation with `PXF_TEST_DEBUG=true` to connect to the automation debug session.
2. Attach to the automation debug session
3. Add a breakpoint at the beginning of the automation test case you are running, or at the end of the beforeClass block to ensure that the PXF server debug session properly attaches
4. Attach to the PXF server debug session
5. Debug as you would normally.
