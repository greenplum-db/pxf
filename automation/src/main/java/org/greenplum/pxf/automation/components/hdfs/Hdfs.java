package org.greenplum.pxf.automation.components.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.tool.DataFileGetMetaTool;
import org.apache.avro.tool.DataFileReadTool;
import org.apache.avro.tool.DataFileWriteTool;
import org.apache.avro.tool.Tool;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.greenplum.pxf.automation.components.common.BaseSystemObject;
import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.fileformats.IAvroSchema;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Represents HDFS, holds HdfsFunctionality interface as a member, the
 * implementation needs to be mentioned in the SUT file.
 */
public class Hdfs extends BaseSystemObject implements IFSFunctionality {
    public static final int K_BYTE = 1024;
    public static final int M_BYTE = K_BYTE * K_BYTE;
    private FileSystem fs;
    private Configuration config;
    // NN host
    private String host;
    private String hostStandby;
    private String port = "8020";
    private String hadoopRoot;
    private short replicationSize;
    private long blockSize;
    private int bufferSize;
    private final int ROW_BUFFER = 10000;
    private String workingDirectory;
    private String haNameservice;
    private String sshUserName;
    private String sshPrivateKey;
    private String testKerberosPrincipal;
    private String testKerberosKeytab;
    private String useDatanodeHostname;

    private String scheme;

    // for SSH connection to the namenode and performing HA failover operations
    private ShellSystemObject namenodeSso;
    private String namenodePrincipal;
    private String namenodeKeytab;
    private String relativeWorkingDirectory;

    public String getRelativeWorkingDirectory() {
        return relativeWorkingDirectory;
    }

    public Hdfs() {

    }

    public Hdfs(FileSystem fileSystem, Configuration conf, boolean silentReport) throws Exception {
        super(silentReport);
        ReportUtils.startLevel(report, getClass(), "Init");
        fs = fileSystem;
        config = conf;
        setDefaultReplicationSize();
        setDefaultBlockSize();
        setDefaultBufferSize();
        ReportUtils.report(report, getClass(), "Block Size: " + getBlockSize());
        ReportUtils.report(report, getClass(), "Replications: "
                + getReplicationSize());
        ReportUtils.stopLevel(report);
    }

    public Hdfs(boolean silentReport) {
        super(silentReport);
    }

    @Override
    public void init() throws Exception {
        super.init();
        ReportUtils.startLevel(report, getClass(), "Init");
        config = new Configuration();

        // if hadoop root exists in the SUT file, load configuration from it
        if (StringUtils.isNotEmpty(hadoopRoot)) {

            hadoopRoot = replaceUser(hadoopRoot);
            ReportUtils.startLevel(report, getClass(), "Using root directory: " + hadoopRoot);

            ProtocolEnum protocol = ProtocolUtils.getProtocol();
            if (protocol == ProtocolEnum.HDFS) {
                config.addResource(new Path(getHadoopRoot() + "/conf/core-site.xml"));
                config.addResource(new Path(getHadoopRoot() + "/conf/hdfs-site.xml"));
                config.addResource(new Path(getHadoopRoot() + "/conf/mapred-site.xml"));
            } else {
                // (i.e) For s3 protocol the file should be s3-site.xml
                config.addResource(new Path(getHadoopRoot() + "/" + protocol.value() + "-site.xml"));
                config.addResource(new Path(getHadoopRoot() + "/mapred-site.xml"));
                config.set("fs.defaultFS", getScheme() + "://" + getWorkingDirectory());
            }
        } else {

            if (StringUtils.isEmpty(host)) {
                throw new Exception("host in hdfs component not configured in SUT");
            }

            if (StringUtils.isNotEmpty(haNameservice)) {
                if (StringUtils.isEmpty(hostStandby)) {
                    throw new Exception(
                            "hostStandby in hdfs component not configured in SUT");
                }
                config.set("fs.defaultFS", "hdfs://" + haNameservice + "/");
                config.set("dfs.client.failover.proxy.provider.mycluster",
                        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
                config.set("dfs.nameservices", haNameservice);
                config.set("dfs.ha.namenodes" + "." + haNameservice, "nn1,nn2");
                config.set(
                        "dfs.namenode.rpc-address." + haNameservice + ".nn1",
                        host + ":" + port);
                config.set(
                        "dfs.namenode.rpc-address." + haNameservice + ".nn2",
                        hostStandby + ":" + port);
            } else {
                config.set("fs.defaultFS", "hdfs://" + host + ":" + port + "/");
            }
        }

        // for Hadoop clusters provisioned in the cloud when running from local workstation
        if (useDatanodeHostname != null && Boolean.parseBoolean(useDatanodeHostname)) {
            config.set("dfs.client.use.datanode.hostname", "true");
        }

        config.set("ipc.client.fallback-to-simple-auth-allowed", "true");

        fs = FileSystem.get(config);
        setDefaultReplicationSize();
        setDefaultBlockSize();
        setDefaultBufferSize();
        ReportUtils.report(report, getClass(), "Block Size: " + getBlockSize());
        ReportUtils.report(report, getClass(), "Replications: "
                + getReplicationSize());

        if (getSshUserName() != null) {
            ReportUtils.report(report, getClass(), "Opening connection to namenode " + getHost());
            namenodeSso = new ShellSystemObject(report.isSilent());
            String namenodeHost = getHost();
            if (namenodeHost != null && namenodeHost.equals("ipa-hadoop")) {
                // this is for local testing, where hostname in SUT will be "ipa-hadoop", tests is CI substitute
                // it with a short hostname
                namenodeHost = getHostForConfiguredNameNode1HA();
            }
            namenodeSso.setHost(namenodeHost);
            namenodeSso.setUserName(getSshUserName());
            namenodeSso.setPrivateKey(getSshPrivateKey());
            namenodeSso.init();


            // source environment file
            namenodeSso.runCommand("source ~/.bash_profile");
            namenodePrincipal = config.get("dfs.namenode.kerberos.principal");
            namenodeKeytab = config.get("dfs.namenode.keytab.file");
            if (namenodePrincipal != null) {
                // substitute _HOST portion of the principal with the namenode FQDN, need to get it from the
                // configuration, since namenodeHost might contain a short hostname
                namenodePrincipal = namenodePrincipal.replace("_HOST", getHostForConfiguredNameNode1HA());
                // kinit as the principal to be ready to perform HDFS commands later
                // e.g. "kinit -kt /opt/security/keytab/hdfs.service.keytab hdfs/ccp-user-nn01.c.gcp-project.internal"
                StringBuilder kinitCommand = new StringBuilder("kinit -kt ")
                        .append(namenodeKeytab).append(" ").append(namenodePrincipal);
                namenodeSso.runCommand(kinitCommand.toString());
            }
        }
        ReportUtils.stopLevel(report);
    }

    @Override
    public void setDefaultReplicationSize() {
        setReplicationSize(fs.getDefaultReplication(new Path("/")));
    }

    @Override
    public void setDefaultBlockSize() {
        setBlockSize(fs.getDefaultBlockSize(new Path("/")));
    }

    private void setDefaultBufferSize() {
        bufferSize = Integer.parseInt(config.get("io.file.buffer.size"));
    }

    private Path getDatapath(String path) {
        String pathString = path;
        if (!path.matches("^[a-zA-Z].*://.*$")) {
            if (ProtocolUtils.getProtocol() != ProtocolEnum.HDFS) {
                pathString = getScheme() + "://" + path;
            } else {
                pathString = "/" + path;
            }
        }

        return new Path(pathString);
    }

    @Override
    public ArrayList<String> list(String path) throws Exception {
        ReportUtils.startLevel(report, getClass(), "List From " + path);
        RemoteIterator<LocatedFileStatus> list = fs.listFiles(getDatapath(path), true);
        ArrayList<String> filesList = new ArrayList<>();
        while (list.hasNext()) {
            filesList.add(list.next().getPath().toString());
        }
        ReportUtils.report(report, getClass(), filesList.toString());
        ReportUtils.stopLevel(report);
        return filesList;
    }

    @Override
    public int listSize(String hdfsDir) throws Exception {
        return list(hdfsDir).size();
    }

    public void setOwner(String path, String userName, String groupName)
            throws Exception {
        ReportUtils.startLevel(report, getClass(), "Change owner to user "
                + userName + ", group " + groupName + " for path " + path);
        fs.setOwner(new Path(path), userName, groupName);
        ReportUtils.stopLevel(report);
    }

    public void setMode(String path, String mode)
            throws Exception {
        ReportUtils.startLevel(report, getClass(), "Change mode to "
                + mode + " for path " + path);
        fs.setPermission(new Path(path), new FsPermission(mode));
        ReportUtils.stopLevel(report);
    }

    @Override
    public void copyFromLocal(String srcPath, String destPath) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Copy from " + srcPath
                + " to " + destPath);
        fs.copyFromLocalFile(new Path(srcPath), getDatapath(destPath));
        ReportUtils.stopLevel(report);
    }

    @Override
    public void copyToLocal(String srcPath, String destPath) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Copy to " + destPath
                + " from " + srcPath);
        fs.copyToLocalFile(new Path(srcPath), new Path(destPath));
        ReportUtils.stopLevel(report);
    }

    @Override
    public void createDirectory(String path) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Create Directory " + path);
        fs.mkdirs(getDatapath(path));
        ReportUtils.stopLevel(report);
    }

    @Override
    public void removeDirectory(String path) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Remove Directory " + path);
        Path dataPath = getDatapath(path);
        if (fs.exists(dataPath)) {
            fs.delete(dataPath, true);
        }
        ReportUtils.stopLevel(report);
    }

    @Override
    public String getFileContent(String path) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Get file content");
        FSDataInputStream fsdis = fs.open(getDatapath(path));
        StringWriter writer = new StringWriter();
        IOUtils.copy(fsdis, writer, "UTF-8");
        ReportUtils.report(report, getClass(), writer.toString());
        ReportUtils.stopLevel(report);
        return writer.toString();
    }

    @Override
    public void writeSequenceFile(Object[] writableData, String pathToFile)
            throws IOException {
        ReportUtils.startLevel(report, getClass(),
                "Writing Sequence file from "
                        + writableData[0].getClass().getName() + " array to "
                        + pathToFile);
        IntWritable key = new IntWritable();
        Path path = getDatapath(pathToFile);

        // Even though this method is deprecated we need to pass the correct
        // fs for multi hadoop tests
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, config,
                path, key.getClass(), writableData[0].getClass());
        for (int i = 1; i < writableData.length; i++) {
            writer.append(key, writableData[i]);
        }
        writer.close();
        ReportUtils.stopLevel(report);
    }

    @Override
    public void writeAvroInSequenceFile(String pathToFile, String schemaName,
                                        IAvroSchema[] data) throws Exception {
        IntWritable key = new IntWritable();
        BytesWritable val = new BytesWritable();
        Path path = getDatapath(pathToFile);

        // Even though this method is deprecated we need to pass the correct
        // fs for multi hadoop tests
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, config, path,
                key.getClass(), val.getClass());

        for (IAvroSchema datum : data) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            datum.serialize(stream);
            val = new BytesWritable(stream.toByteArray());
            writer.append(key, val);
        }
        writer.close();
    }

    @Override
    public void writeAvroFile(String pathToFile, String schemaName,
                              String codecName, IAvroSchema[] data)
            throws Exception {
        Path path = getDatapath(pathToFile);
        OutputStream outStream = fs.create(path, true, bufferSize,
                replicationSize, blockSize);
        Schema schema = new Schema.Parser().parse(new FileInputStream(
                schemaName));
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(
                schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                writer);
        if (!StringUtils.isEmpty(codecName)) {
            dataFileWriter.setCodec(CodecFactory.fromString(codecName));
        }

        dataFileWriter.create(schema, outStream);

        for (IAvroSchema iAvroSchema : data) {
            GenericRecord datum = iAvroSchema.serialize();
            dataFileWriter.append(datum);
        }
        dataFileWriter.close();
    }

    @Override
    public void writeAvroFileFromJson(String pathToFile, String schemaName,
                                      String jsonFileName, String codecName)
            throws Exception {
        ReportUtils.startLevel(report, getClass(), "Write Avro File to "
                + pathToFile + " (schema file " + schemaName
                + ", json data file " + jsonFileName + ")");

        Tool tool = new DataFileWriteTool();
        List<String> args = Arrays.asList("--schema-file", schemaName, jsonFileName);
        if (codecName != null) {
            args.add("--codec");
            args.add(codecName);
        }

        FSDataOutputStream out = fs.create(getDatapath(pathToFile), true,
                bufferSize, replicationSize, blockSize);
        PrintStream printStream = new PrintStream(out);

        tool.run(null, printStream, System.err, args);

        printStream.flush();
        printStream.close();

        ReportUtils.stopLevel(report);
    }

    public void writeJsonFileFromAvro(String pathToFile, String pathToJson)
            throws Exception {
        Tool tool = new DataFileReadTool();
        List<String> args = Collections.singletonList(pathToFile);

        try (PrintStream printStream = new PrintStream(new FileOutputStream(new File(pathToJson)))) {
            tool.run(null, printStream, System.err, args);
        }
    }

    public void writeAvroMetadata(String pathToFile, String pathToMetadata)
            throws Exception {
        Tool tool = new DataFileGetMetaTool();
        List<String> args = Collections.singletonList(pathToFile);

        try (PrintStream printStream = new PrintStream(new FileOutputStream(new File(pathToMetadata)))) {
            tool.run(null, printStream, System.err, args);
        }
    }

    @Override
    public void writeProtocolBufferFile(String filePath,
                                        com.google.protobuf.GeneratedMessage data)
            throws Exception {
        Path path = getDatapath(filePath);
        OutputStream out_stream = fs.create(path, true, bufferSize,
                replicationSize, blockSize);
        data.writeTo(out_stream);
        out_stream.close();
    }

    @Override
    public boolean doesFileExist(String pathToFile) throws Exception {
        return fs.exists(new Path(pathToFile));
    }

    @Override
    public short getReplicationSize() {
        return replicationSize;
    }

    @Override
    public void setReplicationSize(short replicationSize) {
        this.replicationSize = replicationSize;
    }

    @Override
    public long getBlockSize() {
        return blockSize;
    }

    @Override
    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public void writeTableToFile(String pathToFile, Table dataTable,
                                 String delimiter) throws Exception {
        writeTableToFile(pathToFile, dataTable, delimiter,
                StandardCharsets.UTF_8);
    }

    @Override
    public void writeTableToFile(String pathToFile, Table dataTable,
                                 String delimiter, Charset encoding)
            throws Exception {
        writeTableToFile(pathToFile, dataTable, delimiter, encoding, null);
    }

    @Override
    public void writeTableToFile(String destPath, Table dataTable,
                                 String delimiter, Charset encoding,
                                 CompressionCodec codec) throws Exception {
        writeTableToFile(destPath, dataTable, delimiter, encoding, codec, "\n");
    }

    @Override
    public void writeTableToFile(String destPath, Table dataTable,
                                 String delimiter, Charset encoding,
                                 CompressionCodec codec, String newLine) throws Exception {

        ReportUtils.startLevel(report, getClass(),
                "Write Text File (Delimiter = '" + delimiter + "', NewLine = '" + newLine + "') to "
                        + destPath
                        + ((encoding != null) ? " encoding: " + encoding : ""));

        FSDataOutputStream out = fs.create(getDatapath(destPath), true,
                bufferSize, replicationSize, blockSize);

        DataOutputStream dos = out;
        if (codec != null) {
            dos = new DataOutputStream(codec.createOutputStream(out));
        }

        writeTableToStream(dos, dataTable, delimiter, encoding, newLine);
        ReportUtils.stopLevel(report);
    }

    public void appendTableToFile(String pathToFile, Table dataTable, String delimiter) throws Exception {
        appendTableToFile(pathToFile, dataTable, delimiter, StandardCharsets.UTF_8);
    }

    private void appendTableToFile(String pathToFile, Table dataTable, String delimiter, Charset encoding) throws Exception {
        ReportUtils.startLevel(report, getClass(),
                "Append Text File (Delimiter = '" + delimiter + "') to "
                        + pathToFile
                        + ((encoding != null) ? " encoding: " + encoding : ""));


        FSDataOutputStream out = fs.append(getDatapath(pathToFile));
        out.writeBytes("\n"); // Need to start on a new line
        writeTableToStream(out, dataTable, delimiter, encoding, "\n");
        ReportUtils.stopLevel(report);
    }

    private void writeTableToStream(DataOutputStream stream, Table dataTable,
                                    String delimiter, Charset encoding, String newLine) throws Exception {
        BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(stream, encoding));
        List<List<String>> data = dataTable.getData();

        for (int i = 0, flushThreshold = 0; i < data.size(); i++, flushThreshold++) {
            List<String> row = data.get(i);
            StringBuilder sBuilder = new StringBuilder();
            for (int j = 0; j < row.size(); j++) {
                sBuilder.append(row.get(j));
                if (j != row.size() - 1) {
                    sBuilder.append(delimiter);
                }
            }
            if (i != data.size() - 1) {
                sBuilder.append(newLine);
            }
            bufferedWriter.append(sBuilder.toString());
            if (flushThreshold > ROW_BUFFER) {
                bufferedWriter.flush();
            }
        }
        bufferedWriter.close();
    }

    public String getNamenodeStatus(String name) throws Exception {
        // to run manually:
        // source ~/.bash_profile && kinit -kt /opt/security/keytab/hdfs.service.keytab hdfs/ccp-user-nn01.c.gcp-project.internal && ./singlecluster-HDP/bin/hdfs haadmin -getServiceState nn01
        String statusCommand = "./singlecluster-HDP/bin/hdfs haadmin -getServiceState " + name;
        namenodeSso.runCommand(statusCommand);
        String fullResponse = namenodeSso.getLastCmdResult();
        if (fullResponse.contains(String.format("haadmin -getServiceState %s\r\nactive\r\n", name))) {
            return "active";
        } else if (fullResponse.contains(String.format("haadmin -getServiceState %s\r\nstandby\r\n", name))) {
            return "standby";
        }
        throw new IllegalStateException("Unknown status: " + fullResponse);
    }

    public void failover(String from, String to) throws Exception {
        assertEquals("active", getNamenodeStatus(from));
        assertEquals("standby", getNamenodeStatus(to));

        // to run manually:
        // source ~/.bash_profile && kinit -kt /opt/security/keytab/hdfs.service.keytab hdfs/ccp-user-nn01.c.gcp-project.internal && ./singlecluster-HDP/bin/hdfs haadmin -failover nn01 nn02"
        String failoverCommand = "./singlecluster-HDP/bin/hdfs haadmin -failover " + from + " " + to;

        namenodeSso.runCommand(failoverCommand);
        String fullResponse = namenodeSso.getLastCmdResult();
        if (!fullResponse.contains(String.format("Failover from %s to %s successful", from, to))) {
            throw new IllegalStateException("Failed to failover: " + fullResponse);
        }

        assertEquals("active", getNamenodeStatus(to));
        assertEquals("standby", getNamenodeStatus(from));
    }
    /**
     * @return the hadoop configuration
     */
    public Configuration getConfiguration() {
        return config;
    }

    /**
     * @return Default FS configured NN address from loaded configuration
     */
    public String getConfiguredNameNodeAddress() {
        return config.get("fs.defaultFS");
    }

    public String getHostForConfiguredNameNode1HA() {
        String nameservice = config.get("dfs.nameservices");
        String nn01 = config.get(String.format("dfs.namenode.rpc-address.%s.nn01", nameservice));
        return nn01 == null ? null : nn01.substring(0, nn01.indexOf(":"));
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = replaceUser(host);
    }

    public String getHostStandby() {
        return hostStandby;
    }

    public void setHostStandby(String hostStandby) {
        this.hostStandby = hostStandby;
    }

    public String getWorkingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(String workingDirectory) {
        this.relativeWorkingDirectory = workingDirectory;

        this.workingDirectory = workingDirectory;

        if (workingDirectory != null) {
            String basePath = getBasePath();
            String uuid = UUID.randomUUID().toString();

            this.relativeWorkingDirectory = workingDirectory
                    .replace("${base.path}/", "")
                    .replace("__UUID__", uuid);

            this.workingDirectory = workingDirectory
                    .replace("${base.path}", basePath)
                    .replace("__UUID__", uuid);
        }
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getHadoopRoot() {
        return hadoopRoot;
    }

    public void setHadoopRoot(String hadoopRoot) {
        this.hadoopRoot = hadoopRoot;

        if (this.hadoopRoot != null) {
            String pxfHome = System.getenv("PXF_HOME");
            String pxfBase = StringUtils.defaultIfBlank(System.getenv("PXF_BASE"), pxfHome);
            this.hadoopRoot = replaceHome(this.hadoopRoot.replace("${pxf.base}", pxfBase));
        }
    }

    public String getBasePath() {
        return StringUtils.defaultIfBlank(System.getenv("BASE_PATH"), "");
    }

    public String getTestKerberosPrincipal() {
        return testKerberosPrincipal;
    }

    public void setTestKerberosPrincipal(String testKerberosPrincipal) {
        this.testKerberosPrincipal = testKerberosPrincipal;
    }

    public String getTestKerberosKeytab() {
        return testKerberosKeytab;
    }

    public void setTestKerberosKeytab(String testKerberosKeytab) {
        this.testKerberosKeytab = testKerberosKeytab;
    }

    public String getHaNameservice() {
        return haNameservice;
    }

    public void setHaNameservice(String haNameservice) {
        this.haNameservice = haNameservice;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public String getSshUserName() {
        return sshUserName;
    }

    public void setSshUserName(String sshUserName) {
        this.sshUserName = sshUserName;
    }

    public String getSshPrivateKey() {
        return sshPrivateKey;
    }

    public void setSshPrivateKey(String sshPrivateKey) {
        this.sshPrivateKey = replaceHome(sshPrivateKey);
    }

    public String getUseDatanodeHostname() {
        return useDatanodeHostname;
    }

    public void setUseDatanodeHostname(String useDatanodeHostname) {
        this.useDatanodeHostname = useDatanodeHostname;
    }
}
