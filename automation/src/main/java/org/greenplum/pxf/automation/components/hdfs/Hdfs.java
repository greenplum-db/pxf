package org.greenplum.pxf.automation.components.hdfs;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.tool.*;
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
import org.apache.hadoop.io.SequenceFile.Writer;

import org.greenplum.pxf.automation.components.common.BaseSystemObject;
import org.greenplum.pxf.automation.fileformats.IAvroSchema;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;

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

        if (StringUtils.isEmpty(host)) {
            throw new Exception("host in hdfs component not configured in SUT");
        }

        // if hadoop root exists in the SUT file, load configuration from it
        if (StringUtils.isNotEmpty(hadoopRoot)) {
            ProtocolEnum protocol = ProtocolUtils.getProtocol();
            if (protocol == ProtocolEnum.HDFS) {
                config.addResource(new Path(getHadoopRoot() + "/conf/core-site.xml"));
                config.addResource(new Path(getHadoopRoot() + "/conf/hdfs-site.xml"));
                config.addResource(new Path(getHadoopRoot() + "/conf/mapred-site.xml"));
            } else {
                // For s3 protocol the file should be s3-site.xml
                config.addResource(new Path(getHadoopRoot() + "/" + protocol.value() + "-site.xml"));
                config.addResource(new Path(getHadoopRoot() + "/mapred-site.xml"));
            }
        } else {
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

        fs = FileSystem.get(config);
        setDefaultReplicationSize();
        setDefaultBlockSize();
        setDefaultBufferSize();
        ReportUtils.report(report, getClass(), "Block Size: " + getBlockSize());
        ReportUtils.report(report, getClass(), "Replications: "
                + getReplicationSize());
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
        bufferSize = Integer.valueOf(config.get("io.file.buffer.size"));
    }

    private Path getDatapath(String path) {
        if (path.matches("^[a-zA-Z].*://.*$"))
            return new Path(path);
        else
            return new Path("/" + path);
    }

    @Override
    public ArrayList<String> list(String path) throws Exception {
        ReportUtils.startLevel(report, getClass(), "List From " + path);
        RemoteIterator<LocatedFileStatus> list = fs.listFiles(getDatapath(path), true);
        ArrayList<String> filesList = new ArrayList<String>();
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
        ReportUtils.startLevel(report, getClass(), "Copy From " + srcPath
                + " to " + destPath);
        fs.copyFromLocalFile(new Path(srcPath), getDatapath(destPath));
        ReportUtils.stopLevel(report);
    }

    @Override
    public void copyToLocal(String srcPath, String destPath) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Copy to " + srcPath
                + " from " + destPath);
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
        fs.delete(getDatapath(path), true);
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
        Writer.Option optPath = SequenceFile.Writer.file(path);
        Writer.Option optKey = SequenceFile.Writer.keyClass(key.getClass());
        Writer.Option optVal = SequenceFile.Writer.valueClass(writableData[0].getClass());
        SequenceFile.Writer writer = SequenceFile.createWriter(config, optPath,
                optKey, optVal);
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

        Writer.Option optPath = SequenceFile.Writer.file(path);
        Writer.Option optKey = SequenceFile.Writer.keyClass(key.getClass());
        Writer.Option optVal = SequenceFile.Writer.valueClass(val.getClass());

        SequenceFile.Writer writer = SequenceFile.createWriter(config, optPath,
                optKey, optVal);

        for (int i = 0; i < data.length; i++) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            data[i].serialize(stream);
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
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
                schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
                writer);
        if (!StringUtils.isEmpty(codecName)) {
            dataFileWriter.setCodec(CodecFactory.fromString(codecName));
        }

        dataFileWriter.create(schema, outStream);

        for (int i = 0; i < data.length; i++) {
            GenericRecord datum = data[i].serialize();
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
        List<String> args = new ArrayList<String>();
        args.add("--schema-file");
        args.add(schemaName);
        args.add(jsonFileName);
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
        ReportUtils.startLevel(report, getClass(),
                "Write Text File (Delimiter = '" + delimiter + "') to "
                        + pathToFile
                        + ((encoding != null) ? " encoding: " + encoding : ""));

        FSDataOutputStream out = fs.create(getDatapath(pathToFile), true,
                bufferSize, replicationSize, blockSize);
        writeTableToStream(out, dataTable, delimiter, encoding);
        ReportUtils.stopLevel(report);
    }

    public void appendTableToFile(String pathToFile, Table dataTable, String delimiter) throws Exception {
        appendTableToFile(pathToFile, dataTable, delimiter, StandardCharsets.UTF_8);
    }

    public void appendTableToFile(String pathToFile, Table dataTable, String delimiter, Charset encoding) throws Exception {
        ReportUtils.startLevel(report, getClass(),
                "Append Text File (Delimiter = '" + delimiter + "') to "
                        + pathToFile
                        + ((encoding != null) ? " encoding: " + encoding : ""));


        FSDataOutputStream out = fs.append(getDatapath(pathToFile));
        out.writeBytes("\n"); // Need to start on a new line
        writeTableToStream(out, dataTable, delimiter, encoding);
        ReportUtils.stopLevel(report);
    }

    private void writeTableToStream(FSDataOutputStream stream, Table dataTable, String delimiter, Charset encoding) throws Exception {
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
                sBuilder.append("\n");
            }
            bufferedWriter.append(sBuilder.toString());
            if (flushThreshold > ROW_BUFFER) {
                bufferedWriter.flush();
            }
        }
        bufferedWriter.close();
    }

    /**
     * @return Default FS configured NN address from loaded configuration
     */
    public String getConfiguredNameNodeAdress() {
        return config.get("fs.defaultFS");
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
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
        this.workingDirectory = workingDirectory;
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
    }

    public String getHaNameservice() {
        return haNameservice;
    }

    public void setHaNameservice(String haNameservice) {
        this.haNameservice = haNameservice;
    }
}
