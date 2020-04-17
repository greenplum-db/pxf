package org.greenplum.pxf.automation.components.hdfs;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import jsystem.framework.system.SystemObject;

import com.google.protobuf.GeneratedMessage;
import org.greenplum.pxf.automation.fileformats.IAvroSchema;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Define functionality of File System
 */
public interface IFSFunctionality extends SystemObject {

    /**
     * get list of Directories and Files for giver path
     *
     * @param path
     * @return {@link List} of Strings
     * @throws Exception
     */
    public List<String> list(String path) throws Exception;

    /**
     * Copy File or directory from local to remote File System
     *
     * @param srcPath
     * @param destPath
     * @throws Exception
     */
    public void copyFromLocal(String srcPath, String destPath) throws Exception;

    /**
     * Copy File or directory from remote File System to local.
     *
     * @param srcPath
     * @param destPath
     * @throws Exception
     */
    public void copyToLocal(String srcPath, String destPath) throws Exception;

    /**
     * Create new Directory
     *
     * @param path
     * @throws Exception
     */
    public void createDirectory(String path) throws Exception;

    /**
     * Remove exists Directory
     *
     * @param path
     * @throws Exception
     */
    public void removeDirectory(String path) throws Exception;

    /**
     * get File content as String
     *
     * @param path
     * @return file content as String
     * @throws Exception
     */
    public String getFileContent(String path) throws Exception;

    /**
     * get Size of Directories and files in giver path
     *
     * @param path
     * @return Size of Directories and files in giver path
     * @throws Exception
     */
    public int listSize(String path) throws Exception;

    /**
     * Write Data Table to file
     *
     * @param destPath
     * @param dataTable Table with data to write
     * @param delimiter put delimiter between columns
     * @throws Exception
     */
    public void writeTableToFile(String destPath, Table dataTable,
                                 String delimiter) throws Exception;

    /***
     * Write Data Table to file using different encodings
     *
     * @param destPath destination file
     * @param dataTable {@link Table} with List of data
     * @param delimiter between fields
     * @param encoding to use to write the file
     * @throws Exception
     */
    public void writeTableToFile(String destPath, Table dataTable,
                                 String delimiter, Charset encoding)
            throws Exception;

    /**
     * Write Dequence Object to file
     *
     * @param writableData
     * @param destPath
     * @throws IOException
     */
    public void writeSequenceFile(Object[] writableData, String destPath)
            throws IOException;

    /**
     * Write {@link IAvroSchema} data class to path according to schemaName. If
     * not null, the file will be compressed using the given codecName.
     * Supported types are specified in {@code CodecFactory}.
     *
     * @param pathToFile
     * @param schemaName
     * @param codecName
     * @param data
     * @throws Exception
     */
    public void writeAvroFile(String pathToFile, String schemaName,
                              String codecName, IAvroSchema[] data)
            throws Exception;

    /**
     * Generate and write Avro file to hdfs, from a schema file and a data file
     * in json format. The file is generated using avro-tools. If not null, the
     * file will be compressed using the given codecName. Supported types are
     * specified in {@code CodecFactory}.
     *
     * @param pathToFile path to file in hdfs
     * @param schemaName path to local schema file
     * @param jsonFileName path to local data file
     * @param codecName codec name
     * @throws Exception
     */
    public void writeAvroFileFromJson(String pathToFile, String schemaName,
                                      String jsonFileName, String codecName)
            throws Exception;

    /**
     * Write {@link IAvroSchema} data class to path according to schemaName to a
     * Sequence file
     *
     * @param pathToFile
     * @param schemaName
     * @param data
     * @throws Exception
     */
    public void writeAvroInSequenceFile(String pathToFile, String schemaName,
                                        IAvroSchema[] data) throws Exception;

    /**
     * @return Replication Size
     */
    public short getReplicationSize();

    /**
     * Set Replication Size
     *
     * @param replicationSize
     */
    public void setReplicationSize(short replicationSize);

    /**
     * @return Block Size
     */
    public long getBlockSize();

    /**
     * Set Block size
     *
     * @param blockSize
     */
    public void setBlockSize(long blockSize);

    /**
     * Set Default Replication size
     */
    public void setDefaultReplicationSize();

    /**
     * Set Default Block size
     */
    public void setDefaultBlockSize();

    /**
     * Write Protocol Buffered data stored in {@link GeneratedMessage}
     *
     * @param filePath
     * @param generatedMessages
     * @throws Exception
     */
    public void writeProtocolBufferFile(String filePath,
                                        com.google.protobuf.GeneratedMessage generatedMessages)
            throws Exception;

    public boolean doesFileExist(String pathToFile) throws Exception;
}
