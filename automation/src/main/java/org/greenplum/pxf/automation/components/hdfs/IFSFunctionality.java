package org.greenplum.pxf.automation.components.hdfs;

import com.google.protobuf.GeneratedMessage;
import jsystem.framework.system.SystemObject;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.greenplum.pxf.automation.fileformats.IAvroSchema;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

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
    List<String> list(String path) throws Exception;

    /**
     * Copy File or directory from local to remote File System
     *
     * @param srcPath
     * @param destPath
     * @throws Exception
     */
    void copyFromLocal(String srcPath, String destPath) throws Exception;

    /**
     * Copy File or directory from remote File System to local.
     *
     * @param srcPath
     * @param destPath
     * @throws Exception
     */
    void copyToLocal(String srcPath, String destPath) throws Exception;

    /**
     * Create new Directory
     *
     * @param path
     * @throws Exception
     */
    void createDirectory(String path) throws Exception;

    /**
     * Remove exists Directory
     *
     * @param path
     * @throws Exception
     */
    void removeDirectory(String path) throws Exception;

    /**
     * get File content as String
     *
     * @param path
     * @return file content as String
     * @throws Exception
     */
    String getFileContent(String path) throws Exception;

    /**
     * get Size of Directories and files in giver path
     *
     * @param path
     * @return Size of Directories and files in giver path
     * @throws Exception
     */
    int listSize(String path) throws Exception;

    /**
     * Write Data Table to file
     *
     * @param destPath
     * @param dataTable Table with data to write
     * @param delimiter put delimiter between columns
     * @throws Exception
     */
    void writeTableToFile(String destPath, Table dataTable,
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
    void writeTableToFile(String destPath, Table dataTable,
                          String delimiter, Charset encoding) throws Exception;

    /***
     * Write Data Table to file using different encodings and using different
     * codecs
     *
     * @param destPath destination file
     * @param dataTable {@link Table} with List of data
     * @param delimiter between fields
     * @param encoding to use to write the file
     * @param codec    to use for compression, or null to disable compression
     * @throws Exception
     */
    void writeTableToFile(String destPath, Table dataTable, String delimiter,
                          Charset encoding, CompressionCodec codec) throws Exception;

    /**
     * Write Data Table to file using different encodings and using different
     * codecs
     *
     * @param destPath  destination file
     * @param dataTable {@link Table} with List of data
     * @param delimiter between fields
     * @param encoding  to use to write the file
     * @param codec     to use for compression, or null to disable compression
     * @param newLine   CR, LF or CRLF
     */
    void writeTableToFile(String destPath, Table dataTable,
                          String delimiter, Charset encoding,
                          CompressionCodec codec, String newLine) throws Exception;

    /**
     * Write Dequence Object to file
     *
     * @param writableData
     * @param destPath
     * @throws IOException
     */
    void writeSequenceFile(Object[] writableData, String destPath)
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
    void writeAvroFile(String pathToFile, String schemaName,
                       String codecName, IAvroSchema[] data)
            throws Exception;

    /**
     * Generate and write Avro file to hdfs, from a schema file and a data file
     * in json format. The file is generated using avro-tools. If not null, the
     * file will be compressed using the given codecName. Supported types are
     * specified in {@code CodecFactory}.
     *
     * @param pathToFile   path to file in hdfs
     * @param schemaName   path to local schema file
     * @param jsonFileName path to local data file
     * @param codecName    codec name
     * @throws Exception
     */
    void writeAvroFileFromJson(String pathToFile, String schemaName,
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
    void writeAvroInSequenceFile(String pathToFile, String schemaName,
                                 IAvroSchema[] data) throws Exception;

    /**
     * @return Replication Size
     */
    short getReplicationSize();

    /**
     * Set Replication Size
     *
     * @param replicationSize
     */
    void setReplicationSize(short replicationSize);

    /**
     * @return Block Size
     */
    long getBlockSize();

    /**
     * Set Block size
     *
     * @param blockSize
     */
    void setBlockSize(long blockSize);

    /**
     * Set Default Replication size
     */
    void setDefaultReplicationSize();

    /**
     * Set Default Block size
     */
    void setDefaultBlockSize();

    /**
     * Write Protocol Buffered data stored in {@link GeneratedMessage}
     *
     * @param filePath
     * @param generatedMessages
     * @throws Exception
     */
    void writeProtocolBufferFile(String filePath,
                                 com.google.protobuf.GeneratedMessage generatedMessages)
            throws Exception;

    boolean doesFileExist(String pathToFile) throws Exception;
}
