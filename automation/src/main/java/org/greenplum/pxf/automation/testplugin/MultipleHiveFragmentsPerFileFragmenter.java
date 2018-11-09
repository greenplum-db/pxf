package org.greenplum.pxf.automation.testplugin;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Properties;

import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.api.Fragmenter;
import org.greenplum.pxf.api.Fragment;
import org.greenplum.pxf.api.Metadata;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.plugins.hive.HiveUserData;
import org.greenplum.pxf.plugins.hive.HiveDataFragmenter;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.greenplum.pxf.plugins.hive.utilities.ProfileFactory;


/**
 * Fragmenter which splits one file into multiple fragments. Helps to simulate a
 * case of big files.
 * 
 * inputData has to have following parameters:
 * TEST-FRAGMENTS-NUM - defines how many fragments will be returned for current file
 */
public class MultipleHiveFragmentsPerFileFragmenter extends Fragmenter {
    private static final Log LOG = LogFactory.getLog(MultipleHiveFragmentsPerFileFragmenter.class);

    private static final long SPLIT_SIZE = 1024;
    private JobConf jobConf;
    private HiveMetaStoreClient client;

    public MultipleHiveFragmentsPerFileFragmenter(InputData inputData) {
        this(inputData, MultipleHiveFragmentsPerFileFragmenter.class);
    }

    public MultipleHiveFragmentsPerFileFragmenter(InputData metaData, Class<?> clazz) {
        super(metaData);
        jobConf = new JobConf(inputData.getConfiguration(), clazz);
        client = HiveUtilities.initHiveClient();
    }


    @Override
    public List<Fragment> getFragments() throws Exception {
        String localhostname = java.net.InetAddress.getLocalHost().getHostName();
        String[] localHosts = new String[] { localhostname, localhostname };

        int fragmentsNum = Integer.parseInt(inputData.getUserProperty("TEST-FRAGMENTS-NUM"));
        Metadata.Item tblDesc = HiveUtilities.extractTableFromName(inputData.getDataSource());
        Table tbl = HiveUtilities.getHiveTable(client, tblDesc);
        Properties properties = getSchema(tbl);

        for (int i = 0; i < fragmentsNum; i++) {

            String userData = "inputFormatName" + HiveUserData.HIVE_UD_DELIM
                    + tbl.getSd().getSerdeInfo().getSerializationLib()
                    + HiveUserData.HIVE_UD_DELIM + "propertiesString"
                    + HiveUserData.HIVE_UD_DELIM + HiveDataFragmenter.HIVE_NO_PART_TBL
                    + HiveUserData.HIVE_UD_DELIM + "filterInFragmenter"
                    + HiveUserData.HIVE_UD_DELIM + "delimiter"
                    + HiveUserData.HIVE_UD_DELIM + properties.getProperty("columns.types");

            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bas);
            os.writeLong(i * SPLIT_SIZE); // start
            os.writeLong(SPLIT_SIZE); // length
            os.writeObject(localHosts); // hosts
            os.close();

            String filePath = getFilePath(tbl);

            fragments.add(new Fragment(filePath, localHosts, bas.toByteArray(), userData.getBytes()));
        }

        return fragments;
    }


    private static Properties getSchema(Table table) {
        return MetaStoreUtils.getSchema(table.getSd(), table.getSd(),
                table.getParameters(), table.getDbName(), table.getTableName(),
                table.getPartitionKeys());
    }

    private String getFilePath(Table tbl) throws Exception {

        StorageDescriptor descTable = tbl.getSd();

        InputFormat<?, ?> fformat = HiveDataFragmenter.makeInputFormat(descTable.getInputFormat(), jobConf);

        FileInputFormat.setInputPaths(jobConf, new Path(descTable.getLocation()));

        InputSplit[] splits = null;
        try {
            splits = fformat.getSplits(jobConf, 1);
        } catch (org.apache.hadoop.mapred.InvalidInputException e) {
            LOG.debug("getSplits failed on " + e.getMessage());
            throw new RuntimeException("Unable to get file path for table.");
        }

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String[] hosts = fsp.getLocations();
            String filepath = fsp.getPath().toUri().getPath();
            return filepath;
        }
        throw new RuntimeException("Unable to get file path for table.");
    }
}
