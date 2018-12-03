package org.greenplum.pxf.automation.testplugin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hive.HiveDataFragmenter;
import org.greenplum.pxf.plugins.hive.HiveUserData;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Properties;


/**
 * Fragmenter which splits one file into multiple fragments. Helps to simulate a
 * case of big files.
 *
 * inputData has to have following parameters:
 * TEST-FRAGMENTS-NUM - defines how many fragments will be returned for current file
 */
public class MultipleHiveFragmentsPerFileFragmenter extends BaseFragmenter {
    private static final Log LOG = LogFactory.getLog(MultipleHiveFragmentsPerFileFragmenter.class);

    private static final long SPLIT_SIZE = 1024;
    private JobConf jobConf;
    private HiveMetaStoreClient client;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        jobConf = new JobConf(configuration, MultipleHiveFragmentsPerFileFragmenter.class);
        client = HiveUtilities.initHiveClient(configuration);
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        String localhostname = java.net.InetAddress.getLocalHost().getHostName();
        String[] localHosts = new String[] { localhostname, localhostname };

        // TODO whitelist property
        int fragmentsNum = Integer.parseInt(context.getOption("TEST-FRAGMENTS-NUM"));
        Metadata.Item tblDesc = HiveUtilities.extractTableFromName(context.getDataSource());
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
