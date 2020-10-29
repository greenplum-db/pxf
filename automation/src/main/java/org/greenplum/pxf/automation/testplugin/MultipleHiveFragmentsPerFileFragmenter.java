package org.greenplum.pxf.automation.testplugin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hive.HiveClientWrapper;
import org.greenplum.pxf.plugins.hive.HiveDataFragmenter;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Properties;


/**
 * Fragmenter which splits one file into multiple fragments. Helps to simulate a
 * case of big files.
 * <p>
 * inputData has to have following parameters:
 * TEST-FRAGMENTS-NUM - defines how many fragments will be returned for current file
 */
public class MultipleHiveFragmentsPerFileFragmenter extends BaseFragmenter {
    private static final Log LOG = LogFactory.getLog(MultipleHiveFragmentsPerFileFragmenter.class);

    private static final ThreadLocal<Kryo> kryo = ThreadLocal.withInitial(Kryo::new);
    private static final long SPLIT_SIZE = 1024;
    private JobConf jobConf;
    private IMetaStoreClient client;
    private final HiveClientWrapper hiveClientWrapper;

    public MultipleHiveFragmentsPerFileFragmenter() {
        hiveClientWrapper = HiveClientWrapper.getInstance();
    }

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
        jobConf = new JobConf(configuration, MultipleHiveFragmentsPerFileFragmenter.class);
        client = hiveClientWrapper.initHiveClient(context, configuration);
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        String localhostname = java.net.InetAddress.getLocalHost().getHostName();
        String[] localHosts = new String[]{localhostname, localhostname};

        // TODO whitelist property
        int fragmentsNum = Integer.parseInt(context.getOption("TEST-FRAGMENTS-NUM"));
        Metadata.Item tblDesc = hiveClientWrapper.extractTableFromName(context.getDataSource());
        Table tbl = hiveClientWrapper.getHiveTable(client, tblDesc);
        Properties properties = getSchema(tbl);

        for (int i = 0; i < fragmentsNum; i++) {

            byte[] userData = serializeProperties(properties);

            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bas);
            os.writeLong(i * SPLIT_SIZE); // start
            os.writeLong(SPLIT_SIZE); // length
            os.writeObject(localHosts); // hosts
            os.close();

            String filePath = getFilePath(tbl);

            fragments.add(new Fragment(filePath, localHosts, bas.toByteArray(), userData));
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

        InputSplit[] splits;
        try {
            splits = fformat.getSplits(jobConf, 1);
        } catch (org.apache.hadoop.mapred.InvalidInputException e) {
            LOG.debug("getSplits failed on " + e.getMessage());
            throw new RuntimeException("Unable to get file path for table.");
        }

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            return fsp.getPath().toString();
        }
        throw new RuntimeException("Unable to get file path for table.");
    }

    /* Turns a Properties class into a string */
    private byte[] serializeProperties(Properties properties) {
        Output out = new Output(4 * 1024, 10 * 1024 * 1024);
        kryo.get().writeObject(out, properties);
        out.close();
        return out.toBytes();
    }
}
