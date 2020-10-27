package org.greenplum.pxf.plugins.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClientCompatibility1xx;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hive.utilities.EnumHiveToGpdbType;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import static org.greenplum.pxf.api.model.ConfigurationFactory.PXF_CONFIG_RESOURCE_PATH_PROPERTY;

public class HiveClientWrapper {

    private static final HiveClientWrapper instance = new HiveClientWrapper();

    private static final Logger LOG = LoggerFactory.getLogger(HiveClientWrapper.class);

    private static final String WILDCARD = "*";
    private static final int DEFAULT_DELIMITER_CODE = 44;

    private static final String STR_RC_FILE_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
    private static final String STR_TEXT_FILE_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    private static final String STR_ORC_FILE_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    private final HiveClientFactory hiveClientFactory;

    private HiveClientWrapper() {
        this(HiveClientFactory.getInstance());
    }

    HiveClientWrapper(HiveClientFactory hiveClientFactory) {
        this.hiveClientFactory = hiveClientFactory;
    }

    /**
     * Returns the static instance for this factory
     *
     * @return the static instance for this factory
     */
    public static HiveClientWrapper getInstance() {
        return instance;
    }

    /**
     * Initializes the IMetaStoreClient
     * Uses classpath configuration files to locate the MetaStore
     *
     * @return initialized client
     */
    public IMetaStoreClient initHiveClient(RequestContext context, Configuration configuration) {
        HiveConf hiveConf = getHiveConf(configuration);
        try {
            if (Utilities.isSecurityEnabled(configuration)) {
                UserGroupInformation loginUser = SecureLogin.getInstance().getLoginUser(context, configuration);
                LOG.debug("initialize HiveMetaStoreClient as login user '{}'", loginUser.getUserName());
                // wrap in doAs for Kerberos to propagate kerberos tokens from login Subject
                return loginUser.
                        doAs((PrivilegedExceptionAction<IMetaStoreClient>) () -> hiveClientFactory.initHiveClient(hiveConf));
            } else {
                return hiveClientFactory.initHiveClient(hiveConf);
            }
        } catch (MetaException | InterruptedException | IOException e) {
            throw new RuntimeException("Failed connecting to Hive MetaStore service: " + e.getMessage(), e);
        }
    }

    public Table getHiveTable(IMetaStoreClient client, Metadata.Item itemName) throws Exception {
        Table tbl = client.getTable(itemName.getPath(), itemName.getName());
        String tblType = tbl.getTableType();

        LOG.debug("Item: {}.{}, type: {}", itemName.getPath(), itemName.getName(), tblType);

        if (TableType.valueOf(tblType) == TableType.VIRTUAL_VIEW) {
            throw new UnsupportedOperationException("Hive views are not supported by PXF");
        }

        return tbl;
    }

    /**
     * Populates the given metadata object with the given table's fields and partitions,
     * The partition fields are added at the end of the table schema.
     * Throws an exception if the table contains unsupported field types.
     * Supported HCatalog types: TINYINT,
     * SMALLINT, INT, BIGINT, BOOLEAN, FLOAT, DOUBLE, STRING, BINARY, TIMESTAMP,
     * DATE, DECIMAL, VARCHAR, CHAR.
     *
     * @param tbl      Hive table
     * @param metadata schema of given table
     */
    public void getSchema(Table tbl, Metadata metadata) {

        int hiveColumnsSize = tbl.getSd().getColsSize();
        int hivePartitionsSize = tbl.getPartitionKeysSize();

        LOG.debug("Hive table: {} fields. {} partitions.", hiveColumnsSize, hivePartitionsSize);

        // check hive fields
        try {
            List<FieldSchema> hiveColumns = tbl.getSd().getCols();
            for (FieldSchema hiveCol : hiveColumns) {
                metadata.addField(HiveUtilities.mapHiveType(hiveCol));
            }
            // check partition fields
            List<FieldSchema> hivePartitions = tbl.getPartitionKeys();
            for (FieldSchema hivePart : hivePartitions) {
                metadata.addField(HiveUtilities.mapHiveType(hivePart));
            }
        } catch (UnsupportedTypeException e) {
            String errorMsg = "Failed to retrieve metadata for table " + metadata.getItem() + ". " +
                    e.getMessage();
            throw new UnsupportedTypeException(errorMsg);
        }
    }

    /**
     * The method which serializes fragment-related attributes, needed for reading and resolution to string
     *
     * @param fragmenterClassName fragmenter class name
     * @param partData            partition data
     * @param hiveIndexes         the list of indices that we will retrieve from the Hive schema columns
     * @param allColumnNames      the comma-separated list of column names defined in hive table
     * @param allColumnTypes      the comma-separated list of column types defined in hive table
     * @return serialized representation of fragment-related attributes
     * @throws Exception when error occurred during serialization
     */
    public byte[] makeUserData(String fragmenterClassName,
                               HiveTablePartition partData,
                               List<Integer> hiveIndexes,
                               String allColumnNames,
                               String allColumnTypes) throws Exception {

        HiveUserData hiveUserData;

        if (fragmenterClassName == null) {
            throw new IllegalArgumentException("No fragmenter provided.");
        }

        Class<?> fragmenterClass = Class.forName(fragmenterClassName);

        String inputFormatName = partData.storageDesc.getInputFormat();
        String serdeClassName = partData.storageDesc.getSerdeInfo().getSerializationLib();
        String propertiesString = serializeProperties(partData.properties);
        String partitionKeys = serializePartitionKeys(partData);
        String delimiter = getDelimiterCode(partData.storageDesc).toString();
        String colTypes = partData.properties.getProperty("columns.types");
        int skipHeader = Integer.parseInt(partData.properties.getProperty("skip.header.line.count", "0"));

        if (HiveInputFormatFragmenter.class.isAssignableFrom(fragmenterClass)) {
            assertFileType(inputFormatName, partData);
        }

        hiveUserData = new HiveUserData(inputFormatName, serdeClassName, propertiesString, partitionKeys, delimiter, colTypes, skipHeader, hiveIndexes, allColumnNames, allColumnTypes);

        return hiveUserData.toString().getBytes();
    }

    /**
     * Extracts the db_name and table_name from the qualifiedName.
     * qualifiedName is the Hive table name that the user enters in the CREATE EXTERNAL TABLE statement
     * or when querying HCatalog table.
     * It can be either <code>table_name</code> or <code>db_name.table_name</code>.
     *
     * @param qualifiedName Hive table name
     * @return {@link Metadata.Item} object holding the full table name
     */
    public Metadata.Item extractTableFromName(String qualifiedName) {
        List<Metadata.Item> items = extractTablesFromPattern(null, qualifiedName);
        if (items.isEmpty()) {
            throw new IllegalArgumentException("No tables found");
        }
        return items.get(0);
    }

    /**
     * The method determines whether metadata definition has any complex type
     *
     * @param metadata metadata of relation
     * @return true if metadata has at least one field of complex type
     * @see EnumHiveToGpdbType for complex type attribute definition
     */
    public boolean hasComplexTypes(Metadata metadata) {
        boolean hasComplexTypes = false;
        List<Metadata.Field> fields = metadata.getFields();
        for (Metadata.Field field : fields) {
            if (field.isComplexType()) {
                hasComplexTypes = true;
                break;
            }
        }

        return hasComplexTypes;
    }

    /**
     * Extracts the db_name(s) and table_name(s) corresponding to the given pattern.
     * pattern is the Hive table name or pattern that the user enters in the CREATE EXTERNAL TABLE statement
     * or when querying HCatalog table.
     * It can be either <code>table_name_pattern</code> or <code>db_name_pattern.table_name_pattern</code>.
     *
     * @param client  MetaStoreClient client
     * @param pattern Hive table name or pattern
     * @return list of {@link Metadata.Item} objects holding the full table name
     */
    public List<Metadata.Item> extractTablesFromPattern(IMetaStoreClient client, String pattern) {

        String dbPattern, tablePattern;
        String errorMsg = " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";

        if (StringUtils.isBlank(pattern)) {
            throw new IllegalArgumentException("empty string" + errorMsg);
        }

        String[] rawTokens = pattern.split("[.]");
        ArrayList<String> tokens = new ArrayList<>();
        for (String tok : rawTokens) {
            if (StringUtils.isBlank(tok)) {
                continue;
            }
            tokens.add(tok.trim());
        }

        if (tokens.size() == 1) {
            dbPattern = MetaStoreUtils.DEFAULT_DATABASE_NAME;
            tablePattern = tokens.get(0);
        } else if (tokens.size() == 2) {
            dbPattern = tokens.get(0);
            tablePattern = tokens.get(1);
        } else {
            throw new IllegalArgumentException("\"" + pattern + "\"" + errorMsg);
        }

        return getTablesFromPattern(client, dbPattern, tablePattern);
    }

    private List<Metadata.Item> getTablesFromPattern(IMetaStoreClient client, String dbPattern, String tablePattern) {

        List<String> databases;
        List<Metadata.Item> itemList = new ArrayList<>();

        if (client == null || (!dbPattern.contains(WILDCARD) && !tablePattern.contains(WILDCARD))) {
            /* This case occurs when the call is invoked as part of the fragmenter api or when metadata is requested for a specific table name */
            itemList.add(new Metadata.Item(dbPattern, tablePattern));
            return itemList;
        }

        try {
            databases = client.getDatabases(dbPattern);
            if (databases.isEmpty()) {
                LOG.warn("No database found for the given pattern: " + dbPattern);
                return null;
            }
            for (String dbName : databases) {
                for (String tableName : client.getTables(dbName, tablePattern)) {
                    itemList.add(new Metadata.Item(dbName, tableName));
                }
            }
            return itemList;

        } catch (TException cause) {
            throw new RuntimeException("Failed connecting to Hive MetaStore service: " + cause.getMessage(), cause);
        }
    }

    /**
     * Initializes HiveConf configuration object from request configuration. Since hive-site.xml
     * is not available on classpath due to multi-server support, it is added explicitly based
     * on location for a given PXF configuration server
     *
     * @param configuration request configuration
     * @return instance of HiveConf object
     */
    private HiveConf getHiveConf(Configuration configuration) {
        // prepare hiveConf object and explicitly add this request's hive-site.xml file to it
        HiveConf hiveConf = new HiveConf(configuration, HiveConf.class);

        String hiveSiteUrl = configuration.get(String.format("%s.%s", PXF_CONFIG_RESOURCE_PATH_PROPERTY, "hive-site.xml"));
        if (hiveSiteUrl != null) {
            try {
                hiveConf.addResource(new URL(hiveSiteUrl));
            } catch (MalformedURLException e) {
                throw new RuntimeException(String.format("Failed to add %s to hive configuration", hiveSiteUrl), e);
            }
        }
        return hiveConf;
    }

    /* Turns a Properties class into a string */
    private String serializeProperties(Properties props) throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        props.store(outStream, ""/* comments */);
        return outStream.toString();
    }

    /* Turns the partition keys into a string */
    private String serializePartitionKeys(HiveTablePartition partData) {
        if (partData.partition == null) {
            /* this is a simple hive table - there are no partitions */
            return HiveDataFragmenter.HIVE_NO_PART_TBL;
        }

        StringBuilder partitionKeys = new StringBuilder();
        String prefix = "";
        ListIterator<String> valsIter = partData.partition.getValues().listIterator();
        ListIterator<FieldSchema> keysIter = partData.partitionKeys.listIterator();
        while (valsIter.hasNext() && keysIter.hasNext()) {
            FieldSchema key = keysIter.next();
            String name = key.getName();
            String type = key.getType();
            String val = valsIter.next();
            String oneLevel = prefix + name + HiveDataFragmenter.HIVE_1_PART_DELIM + type
                    + HiveDataFragmenter.HIVE_1_PART_DELIM + val;
            partitionKeys.append(oneLevel);
            prefix = HiveDataFragmenter.HIVE_PARTITIONS_DELIM;
        }

        return partitionKeys.toString();
    }

    /**
     * The method which extracts field delimiter from storage descriptor.
     * When unable to extract delimiter from storage descriptor, default value is used
     *
     * @param sd StorageDescriptor of table/partition
     * @return ASCII code of delimiter
     */
    public Integer getDelimiterCode(StorageDescriptor sd) {
        Integer delimiterCode;

        String delimiter = getSerdeParameter(sd, serdeConstants.FIELD_DELIM);
        if (delimiter != null) {
            delimiterCode = (int) delimiter.charAt(0);
            return delimiterCode;
        }

        delimiter = getSerdeParameter(sd, serdeConstants.SERIALIZATION_FORMAT);
        if (delimiter != null) {
            delimiterCode = Integer.parseInt(delimiter);
            return delimiterCode;
        }

        return DEFAULT_DELIMITER_CODE;
    }

    private String getSerdeParameter(StorageDescriptor sd, String parameterKey) {
        String parameterValue = null;
        if (sd != null && sd.getSerdeInfo() != null && sd.getSerdeInfo().getParameters() != null && sd.getSerdeInfo().getParameters().get(parameterKey) != null) {
            parameterValue = sd.getSerdeInfo().getParameters().get(parameterKey);
        }

        return parameterValue;
    }

    /*
     * Validates that partition format corresponds to PXF supported formats and
     * transforms the class name to an enumeration for writing it to the
     * accessors on other PXF instances.
     */
    private String assertFileType(String className, HiveTablePartition partData) {
        switch (className) {
            case STR_RC_FILE_INPUT_FORMAT:
                return HiveInputFormatFragmenter.PXF_HIVE_INPUT_FORMATS.RC_FILE_INPUT_FORMAT.name();
            case STR_TEXT_FILE_INPUT_FORMAT:
                return HiveInputFormatFragmenter.PXF_HIVE_INPUT_FORMATS.TEXT_FILE_INPUT_FORMAT.name();
            case STR_ORC_FILE_INPUT_FORMAT:
                return HiveInputFormatFragmenter.PXF_HIVE_INPUT_FORMATS.ORC_FILE_INPUT_FORMAT.name();
            default:
                throw new IllegalArgumentException(
                        "HiveInputFormatFragmenter does not yet support "
                                + className
                                + " for "
                                + partData
                                + ". Supported InputFormat are "
                                + Arrays.toString(HiveInputFormatFragmenter.PXF_HIVE_INPUT_FORMATS.values()));
        }
    }

    public static class HiveClientFactory {
        private static final HiveClientFactory instance = new HiveClientFactory();

        /**
         * Returns the static instance for this factory
         *
         * @return the static instance for this factory
         */
        static HiveClientFactory getInstance() {
            return instance;
        }

        IMetaStoreClient initHiveClient(HiveConf hiveConf) throws MetaException {
            try {
                return RetryingMetaStoreClient.getProxy(hiveConf, new Class[]{HiveConf.class, HiveMetaHookLoader.class, Boolean.class},
                        new Object[]{hiveConf, null, true}, null, HiveMetaStoreClientCompatibility1xx.class.getName()
                );
            } catch (RuntimeException ex) {
                // Report MetaException if found in the stack. A RuntimeException
                // was thrown when the HiveMetaStoreClientCompatibility1xx
                // failed to instantiate with a MetaException cause.
                // java.lang.RuntimeException: Unable to instantiate org.apache.hadoop.hive.metastore.HiveMetaStoreClientCompatibility1xx
                // and it reports an error message that is hard to interpret
                // by the user/admin
                Throwable e = ex;
                while (e.getCause() != null) {
                    if (e.getCause() instanceof MetaException) {
                        LOG.warn("Original exception not re-thrown", ex);
                        throw (MetaException) e.getCause();
                    }
                    e = e.getCause();
                }
                throw ex;
            }
        }
    }
}
