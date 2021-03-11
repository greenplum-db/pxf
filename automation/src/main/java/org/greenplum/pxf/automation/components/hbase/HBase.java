package org.greenplum.pxf.automation.components.hbase;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import jsystem.framework.report.Reporter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.BlockingRpcChannel;
import org.greenplum.pxf.automation.components.common.BaseSystemObject;
import org.greenplum.pxf.automation.components.common.IDbFunctionality;
import org.greenplum.pxf.automation.structures.tables.hbase.HBaseTable;
import org.greenplum.pxf.automation.utils.hbase.HBaseUtils;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * HBase system object
 */
public class HBase extends BaseSystemObject implements IDbFunctionality {

    private Configuration config;
    private Admin admin;
    private Connection connection;
    private String host;
    private String hbaseRoot;
    private boolean isAuthorizationEnabled = false;

    public HBase() {

    }

    public HBase(boolean silentReport) {
        super(silentReport);
    }

    @Override
    public void init() throws Exception {

        super.init();

        ReportUtils.startLevel(report, getClass(), "Init");

        config = new Configuration();

        // if hbaseRoot root exists in the SUT file, load configuration from it
        if (StringUtils.isNotEmpty(hbaseRoot)) {
            config.addResource(new Path(getHbaseRoot() + "/conf/hbase-site.xml"));
        } else {
            config.set("hbase.rootdir", "hdfs://" + host + ":8020/hbase");
        }

        HBaseAdmin.checkHBaseAvailable(config);
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        if (admin.getClusterStatus().getServersSize() == 0) {
            ReportUtils.report(report, getClass(),
                    "No HBase region servers running", Reporter.FAIL);
        }

        ReportUtils.report(report, getClass(), "HBase Admin created");

        ReportUtils.stopLevel(report);
    }

    @Override
    public void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        super.close();
    }

    @Override
    public ArrayList<String> getTableList(String schema) throws Exception {

        ReportUtils.startLevel(report, getClass(), "List Tables");

        HTableDescriptor[] tables = admin.listTables();

        ArrayList<String> tablesNames = new ArrayList<String>();

        for (int i = 0; i < tables.length; i++) {

            tablesNames.add(tables[i].getNameAsString());

        }

        ReportUtils.report(report, getClass(), tablesNames.toString());

        ReportUtils.stopLevel(report);

        return tablesNames;
    }

    /**
     * Creates Data in HBase table from rowsToGenerate() in the hbaseTable
     * object.
     *
     * @param hbaseTable
     * @throws Exception
     */
    public void put(HBaseTable hbaseTable) throws Exception {

        ReportUtils.startLevel(report, getClass(), "Put data to Table: "
                + hbaseTable.getName());
        org.apache.hadoop.hbase.client.Table table = connection.getTable(TableName.valueOf(hbaseTable.getName()));
        table.put(hbaseTable.getRowsToGenerate());
        table.close();

        ReportUtils.stopLevel(report);
    }

    public void removeRow(HBaseTable table, String[] rowIds) throws Exception {

        List<Delete> deleteList = new ArrayList<Delete>();
        StringBuilder sBuilder = new StringBuilder();

        for (int i = 0; i < rowIds.length; i++) {
            Delete delete = new Delete(rowIds[i].getBytes());
            deleteList.add(delete);
            sBuilder.append(rowIds[i]);
            sBuilder.append(" ");
        }

        ReportUtils.startLevel(
                report,
                getClass(),
                "Remove " + sBuilder.toString() + " rowIds from "
                        + table.getName());
        org.apache.hadoop.hbase.client.Table hTable = connection.getTable(TableName.valueOf(table.getName()));
        hTable.delete(deleteList);
        hTable.close();

        ReportUtils.stopLevel(report);
    }

    @Override
    public void queryResults(Table table,
                             String query) throws Exception {

        ReportUtils.startLevel(report, getClass(),
                "Scan Table: " + table.getName());

        org.apache.hadoop.hbase.client.Table tbl = connection.getTable(TableName.valueOf(table.getName()));

        Scan scan = new Scan();

        HBaseTable hTable = (HBaseTable) table;

        if (hTable.getFilters() != null) {

            scan.setFilter(hTable.getFilters());

            StringBuilder filterListPrint = new StringBuilder();
            HBaseUtils.getFilterListPrint(filterListPrint, hTable.getFilters());
            ReportUtils.report(report, getClass(), filterListPrint.toString());
        }

        hTable.initDataStructures();

        if (hTable.getQualifiers() != null) {
            for (int i = 0; i < hTable.getQualifiers().length; i++) {

                String[] splitResults = hTable.getQualifiers()[i].split(":");
                hTable.addColumnHeader(hTable.getQualifiers()[i]);

                String family = splitResults[0];
                String qualifier = splitResults[1];

                scan.addColumn(family.getBytes(), qualifier.getBytes());
            }
        }

        ResultScanner rs = tbl.getScanner(scan);

        List<List<String>> data = new ArrayList<List<String>>();

        /**
         * go over rows
         */
        for (Result result : rs) {

            List<String> row = new ArrayList<String>();
            row.add(new String(result.getRow()));

            if (hTable.getQualifiers() != null) {
                for (int i = 0; i < hTable.getQualifiers().length; i++) {
                    String[] splitResults = hTable.getQualifiers()[i].split(":");

                    String family = splitResults[0];
                    String qualifier = splitResults[1];

                    Cell cell = result.getColumnLatestCell(family.getBytes(),
                            qualifier.getBytes());

                    if (cell != null) {
                        row.add(Bytes.toString(CellUtil.cloneValue(cell)));
                    } else {
                        row.add(Bytes.toString(new byte[] {}));
                    }
                }
            } else {
                List<Cell> cells = result.listCells();

                for (Cell cell : cells) {
                    if (cell != null) {
                        row.add(Bytes.toString(CellUtil.cloneValue(cell)));
                    } else {
                        row.add(Bytes.toString(new byte[] {}));
                    }
                }
            }

            data.add(row);
        }

        rs.close();
        tbl.close();

        table.setData(data);

        ReportUtils.reportHtml(report, getClass(), table.getDataHtml());

        ReportUtils.stopLevel(report);
    }

    /**
     * Loads Bulk of data using ImportTsv.
     *
     * @param table to load to
     * @param inputPath
     * @param cols to which columns
     * @throws Exception
     */
    public void loadBulk(Table table,
                         String inputPath, String... cols) throws Exception {

        ReportUtils.startLevel(report, getClass(), "Load Bulk from "
                + inputPath + " to Table: " + table.getName());

        ArrayList<String> argsList = new ArrayList<String>();

        StringBuilder sb = new StringBuilder();

        sb.append("-Dimporttsv.columns=HBASE_ROW_KEY,");

        for (int i = 0; i < cols.length; i++) {
            sb.append(cols[i]);
            if (i != cols.length - 1) {
                sb.append(",");
            }
        }

        argsList.add(sb.toString());
        argsList.add(table.getName());
        argsList.add("/" + inputPath);

        String[] args = new String[argsList.size()];

        for (int i = 0; i < argsList.size(); i++) {
            args[i] = argsList.get(i);
        }

        try {
            /**
             * ImportTsv.main performing exit 0 when done. In order to prevent
             * it, I have here a hack that catches the exit and preventing it.
             */
            forbidSystemExitCall();
            ImportTsv.main(args);
        } catch (Exception e) {

            /**
             * When this ExitTrappedException thrown , the exit 0 performed.
             */
            if (e instanceof ExitTrappedException) {
                System.out.println("Prevent Exit VM 0");
            }
        }

        ReportUtils.stopLevel(report);
    }

    private static class ExitTrappedException extends SecurityException {

        private static final long serialVersionUID = 1L;
    }

    private static void forbidSystemExitCall() {
        final SecurityManager securityManager = new SecurityManager() {
            @Override
            public void checkPermission(Permission permission) {

                if ("exitVM.0".equals(permission.getName())) {
                    throw new ExitTrappedException();
                }
            }
        };
        System.setSecurityManager(securityManager);
    }

    private static void enableSystemExitCall() {
        System.setSecurityManager(null);
    }

    @Override
    public void createTable(Table table)
            throws Exception {

        HBaseTable hTable = (HBaseTable) table;

        ReportUtils.startLevel(report, getClass(),
                "Create Table " + table.getName());

        HTableDescriptor htd = new HTableDescriptor(
                TableName.valueOf(table.getName()));

        for (int i = 0; i < hTable.getFields().length; i++) {

            HColumnDescriptor hcd = new HColumnDescriptor(hTable.getFields()[i]);

            htd.addFamily(hcd);
        }

        String[] splits = generateSplits(hTable.getNumberOfSplits(),
                hTable.getRowKeyPrefix(), hTable.getRowsPerSplit());

        admin.createTable(htd, Bytes.toByteArrays(splits));
        ReportUtils.stopLevel(report);
    }

    @Override
    public void dropTable(Table table,
                          boolean cascade) throws Exception {

        ReportUtils.startLevel(report, getClass(),
                "Remove Table: " + table.getName());

        if (checkTableExists(table)) {
            disableTable(table);
            admin.deleteTable(TableName.valueOf(table.getName()));
        }

        ReportUtils.stopLevel(report);
    }

    public void disableTable(Table table)
            throws Exception {
        ReportUtils.startLevel(report, getClass(),
                "Disable Table: " + table.getName());

        if (!admin.isTableDisabled(TableName.valueOf(table.getName()))) {
            admin.disableTable(TableName.valueOf(table.getName()));
        }
        ReportUtils.stopLevel(report);
    }

    public void enableTable(Table table)
            throws Exception {
        ReportUtils.startLevel(report, getClass(),
                "Enable Table: " + table.getName());
        admin.enableTable(TableName.valueOf(table.getName()));
        ReportUtils.stopLevel(report);
    }

    public void removeColumn(Table table,
                             String[] columns) throws Exception {

        ReportUtils.startLevel(report, getClass(), "Remove " + columns.length
                + "columns from Table: " + table.getName());

        disableTable(table);

        for (int i = 0; i < columns.length; i++) {
            admin.deleteColumn(TableName.valueOf(table.getName()),
                    Bytes.toBytes(columns[i]));
        }

        enableTable(table);

        ReportUtils.stopLevel(report);
    }

    public void addColumn(Table table,
                          String[] columns) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Add " + columns.length
                + "columns from Table: " + table.getName());

        disableTable(table);

        for (int i = 0; i < columns.length; i++) {
            HColumnDescriptor column = new HColumnDescriptor(
                    Bytes.toBytes(columns[i]));
            admin.addColumn(TableName.valueOf(table.getName()), column);
        }

        enableTable(table);

        ReportUtils.stopLevel(report);
    }

    @Override
    public void dropDataBase(String schemaName, boolean cascade,
                             boolean ignoreFail) throws Exception {

        ReportUtils.throwUnsupportedFunctionality(getClass(), "Drop Schema");

    }

    @Override
    public void insertData(Table source,
                           Table target)
            throws Exception {

        ReportUtils.throwUnsupportedFunctionality(getClass(), "Insert Data");

    }

    @Override
    public void createDataBase(String schemaName, boolean ignoreFail)
            throws Exception {

        ReportUtils.throwUnsupportedFunctionality(getClass(), "Create Schema");
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public void createTableAndVerify(Table table)
            throws Exception {

        ReportUtils.startLevel(report, getClass(), "Create and Verify Table: "
                + table.getFullName());

        dropTable(table, false);

        createTable(table);

        if (!checkTableExists(table)) {
            ReportUtils.stopLevel(report);
            throw new Exception("Table " + table.getName() + " do not exists");
        }

        ReportUtils.stopLevel(report);
    }

    @Override
    public boolean checkTableExists(Table table)
            throws Exception {
        return admin.isTableAvailable(TableName.valueOf(table.getName()));
    }

    @Override
    public boolean checkDataBaseExists(String dbName) throws Exception {
        ReportUtils.throwUnsupportedFunctionality(getClass(),
                "Check Data Base Exists");
        return false;
    }

    @Override
    public ArrayList<String> getDataBasesList() throws Exception {
        ReportUtils.throwUnsupportedFunctionality(getClass(),
                "Get Data Bases List");
        return null;
    }

    @Override
    public void grantReadOnTable(Table table,
                                 String user) throws Exception {
        this.grantPermissions(table, user, Action.READ);
    }

    @Override
    public void grantWriteOnTable(Table table,
                                  String user) throws Exception {
        this.grantPermissions(table, user, Action.WRITE);
    }

    @Override
    public void createDataBase(String schemaName, boolean ignoreFail, String encoding, String localeCollate, String localeCollateType) {
        throw new UnsupportedOperationException();
    }

    public void grantCreateReadOnTable(Table table,
                                       String user) throws Exception {
        // each subsequent grant call overrides previous grants, so use this one to grant both permissions at once
        this.grantPermissions(table, user, Action.CREATE, Action.READ);
    }

    public static void main(String[] args) throws Exception {
        HBase hbase = new HBase();
        hbase.setHost("localhost");
        hbase.init();
    }

    public String getHbaseRoot() {
        return hbaseRoot;
    }

    public void grantGlobalForUser(String user) throws Exception {
        this.grantPermissions(null, user, Action.CREATE, Action.READ,
                Action.WRITE, Action.ADMIN);
    }

    public void setHbaseRoot(String hbaseRoot) {
        this.hbaseRoot = hbaseRoot;
    }

    public void setAuthorization(boolean authEnabled) {
        this.isAuthorizationEnabled = authEnabled;
    }

    private String[] generateSplits(int numberOfSplits, String rowKeyPrefix,
                                    int rowsPerSplit) {
        String[] splits = new String[numberOfSplits];

        for (int i = 0; i < numberOfSplits; ++i)
            splits[i] = String.format("%s%08d", rowKeyPrefix, (i + 1)
                    * rowsPerSplit);

        return splits;
    }

    private void grantPermissions(Table table,
                                  String user, Action... actions)
            throws Exception {

        ReportUtils.report(report, getClass(), config.toString());
        ReportUtils.report(report, getClass(),"grant request for user=" + user + " table" + table);
        String hbaseAuthEnabled = config.get("hbase.security.authorization");
        if (!isAuthorizationEnabled && (hbaseAuthEnabled == null || !hbaseAuthEnabled.equals("true"))) {
            ReportUtils.report(report, getClass(),
                    "HBase security authorization is not enabled, cannot grant permissions");
            return;
        }

        org.apache.hadoop.hbase.client.Table acl = connection.getTable(AccessControlLists.ACL_TABLE_NAME);
        try {
            BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
            AccessControlProtos.AccessControlService.BlockingInterface protocol = AccessControlProtos.AccessControlService.newBlockingStub(service);
            if (table == null) {
                ProtobufUtil.grant(protocol, user, actions);
            } else {
                ProtobufUtil.grant(protocol, user, TableName.valueOf(table.getName()), null, null, actions);
            }
        } finally {
            acl.close();
        }
    }

}
