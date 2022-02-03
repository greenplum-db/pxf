package org.greenplum.pxf.automation.components.hive;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.greenplum.pxf.automation.components.common.DbSystemObject;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;

/**
 * Hive System Object
 */
public class Hive extends DbSystemObject {

    private static final String DEFAULT_PORT = "10000";
    String saslQop;

    public Hive() {
        this.port = DEFAULT_PORT;
    }

    public Hive(boolean silenceReport) {
        super(silenceReport);
        this.port = DEFAULT_PORT;
    }

    @Override
    public void init() throws Exception {

        super.init();

        if (userName == null) {
            userName = System.getProperty("user.name");
        }

        driver = "org.apache.hive.jdbc.HiveDriver";

        // On kerberized cluster, enabled then we need the hive/hiveserver2_hostname principal in the connection string.
        // Assuming here that somewhere upstream ugi has a valid login context
        address = "jdbc:hive2://" + host + ":" + port + "/default";
        if (!StringUtils.isEmpty(kerberosPrincipal)) {
            address += ";principal=" + kerberosPrincipal.replace("HOSTNAME", host);
            if (StringUtils.isNotBlank(getSaslQop())) {
                address += String.format(";saslQop=%s", getSaslQop());
            }
        }

        connect();
    }

    /**
     * Loads data to Hive Table from file (local or HDFS)
     *
     * @param table
     * @param filePath
     * @param isLocal true - load data from local filePath, false - load from
     *            HDFS
     * @throws Exception
     */
    public void loadData(HiveTable table, String filePath, boolean isLocal)
            throws Exception {
        runQuery(createLoadDataStmt(table, filePath, isLocal));
    }

    /**
     * Creates string for load data query.
     *
     * @param table hive table to load to
     * @param filePath file path of data to load
     * @param isLocal true if the the data is local
     * @return query string
     */
    private String createLoadDataStmt(HiveTable table, String filePath,
                                      boolean isLocal) {
        String statement = "LOAD DATA " + ((isLocal) ? "LOCAL" : "")
                + " INPATH '" + ((isLocal) ? "" : "/") + filePath
                + "' INTO TABLE " + table.getName();
        return statement;
    }

    /**
     * Loads Data to Hive Table from local file
     *
     * @param table hive table
     * @param filePath
     * @throws Exception
     */
    public void loadData(HiveTable table, String filePath) throws Exception {
        loadData(table, filePath, true);
    }

    /**
     * Loads data to given partition in Hive table.
     *
     * @param table hive table
     * @param filePath data to load
     * @param isLocal true if data is local
     * @param partitions partition(s) to load to
     * @throws Exception
     */
    public void loadDataToPartition(HiveTable table, String filePath,
                                    boolean isLocal, String[] partitions)
            throws Exception {
        if (ArrayUtils.isEmpty(partitions)) {
            throw new IllegalArgumentException("No partitions to load data to");
        } else {
            String query = createLoadDataStmt(table, filePath, isLocal);
            query += " PARTITION(" + StringUtils.join(partitions, ", ") + ")";
            runQuery(query);
        }
    }

    public void alterTableAddPartition(HiveTable table, String[] partitions)
            throws Exception {
        runQuery("ALTER TABLE " + table.getName() + " ADD PARTITION ("
                + StringUtils.join(partitions, ",") + ")");
    }

    @Override
    public void dropTable(Table table, boolean cascade) throws Exception {
        // no DROP ... CASCADE in Hive
        runQuery(table.constructDropStmt(false));
    }

    @Override
    public void setHost(String host) {

        this.host = replaceUser(host);
    }

    @Override
    protected ResultSet getTablesForSchema(Table table) throws SQLException {
        String query = "SHOW TABLES";

        if (table != null) {
            query += " " + table.getName();
        }
        return stmt.executeQuery(query);
    }

    @Override
    public void insertData(Table source, Table target) throws Exception {

        runQuery("INSERT INTO TABLE " + target.getName() + " SELECT * FROM "
                + source.getName());
    }

    public void insertDataToPartition(Table source, Table target,
                                      String[] partitions, String[] columnsToSelect) throws Exception {
        insertDataToPartition(source, target, partitions, columnsToSelect, null);
    }

    public void insertDataToPartition(Table source, Table target,
                                      String[] partitions, String[] columnsToSelect, String filter) throws Exception {
        if (ArrayUtils.isEmpty(partitions)) {
            throw new IllegalArgumentException("No partitions to insert data to");
        } else {
            runQuery("INSERT INTO TABLE " + target.getName() + " PARTITION ("
                    + StringUtils.join(partitions, ", ") + ")"
                    + " SELECT " + StringUtils.join(columnsToSelect, ", ") + " FROM " + source.getName()
                    + (filter == null ? "" : " WHERE " + filter));
        }
    }

    /**
     * Verifies table exists
     */
    @Override
    public boolean checkTableExists(Table table) throws Exception {

        String query = "SHOW TABLES";
        if (table.getSchema() != null) {
            query += " IN " + table.getSchema();
        }
        query += " LIKE '" + table.getName() + "'";
        queryResults(table, query);

        // check if result has entry
        if (table.getData().size() > 0) {
            return true;
        }

        return false;
    }

    @Override
    public void createDataBase(String schemaName, boolean ignoreFail, String encoding, String localeCollate, String localeCollateType) {
        throw new UnsupportedOperationException();
    }

    public String getSaslQop() {
        return saslQop;
    }

    public void setSaslQop(String saslQop) {
        this.saslQop = saslQop;
    }
}
