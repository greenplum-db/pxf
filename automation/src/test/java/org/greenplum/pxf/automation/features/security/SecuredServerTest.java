package org.greenplum.pxf.automation.features.security;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

/**
 * SecuredServerTest verifies functionality when running queries against
 * a Kerberized Hadoop cluster via PXF.
 */
public class SecuredServerTest extends BaseFeature {

    @Test(groups = {"features", "multiClusterSecurity"})
    public void testSecuredServerFailsWithInvalidPrincipalName() throws Exception {

        exTable = TableFactory.getPxfReadableTextTable("pxf_secured_invalid_principal", new String[] {
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hdfs.getWorkingDirectory() + "/" + fileName, ",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setProfile("hdfs:text");
        exTable.setServer("SERVER=secure-hdfs-invalid-principal");

        gpdb.createTableAndVerify(exTable);
        runSqlTest("features/general/secured/errors/invalid_principal");
    }

    @Test(groups = {"features", "multiClusterSecurity"})
    public void testSecuredServerFailsWithInvalidKeytabPath() throws Exception {

        exTable = TableFactory.getPxfReadableTextTable("pxf_secured_invalid_keytab", new String[] {
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hdfs.getWorkingDirectory() + "/" + fileName, ",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setProfile("hdfs:text");
        exTable.setServer("SERVER=secure-hdfs-invalid-keytab");

        gpdb.createTableAndVerify(exTable);
        runSqlTest("features/general/secured/errors/invalid_keytab");
    }
}
