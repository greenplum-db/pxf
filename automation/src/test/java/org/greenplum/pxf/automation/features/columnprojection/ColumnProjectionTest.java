package org.greenplum.pxf.automation.features.columnprojection;

import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.annotations.Test;

import java.io.File;

/** Functional PXF column projection cases */
@WorksWithFDW
public class ColumnProjectionTest extends BaseFeature {

    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String testPackage = "org.greenplum.pxf.automation.testplugin.";

    @Override
    protected void beforeClass() throws Exception {
        String newPath = "/tmp/publicstage/pxf";
        // copy additional plugins classes to cluster nodes, used for filter pushdown cases
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "ColumnProjectionVerifyFragmentMetadata.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "ColumnProjectionVerifyFragmenter.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "ColumnProjectionVerifyAccessor.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    /**
     * Check PXF receive the expected column projection string from GPDB.
     * Column delimiter is ",".
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void checkColumnProjection() throws Exception {

        // Create PXF external table for column projection testing
        ReadableExternalTable pxfExternalTable = TableFactory.getPxfReadableTestTextTable("test_column_projection", new String[] {
                "t0    text",
                "a1    integer",
                "b2    boolean",
                "colprojValue  text"
        }, "dummy_path",",");

        pxfExternalTable.setFragmenter(testPackage + "ColumnProjectionVerifyFragmenter");
        pxfExternalTable.setAccessor(testPackage + "ColumnProjectionVerifyAccessor");
        pxfExternalTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        gpdb.createTableAndVerify(pxfExternalTable);

        // TODO: revert when 2 queries with GP7 planner start propagating projection info to foreign scans
        // SELECT t0, colprojvalue FROM test_column_projection GROUP BY t0, colprojvalue HAVING AVG(a1) < 5 ORDER BY t0;
        // SELECT b.value, a.colprojvalue FROM test_column_projection a JOIN t0_values b ON a.t0 = b.key;
        if (gpdb.getVersion() >= 7) {
            /** The below query (mentioned in above comment as well) is propagating for FDW but not for external-table, so a different test set for FDW.
             * The Call stack is different in case of external-table and FDW.

             SELECT b.value, a.colprojvalue FROM test_column_projection a JOIN t0_values b ON a.t0 = b.key;
             value |  colprojvalue
             -------+-----------------
             50 | t0|colprojvalue
             (1 row)

             There is the explain plan for the external-table and FDW for the same query, '
             that might be the reason for one it's projecting and for other it's not.
             External Table:

             pxfautomation=# \d+ e_test_column_projection
             Foreign table "public.e_test_column_projection"
             Column    |  Type   | Collation | Nullable | Default | FDW options | Storage  | Stats target | Description
             --------------+---------+-----------+----------+---------+-------------+----------+--------------+-------------
             t0           | text    |           |          |         |             | extended |              |
             a1           | integer |           |          |         |             | plain    |              |
             b2           | boolean |           |          |         |             | plain    |              |
             colprojvalue | text    |           |          |         |             | extended |              |
             FDW options: (format 'text', delimiter ',', "null" E'\\N', escape E'\\', format_type 't', location_uris 'pxf://dummy_path?PROFILE=test:text&FRAGMENTER=org.greenplum.pxf.automation.testplugin.ColumnProjectionVerifyFragmenter&ACCESSOR=org.greenplum.pxf.automation.testplugin.ColumnProjectionVerifyAccessor&RESOLVER=org.greenplum.pxf.plugins.hdfs.StringPassResolver', execute_on 'ALL_SEGMENTS', log_errors 'f', encoding '6', is_writable 'false')

             pxfautomation=# explain analyze SELECT b.value, a.colprojvalue FROM e_test_column_projection a JOIN t0_values b ON a.t0 = b.key;
             QUERY PLAN
             ---------------------------------------------------------------------------------------------------------------------------------------------------
             Gather Motion 3:1  (slice1; segments: 3)  (cost=2306.08..20789139.42 rows=77900000 width=36) (actual time=78.603..78.606 rows=1 loops=1)
             ->  Hash Join  (cost=2306.08..19750472.75 rows=25966667 width=36) (actual time=52.602..63.398 rows=1 loops=1)
             Hash Cond: (a.t0 = (b.key)::text)
             Extra Text: (seg0)   Hash chain length 1.0 avg, 1 max, using 1 of 524288 buckets.
             ->  Foreign Scan on e_test_column_projection a  (cost=0.00..11000.00 rows=1000000 width=64) (actual time=51.045..51.076 rows=10 loops=1)
             ->  Hash  (cost=1332.33..1332.33 rows=77900 width=12) (actual time=0.039..0.040 rows=1 loops=1)
             Buckets: 524288  Batches: 1  Memory Usage: 4097kB
             ->  Broadcast Motion 3:3  (slice2; segments: 3)  (cost=0.00..1332.33 rows=77900 width=12) (actual time=0.013..0.014 rows=1 loops=1)
             ->  Seq Scan on t0_values b  (cost=0.00..293.67 rows=25967 width=12) (actual time=1.760..1.762 rows=1 loops=1)
             Optimizer: Postgres query optimizer
             Planning Time: 1.151 ms
             (slice0)    Executor memory: 106K bytes.
             (slice1)    Executor memory: 4253K bytes avg x 3 workers, 4253K bytes max (seg0).  Work_mem: 4097K bytes max.
             (slice2)    Executor memory: 37K bytes avg x 3 workers, 37K bytes max (seg0).
             Memory used:  128000kB
             Execution Time: 8581.766 ms
             (16 rows)

             FDW:

             pxfautomation=# \d+ test_column_projection
             Foreign table "public.test_column_projection"
             Column    |  Type   | Collation | Nullable | Default | FDW options | Storage  | Stats target | Description
             --------------+---------+-----------+----------+---------+-------------+----------+--------------+-------------
             t0           | text    |           |          |         |             | extended |              |
             a1           | integer |           |          |         |             | plain    |              |
             b2           | boolean |           |          |         |             | plain    |              |
             colprojvalue | text    |           |          |         |             | extended |              |
             Server: default_test
             FDW options: (resource 'dummy_path', format 'text', fragmenter 'org.greenplum.pxf.automation.testplugin.ColumnProjectionVerifyFragmenter', accessor 'org.greenplum.pxf.automation.testplugin.ColumnProjectionVerifyAccessor', resolver 'org.greenplum.pxf.plugins.hdfs.StringPassResolver', delimiter ',')

             pxfautomation=# explain analyze SELECT b.value, a.colprojvalue FROM test_column_projection a JOIN t0_values b ON a.t0 = b.key;
             QUERY PLAN
             ------------------------------------------------------------------------------------------------------------------------------------------------------------
             Gather Motion 3:1  (slice1; segments: 3)  (cost=50077.50..52328.93 rows=77900 width=36) (actual time=117.120..117.133 rows=1 loops=1)
             ->  Hash Join  (cost=50077.50..51290.27 rows=25967 width=36) (actual time=111.568..112.709 rows=1 loops=1)
             Hash Cond: ((b.key)::text = a.t0)
             Extra Text: (seg2)   Hash chain length 1.0 avg, 1 max, using 10 of 262144 buckets.
             ->  Seq Scan on t0_values b  (cost=0.00..293.67 rows=25967 width=12) (actual time=0.964..0.966 rows=1 loops=1)
             ->  Hash  (cost=50040.00..50040.00 rows=3000 width=64) (actual time=110.975..110.976 rows=10 loops=1)
             Buckets: 262144  Batches: 1  Memory Usage: 2049kB
             ->  Broadcast Motion 3:3  (slice2; segments: 3)  (cost=50000.00..50040.00 rows=3000 width=64) (actual time=110.902..110.906 rows=10 loops=1)
             ->  Foreign Scan on test_column_projection a  (cost=50000.00..50000.00 rows=1000 width=64) (actual time=1.312..1.329 rows=10 loops=1)
             Optimizer: Postgres query optimizer
             Planning Time: 0.592 ms
             (slice0)    Executor memory: 38K bytes.
             (slice1)    Executor memory: 2101K bytes avg x 3 workers, 2101K bytes max (seg0).  Work_mem: 2049K bytes max.
             (slice2)    Executor memory: 90K bytes avg x 3 workers, 97K bytes max (seg2).
             Memory used:  128000kB
             Execution Time: 117.692 ms
             (16 rows)
             **/
            if (FDWUtils.useFDW) {
                runTincTest("pxf.features.columnprojection.checkColumnProjection_fdw.runTest");
            }
            else {
                runTincTest("pxf.features.columnprojection.checkColumnProjection_gp7.runTest");
            }
        } else {
            runTincTest("pxf.features.columnprojection.checkColumnProjection.runTest");
        }
    }
}
