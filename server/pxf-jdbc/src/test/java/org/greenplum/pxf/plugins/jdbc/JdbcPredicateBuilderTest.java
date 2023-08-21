package org.greenplum.pxf.plugins.jdbc;

import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test is similar to the test of the superclass: org.greenplum.pxf.api.filter.ColumnPredicateBuilderTest
 * in the API package. The table structure and the filter strings are similar to the ones used by the
 * automation test org.greenplum.pxf.automation.features.filterpushdown.FilterPushDownTest in the automation/src/test folder.
 */
public class JdbcPredicateBuilderTest {
    private List<ColumnDescriptor> columnDescriptors;
    private TreeTraverser treeTraverser;

    @BeforeEach
    public void setup() {
        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("t0", DataType.TEXT.getOID()   , 0, null, null));
        columnDescriptors.add(new ColumnDescriptor("a1", DataType.INTEGER.getOID(), 1, null, null));
        columnDescriptors.add(new ColumnDescriptor("b2", DataType.BOOLEAN.getOID(), 2, null, null));
        columnDescriptors.add(new ColumnDescriptor("c3", DataType.NUMERIC.getOID(), 3, null, null));
        columnDescriptors.add(new ColumnDescriptor("d4", DataType.BPCHAR.getOID() , 4, null, null));
        columnDescriptors.add(new ColumnDescriptor("e5", DataType.VARCHAR.getOID(), 5, null, null));

        treeTraverser = new TreeTraverser();
    }

    @Test
    public void testTextFilters() throws Exception {
        runScenario("a0c25s1dBo5", "WHERE t0 = 'B'");
        runScenario("a0c25s1dBo1", "WHERE t0 < 'B'");
        runScenario("a0c25s1dBo3", "WHERE t0 <= 'B'");
        runScenario("a0c25s1dBo2", "WHERE t0 > 'B'");
        runScenario("a0c25s1dBo4", "WHERE t0 >= 'B'");
        runScenario("a0c25s1dBo6", "WHERE t0 <> 'B'");
        runScenario("a0c25s2dB%o7", "WHERE t0 LIKE 'B%'");
        runScenario("a0m1009s1dBs1dCo10", "WHERE t0 IN ('B','C')");
    }

    @Test
    public void testIntegerFilters() throws Exception {
        runScenario("a1c23s1d1o5", "WHERE a1 = 1");
        runScenario("a1c23s1d1o1", "WHERE a1 < 1");
        runScenario("a1c23s1d1o3", "WHERE a1 <= 1");
        runScenario("a1c23s1d1o2", "WHERE a1 > 1");
        runScenario("a1c23s1d1o4", "WHERE a1 >= 1");
        runScenario("a1c23s1d1o6", "WHERE a1 <> 1");
        runScenario("a1m1007s1d1s1d2o10", "WHERE a1 IN (1,2)");
    }

    @Test
    public void testNumericFilters() throws Exception {
        runScenario("a3c1700s4d1.11o5", "WHERE c3 = 1.11");
        runScenario("a3c1700s4d1.11o1", "WHERE c3 < 1.11");
        runScenario("a3c1700s4d1.11o3", "WHERE c3 <= 1.11");
        runScenario("a3c1700s4d1.11o2", "WHERE c3 > 1.11");
        runScenario("a3c1700s4d1.11o4", "WHERE c3 >= 1.11");
        runScenario("a3c1700s4d1.11o6", "WHERE c3 <> 1.11");
        // IN operator is not yet supported for NUMERIC types
    }

    @Test
    public void testCharFilters() throws Exception {
        runScenario("a4c1042s2dBBo5", "WHERE d4 = 'BB'");
        runScenario("a4c1042s2dBBo1", "WHERE d4 < 'BB'");
        runScenario("a4c1042s2dBBo3", "WHERE d4 <= 'BB'");
        runScenario("a4c1042s2dBBo2", "WHERE d4 > 'BB'");
        runScenario("a4c1042s2dBBo4", "WHERE d4 >= 'BB'");
        runScenario("a4c1042s2dBBo6", "WHERE d4 <> 'BB'");
        // LIKE operator is not yet supported for CHAR types
        // IN   operator is not yet supported for CHAR types
    }

    @Test
    public void testVarcharFilters() throws Exception {
        // note that filters for VARCHAR column come with encoded OID of 25 which is TEXT
        runScenario("a5c25s2dBBo5", "WHERE e5 = 'BB'");
        runScenario("a5c25s2dBBo1", "WHERE e5 < 'BB'");
        runScenario("a5c25s2dBBo3", "WHERE e5 <= 'BB'");
        runScenario("a5c25s2dBBo2", "WHERE e5 > 'BB'");
        runScenario("a5c25s2dBBo4", "WHERE e5 >= 'BB'");
        runScenario("a5c25s2dBBo6", "WHERE e5 <> 'BB'");
        // LIKE operator is not yet supported for VARCHAR types
        // IN   operator is not yet supported for VARCHAR types
    }

    @Test
    public void testLogicalFilters() throws Exception {
        runScenario("a0c25s1dAo5a1c23s1d0o5l0", "WHERE (t0 = 'A' AND a1 = 0)");
        runScenario("a1c23s1d1o3a0c25s1dBo5l0", "WHERE (a1 <= 1 AND t0 = 'B')");
        runScenario("a0c25s1dBo5a1c23s1d1o5a1c23s2d10o5l1l0", "WHERE (t0 = 'B' AND (a1 = 1 OR a1 = 10))");
        runScenario("a0c25s1dBo5a1c23s1d0o4a1c23s1d2o3l0l1", "WHERE (t0 = 'B' OR (a1 >= 0 AND a1 <= 2))");
        runScenario("a2c16s4dtrueo0l2", "WHERE NOT (b2)");
        runScenario("a2c16s4dtrueo0l2a1c23s1d5o1l0", "WHERE (NOT (b2) AND a1 < 5)");
        runScenario("a2c16s4dtrueo0l2a1c23s1d1o5a1c23s2d10o5l1l0", "WHERE (NOT (b2) AND (a1 = 1 OR a1 = 10))");
        runScenario("a2c16s4dtrueo0l2a1c23s1d0o4a1c23s1d2o3l0l1", "WHERE (NOT (b2) OR (a1 >= 0 AND a1 <= 2))");
    }

    /**
     * Runs a test scenario for all supported flavors of databases
     * @param filterString filter string
     * @param expectedQuery expected SQL query fragment without the leading space
     * @throws Exception if an error occurs
     */
    private void runScenario(String filterString, String expectedQuery) throws Exception {
        // test for all supported database flavors
        for (DbProduct db : DbProduct.values()) {
            runScenario(filterString, db, expectedQuery, null);
        }
    }

    /**
     * Runs a scenario where a given filter is parsed and used by the JdbcPredicateBuilder to create a WHERE SQL statement
     * @param filterString filter string passed from Greenplum
     * @param dbProduct database flavor
     * @param expectedQuery expected SQL query fragment without the leading space
     * @param quoteString a quote string to use, null if default
     * @throws Exception if an error occurs
     */
    private void runScenario(String filterString, DbProduct dbProduct, String expectedQuery, String quoteString) throws Exception {
        Node root = new FilterParser().parse(filterString);
        JdbcPredicateBuilder jdbcPredicateBuilder;
        if (quoteString != null) {
            jdbcPredicateBuilder = new JdbcPredicateBuilder(dbProduct, quoteString, columnDescriptors);
        } else {
            jdbcPredicateBuilder = new JdbcPredicateBuilder(dbProduct, columnDescriptors);
        }
        treeTraverser.traverse(root, jdbcPredicateBuilder);

        // result will have a space added before WHERE clause, we will pass expected values without the space
        assertEquals(" " + expectedQuery, jdbcPredicateBuilder.toString());
    }
}
