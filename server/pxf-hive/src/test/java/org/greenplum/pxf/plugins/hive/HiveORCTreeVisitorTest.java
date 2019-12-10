package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.greenplum.pxf.api.filter.BaseTreePruner;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.TreePruner;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.greenplum.pxf.plugins.hive.HiveORCAccessor.SUPPORTED_OPERATORS;
import static org.junit.Assert.assertNotNull;

public class HiveORCTreeVisitorTest {

    private static final TreePruner TREE_PRUNER = new BaseTreePruner(SUPPORTED_OPERATORS);
    private List<ColumnDescriptor> tupleDescription;

    @Before
    public void setup() {

        tupleDescription = new ArrayList<>();
        tupleDescription.add(new ColumnDescriptor("col1", 1, 1, "TEXT", null));
        tupleDescription.add(new ColumnDescriptor("col2", 1, 1, "TEXT", null));
        tupleDescription.add(new ColumnDescriptor("col3", 1, 1, "TEXT", null));
    }

    @Test
    public void testIsNotNull() throws Exception {
        // NOT (_1_ IS NULL)
        String filterStr = "a1o8l2"; // ORCA transforms is not null to NOT ( a IS NULL )

        Node root = new FilterParser().parse(filterStr.getBytes());
        root = TREE_PRUNER.prune(root);

        HiveORCTreeVisitor treeVisitor = new HiveORCTreeVisitor(tupleDescription, new Configuration());
        new TreeTraverser().inOrderTraversal(root, treeVisitor);

        SearchArgument.Builder filterBuilder = treeVisitor.getFilterBuilder();

        assertNotNull(filterBuilder);
    }

}