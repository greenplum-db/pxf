package org.greenplum.pxf.automation.testplugin;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hbase.HBaseAccessor;
import org.greenplum.pxf.plugins.hbase.HBaseFilterBuilder;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseTupleDescription;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Test class for regression tests.
 * The class is based on {@link HBaseAccessor}, with the only difference
 * that the filter is read from a user defined parameter TEST-HBASE-FILTER
 * instead of from GPDB.
 */
public class HBaseAccessorWithFilter extends BasePlugin implements Accessor {
    static private Log Log = LogFactory.getLog(HBaseAccessorWithFilter.class);

    static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    Operator.IS_NOT_NULL,
                    Operator.IS_NULL,
                    Operator.AND,
                    Operator.OR
            );

    private static TreeVisitor TREE_PRUNER = new SupportedOperatorPruner(SUPPORTED_OPERATORS);
    private static TreeTraverser TREE_TRAVERSER = new TreeTraverser();

    private HBaseTupleDescription tupleDescription;
    private HTable table;
    private List<SplitBoundary> splits;
    private Scan scanDetails;
    private ResultScanner currentScanner;
    private int currentRegionIndex;
    private byte[] scanStartKey;
    private byte[] scanEndKey;

    /*
     * The class represents a single split of a table
     * i.e. a start key and an end key
     */
    private class SplitBoundary {
        protected byte[] startKey;
        protected byte[] endKey;

        SplitBoundary(byte[] first, byte[] second) {
            startKey = first;
            endKey = second;
        }

        byte[] startKey() {
            return startKey;
        }

        byte[] endKey() {
            return endKey;
        }
    }

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        tupleDescription = new HBaseTupleDescription(requestContext);
        splits = new ArrayList<>();
        currentRegionIndex = 0;
        scanStartKey = HConstants.EMPTY_START_ROW;
        scanEndKey = HConstants.EMPTY_END_ROW;
    }

    @Override
    public boolean openForRead() throws Exception {
        openTable();
        createScanner();
        selectTableSplits();

        return openCurrentRegion();
    }

    /*
     * Close the table
     */
    @Override
    public void closeForRead() throws Exception {
        table.close();
    }

    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     * @throws Exception if opening the resource failed
     */
    @Override
    public boolean openForWrite() throws Exception {
        return false;
    }

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     * @throws Exception writing to the resource failed
     */
    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        return false;
    }

    /**
     * Closes the resource for write.
     *
     * @throws Exception if closing the resource failed
     */
    @Override
    public void closeForWrite() throws Exception {

    }

    @Override
    public OneRow readNextObject() throws IOException {
        Result result;
        // while currentScanner can't return a new result
        while ((result = currentScanner.next()) == null) {
            currentScanner.close(); // close it
            ++currentRegionIndex; // open next region

            if (!openCurrentRegion()) {
                return null; // no more splits on the list
            }
        }

        return new OneRow(null, result);
    }

    private void openTable() throws IOException {
        table = new HTable(HBaseConfiguration.create(configuration), context.getDataSource().getBytes());
    }

    /*
     * The function creates an array of start,end keys pairs for each
     * table split this Accessor instance is assigned to scan.
     *
     * The function verifies splits are within user supplied range
     *
     * It is assumed, |startKeys| == |endKeys|
     * This assumption is made through HBase's code as well
     */
    private void selectTableSplits() {

        byte[] serializedMetadata = context.getFragmentMetadata();
        if (serializedMetadata == null) {
            throw new IllegalArgumentException("Missing fragment metadata information");
        }
        try {
            ByteArrayInputStream bytesStream = new ByteArrayInputStream(serializedMetadata);
            ObjectInputStream objectStream = new ObjectInputStream(bytesStream);

            byte[] startKey = (byte[]) objectStream.readObject();
            byte[] endKey = (byte[]) objectStream.readObject();

            if (withinScanRange(startKey, endKey)) {
                splits.add(new SplitBoundary(startKey, endKey));
            }

        } catch (Exception e) {
            throw new RuntimeException("Exception while reading expected fragment metadata", e);
        }
    }

    /*
     * returns true if given start/end key pair is within the scan range
     */
    private boolean withinScanRange(byte[] startKey, byte[] endKey) {
        // startKey <= scanStartKey
        if (Bytes.compareTo(startKey, scanStartKey) <= 0) {
            if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) || // endKey == table's end
                    Bytes.compareTo(endKey, scanStartKey) >= 0) { // endKey >= scanStartKey
                return true;
            }
        } else { // startKey > scanStartKey
            if (Bytes.equals(scanEndKey, HConstants.EMPTY_END_ROW) || //  scanEndKey == table's end
                    Bytes.compareTo(startKey, scanEndKey) <= 0) { // startKey <= scanEndKey
                return true;
            }
        }
        return false;
    }

    /*
     * The function creates the Scan object used to describe the query
     * requested from HBase.
     * As the row key column always gets returned, no need to ask for it
     */
    private void createScanner() throws Exception {
        scanDetails = new Scan();
        // Return only one version (latest)
        scanDetails.setMaxVersions(1);

        addColumns();
        addFilters();
    }

    /*
     * Open the region of index currentRegionIndex from splits list.
     * Update the Scan object to retrieve only rows from that region.
     */
    private boolean openCurrentRegion() throws IOException {
        if (currentRegionIndex == splits.size()) {
            return false;
        }

        SplitBoundary region = splits.get(currentRegionIndex);
        scanDetails.setStartRow(region.startKey());
        scanDetails.setStopRow(region.endKey());

        currentScanner = table.getScanner(scanDetails);
        return true;
    }

    private void addColumns() {
        for (int i = 0; i < tupleDescription.columns(); ++i) {
            HBaseColumnDescriptor column = tupleDescription.getColumn(i);
            if (!column.isKeyColumn()) { // Row keys return anyway
                scanDetails.addColumn(column.columnFamilyBytes(), column.qualifierBytes());
            }
        }
    }

    /*
     * Uses HBaseFilterBuilder to translate a filter string into a
     * HBase Filter object. The result is added as a filter to the
     * Scan object
     *
     * use row key ranges to limit split count
     *
     * ignores filter from gpdb, use user defined filter
     */
    private void addFilters() throws Exception {

        // TODO whitelist option
        String filterStr = context.getOption("TEST-HBASE-FILTER");
        Log.debug("user defined filter: " + filterStr);
        if ((filterStr == null) || filterStr.isEmpty() || "null".equals(filterStr))
            return;

        HBaseFilterBuilder hBaseTreeVisitor = new HBaseFilterBuilder(tupleDescription);
        Node root = new FilterParser().parse(filterStr.getBytes(FilterParser.DEFAULT_CHARSET));
        root = TREE_PRUNER.visit(root);

        TREE_TRAVERSER.inOrderTraversal(root, hBaseTreeVisitor);

        Filter filter = hBaseTreeVisitor.buildFilter();
        scanDetails.setFilter(filter);

        scanStartKey = hBaseTreeVisitor.getStartKey();
        scanEndKey = hBaseTreeVisitor.getEndKey();
    }
}
