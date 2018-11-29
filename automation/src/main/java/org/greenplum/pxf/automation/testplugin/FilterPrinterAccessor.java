package org.greenplum.pxf.automation.testplugin;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;

/**
 * Test class for regression tests.
 * The only thing this class does is to throw an exception
 * containing the received filter from GPDB (HAS-FILTER & FILTER).
 */
public class FilterPrinterAccessor extends BasePlugin implements Accessor
{
    static private Log Log = LogFactory.getLog(FilterPrinterAccessor.class);

    /*
     * exception for exposing the filter to the world
     */
    class FilterPrinterException extends Exception {
        FilterPrinterException(String filter) {
            super("Filter string: '" + filter + "'");
        }
    }

    @Override
    public boolean openForRead() throws Exception {

        String filter = context.hasFilter() ?
                context.getFilterString() : "No filter";

        throw new FilterPrinterException(filter);
    }

    @Override
    public void closeForRead() {
        throw new UnsupportedOperationException("closeForRead is not implemented");
    }

    @Override
    public OneRow readNextObject() {
        throw new UnsupportedOperationException("readNextObject is not implemented");
    }

    @Override
    public boolean openForWrite() throws Exception {
        throw new UnsupportedOperationException("openForWrite is not implemented");
    }

    @Override
    public boolean writeNextObject(OneRow oneRow) throws Exception {
        throw new UnsupportedOperationException("writeNextObject is not implemented");
    }

    @Override
    public void closeForWrite() throws Exception {
        throw new UnsupportedOperationException("closeForWrite is not implemented");
    }
}
